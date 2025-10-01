import unittest
from datetime import date
from possum_pipeline_control import ingest3Dpipeline
from automation import (insert_database_script as db, database_queries as db_query)

class Ingest3DPipelineTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_query.get_database_connection(test=True)
        db.create_test_schema(self.conn)
        db.create_tile_3d_pipeline_tables(self.conn)
        _3d_data = [
            ('1239', None, '', None),
            ('1240', '', None, ''),
            ('1241', 'WaitingForValidation', '', ''),
            ('1242', 'Running', None, 'IngestRunning')
        ]
        for row in _3d_data:
            db.insert_3d_pipeline_test_data(tile_id=row[0], _3d_pipeline=row[1],
                                            val=row[2], ingest=row[3], conn=self.conn)

    def tearDown(self):
        if self.conn:
            db.drop_test_tables(self.conn)
            db.drop_test_schema(self.conn)
            self.conn.close()
            self.conn = None

    def test_update_validation_spreadsheet(self):
        """
        Test updating 3d_pipeline_ingest status in ingest3dpipeline
        """
        tilenumber = '1241'
        # When status is not 'IngestRunning', you should get an error
        with self.assertRaises(ValueError):
            ingest3Dpipeline.update_validation_spreadsheet.fn(tilenumber, "943MHz", 'Ingested', test_flag=False, conn=self.conn)
        ingest_status = db_query.get_3d_tile_data(tilenumber, '1', self.conn)[0][3]
        assert(ingest_status == '') # Status should not be updated
        
        # Status is IngestRunning
        tilenumber = '1242'
        ingest3Dpipeline.update_validation_spreadsheet.fn(tilenumber, "943MHz", 'Ingested', test_flag=False, conn=self.conn)
        ingest_status = db_query.get_3d_tile_data(tilenumber, '1', self.conn)[0][3]
        assert(ingest_status == 'Ingested') # Status should be updated

    def test_update_status_spreadsheet(self):
        """
        Test updating 3d_pipeline status in the database with timestamp
        """
        tilenumber = '1239'
        timestamp = date(2025, 7, 15).isoformat()
        row_num = db_query.update_3d_pipeline(tilenumber, "1", timestamp, self.conn)
        assert row_num == 1
        
        # Verify it's successful
        status = db_query.get_3d_tile_data(tilenumber, '1', self.conn)[0][4]
        assert status == timestamp
