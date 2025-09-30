import unittest
from possum_pipeline_control import check_ingest_3Dpipeline
from automation import insert_database_script as db
from automation import database_queries as db_query

class CheckIngest3dPipeline(unittest.TestCase):

    def setUp(self):
        self.conn = db_query.get_database_connection(True)
        db.create_test_schema(self.conn)
        db.create_tile_3d_pipeline_tables(self.conn)

        _3d_data = [
            # ingested
            ('321', 'WaitingForValidation', 'Good', 'Ingested'),
            # ingest failed
            ('421', 'WaitingForValidation', 'Good', 'IngestFailed'),
            # ingest running
            ('532', 'WaitingForValidation', 'Good', 'IngestRunning'),
            # ready to ingest
            ('1234', 'WaitingForValidation', 'Good', ''),
            ('1235', 'WaitingForValidation', 'Good', None),
            # validated but not ready to ingest
            ('1236', 'WaitingForValidation', '', ''),
            ('1237', 'WaitingForValidation', None, None),
            # validated but not going to be ingested
            ('1238', 'WaitingForValidation', 'Bad', None),
            # not yet validated
            ('1239', None, '', None),
            ('1240', '', None, ''),
            ('1241', 'Failed', '', ''),
            ('1242', 'Running', None, None)
        ]
        for row in _3d_data:
            db.insert_3d_pipeline_test_data(row[0], row[1], row[2], row[3], self.conn)

    def tearDown(self):
        db.drop_test_tables(self.conn)
        db.drop_test_schema(self.conn)
        self.conn.close()

    def test_get_tiles_for_ingest(self):
        """
        Based on the original code:
        tiles_to_run = [row['tile_id'] for row in tile_table if ( (row['3d_pipeline_val'] == 'Good') and (row['3d_pipeline_ingest'] == '') )]
        """
        tiles_to_run = check_ingest_3Dpipeline.get_tiles_for_ingest('1', self.conn)
        assert len(tiles_to_run) == 2
        assert tiles_to_run[0] == 1234
        assert tiles_to_run[1] == 1235

    def test_update_status(self):
        """
        Test updating ingest status
        """
        num_rows = check_ingest_3Dpipeline.update_status(tile_number=1235, band='943MHz', status='Ingested', conn=self.conn)
        assert num_rows == 1 #succesful update
		#Check if the value was updated
        results = db_query.get_3d_tile_data(1235, '1', self.conn)
        assert len(results) == 1
        assert results[0][3] == 'Ingested'

        num_rows = check_ingest_3Dpipeline.update_status(tile_number=000, band='943MHz', status='Ingested', conn=self.conn)
        assert num_rows == 0 #tile not found


