"""
Test possum_pipeline_control: Ingest3Dpipeline.py
"""
from datetime import date
from possum_pipeline_control import ingest3Dpipeline
from automation import database_queries as db_query
from automation.unit_tests._3dpipeline_base_test import _3DPipelineBaseTest
class Ingest3DPipelineTest(_3DPipelineBaseTest):
    """
    Setup and tearDown is done in the 3DPipelineBaseTest class.
    """
    def test_update_validation_spreadsheet(self):
        """
        Test updating 3d_pipeline_ingest status in ingest3dpipeline
        """
        tilenumber = '1241'
        # When status is not 'IngestRunning', you should get an error
        with self.assertRaises(ValueError):
            ingest3Dpipeline.update_validation_spreadsheet.fn(tilenumber, "943MHz", 'Ingested', test_flag=False, conn=self.conn)
        ingest_status = db_query.get_3d_tile_data(tilenumber, '1', self.conn)[0][3]
        assert(ingest_status == '') # 3d_pipeline_ingest

        # Status is IngestRunning
        tilenumber = '1242'
        ingest3Dpipeline.update_validation_spreadsheet.fn(tilenumber, "943MHz", 'Ingested', test_flag=False, conn=self.conn)
        ingest_status = db_query.get_3d_tile_data(tilenumber, '1', self.conn)[0][3]
        assert(ingest_status == 'Ingested') # 3d_pipeline_ingest

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
        assert status == timestamp #3d_pipeline
