"""
Test possum_pipeline_control: check_status_and_launch_3Dpipeline_v2.py
"""
from automation import database_queries as db_query
from automation.unit_tests._3dpipeline_base_test import _3DPipelineBaseTest

class CheckStatusAndLaunch3DPipelinev2Test(_3DPipelineBaseTest):
    """
    Setup and tearDown is done in the 3DPipelineBaseTest class.
    """
    def test_update_status(self):
        "Test update_status"
        tile_number = 1239
        row_num = db_query.update_3d_pipeline_table(tile_number, "1", "Running", "3d_pipeline", self.conn)
        assert row_num == 1

        tile_number = '1240'
        row_num = db_query.update_3d_pipeline_table(tile_number, "1", "Running", "3d_pipeline", self.conn)
        assert row_num == 1

        #check that the update works
        results = db_query.get_3d_tile_data('1239', '1', self.conn)
        assert results[0][4] == 'Running' #3d_pipeline

        results = db_query.get_3d_tile_data(1240, '1', self.conn)
        assert results[0][4] == 'Running' #3d_pipeline
