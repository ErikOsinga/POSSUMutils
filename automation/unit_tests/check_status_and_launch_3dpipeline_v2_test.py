"""
Test possum_pipeline_control: check_status_and_launch_3Dpipeline_v2.py
"""
import unittest
from automation import (insert_database_script as db, database_queries as db_query)

class CheckStatusAndLaunch3DPipelinev2Test(unittest.TestCase):
    def setUp(self):
        self.conn = db_query.get_database_connection(test=True)
        db.create_test_schema(self.conn)   
        db.create_tile_3d_pipeline_tables(self.conn)
        _3d_data = [
            ('1239', None, '', None),
            ('1240', '', None, ''),
            ('1241', 'Failed', '', ''),
            ('1242', 'Running', None, None)
        ]
        for row in _3d_data:
            db.insert_3d_pipeline_test_data(row[0], row[1], row[2], row[3], self.conn)
         
    def tearDown(self):
        if self.conn:
            db.drop_test_tables(self.conn)
            db.drop_test_schema(self.conn)
            self.conn.close()
            self.conn = None
                
    def test_update_status(self):
        tile_number = 1239
        row_num = db_query.update_3d_pipeline(tile_number, "1", "Running", self.conn)
        assert row_num == 1
        
        tile_number = '1240'
        row_num = db_query.update_3d_pipeline(tile_number, "1", "Running", self.conn)
        assert row_num == 1
        
        #check that the update works
        results = db_query.get_3d_tile_data('1239', '1', self.conn)
        assert results[0][4] == 'Running'
        
        results = db_query.get_3d_tile_data(1240, '1', self.conn)
        assert results[0][4] == 'Running'
    