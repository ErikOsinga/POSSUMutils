"""
Test cirada_software: log_processing_status_1D_PartialTiles.py
"""
import unittest
from cirada_software import log_processing_status_1D_PartialTiles
from automation import (insert_database_script as db, database_queries as db_query)

class LogProcessingStatus1DPartialTilesTest(unittest.TestCase):

    def setUp(self):
        self.conn = db_query.get_database_connection(test=True)
        db.create_test_schema(self.conn)
        db.create_observation_1d_relation_tables(self.conn)
        db.create_partial_tile_pipeline_tables(self.conn)
        # insert partial tile data
        rows = [
            # field name, sbid, tile1, tile2, tile3, tile4, type, number_sources, 1d_pipeline
            ["EMU_1748-64", "54926", "11726", "11727", "11791", "11792", "corner - crosses projection boundary!", "", ""],
            ["EMU_1748-64", "54926", "11791", "11792", "11852", "11853", "corner - crosses projection boundary!", "1000", ""]            
        ]
        for row in rows:
            db.insert_row_into_partial_tile_table(row, '1', self.conn)
        # insert observation data
        rows = [
            ["EMU_1748-64",	"54926", "", ""]
        ]
        for row in rows:
            db_query.update_1d_pipeline_validation_status(row[0], row[1], '1', row[2], self.conn)
            db_query.update_single_sb_1d_pipeline_status(row[0], row[1], '1', row[3], self.conn)

    def tearDown(self):
        if self.conn:
            db.drop_test_tables(self.conn)
            db.drop_test_schema(self.conn)
            self.conn.close()
            self.conn = None
            
    def test_update_partial_tile_1d_pipeline(self):
        """ Test update_partial_tile_1d_pipeline
        """
        field_ID = '1748-64'
        tile_numbers = ['11791', '11792', '11852', '11853']
        band = '943MHz'
        status = 'Completed'
        log_processing_status_1D_PartialTiles.update_partial_tile_1d_pipeline(field_ID, tile_numbers, band, status, self.conn)
        new_status = db_query.get_1d_pipeline_status('EMU_1748-64', tile_numbers, 1, self.conn)
        assert new_status[0][0] == 'Completed'