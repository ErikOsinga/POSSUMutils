"""
Test cirada_software: log_processing_status_1D_PartialTiles_summary.py
"""
import unittest
from cirada_software import log_processing_status_1D_PartialTiles_summary
from automation import database_queries as db_query
from automation import insert_database_script as db_insert
from possum_pipeline_control import util

class LogProcessingStatus1DPartialTilesSummaryTest(unittest.TestCase):
    def setUp(self):
        self.conn = db_query.get_database_connection(test=True)
        db_insert.create_test_schema(self.conn)
        db_insert.create_partial_tile_pipeline_tables(self.conn)
        db_insert.create_observation_1d_relation_tables(self.conn)
        # insert partial tile data
        rows = [
            # field name, sbid, tile1, tile2, tile3, tile4, type, number_sources, 1d_pipeline
            ["EMU_1136-62", "46946", "11490", "11491", "11492", "11493", "corner - crosses projection boundary!", "77", ""],
            ["EMU_1136-62", "46946", "11491", "11492", "11493", "11494", "corner", "640", "Completed"],
            ["EMU_1136-61", "46950", "11500", "", "", "", "center", "12", "Completed"],
            ["EMU_1136-61", "46950", "11500", "11501", "11502", "11503", "corner", "55", "Completed"],
        ]
        for row in rows:
            db_insert.insert_row_into_partial_tile_table(row, '1', self.conn)
        # insert observation data
        rows = [
            ["EMU_1136-61", "46950", "", ""],
            ["EMU_1136-62", "46946", "", ""]
        ]
        for row in rows:
            db_query.update_1d_pipeline_validation_status(row[0], row[1], '1', row[2], self.conn)
            db_query.update_single_sb_1d_pipeline_status(row[0], row[1], '1', row[3], self.conn)

    def tearDown(self):
        if self.conn:
            db_insert.drop_test_tables(self.conn)
            db_insert.drop_test_schema(self.conn)
            self.conn.close()
            self.conn = None

    def test_update_validation_spreadsheet(self):
        """
        Test update_validation_spreadsheet
        """
        field_ID = "1136-62"
        SBid = "46946"
        band = "943MHz"
        status = "Failed"
        # has boundary issues
        boundary_issues = log_processing_status_1D_PartialTiles_summary.update_validation_spreadsheet(field_ID, SBid, band, status, self.conn)
        assert boundary_issues is True
        # check that the status was updated
        full_field_name = util.get_full_field_name(field_ID, band)
        _1d_pipeline_validation = db_query.get_1d_pipeline_validation_status(field_name=full_field_name, band_number="1", conn=self.conn)[0][0]
        assert _1d_pipeline_validation == "Failed"

        # no boundary issues
        field_ID = "1136-61"
        SBid = "46950"
        status = "Completed"
        boundary_issues = log_processing_status_1D_PartialTiles_summary.update_validation_spreadsheet(field_ID, SBid, band, status, self.conn)
        assert boundary_issues is False
        full_field_name = util.get_full_field_name(field_ID, band)
        _1d_pipeline_validation = db_query.get_1d_pipeline_validation_status(field_name=full_field_name, band_number="1", conn=self.conn)[0][0]
        assert _1d_pipeline_validation == "Completed"
        
        # row doesn't exist
        field_ID = "1136-1000"
        boundary_issues = log_processing_status_1D_PartialTiles_summary.update_validation_spreadsheet(field_ID, SBid, band, status, self.conn)
        assert boundary_issues is False
        
    def test_update_status_spreadsheet(self):
        """
        Test update_status_spreadsheet
        """
        field_ID = "1136-62"
        SBid = "46946"
        band = "943MHz"
        status = "Failed"
        full_field_name = util.get_full_field_name(field_ID, band)
        band_number = util.get_band_number(band)
        row_num = db_query.update_single_sb_1d_pipeline_status(full_field_name, SBid, band_number, status, self.conn)
        # Verify successful
        assert row_num == 1
        status = db_query.get_single_sb_1d_pipeline_status(full_field_name, "1", self.conn)[0][0]
        assert status == 'Failed'
        