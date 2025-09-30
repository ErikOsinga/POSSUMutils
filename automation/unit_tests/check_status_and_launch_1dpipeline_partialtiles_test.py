import unittest
from possum_pipeline_control import check_status_and_launch_1Dpipeline_PartialTiles as check_status
from automation import (insert_database_script as db, database_queries as db_query)

class CheckStatusAndLaunch1DPipelinePartialTiles(unittest.TestCase):

    def setUp(self):
        self.conn = db_query.get_database_connection(test=True)
        db.create_test_schema(self.conn)
        db.create_observation_1d_relation_tables(self.conn)
        db.create_partial_tile_pipeline_tables(self.conn)
        # insert partial tile data
        rows = [
            # field name, sbid, tile1, tile2, tile3, tile4, type, number_sources, 1d_pipeline
            ["EMU_1748-64", "54926", "11726", "11727", "11791", "11792", "corner - crosses projection boundary!", "", ""],
            ["EMU_1748-64", "54926", "11791", "11792", "11852", "11853", "corner - crosses projection boundary!", "1000", ""],
            ["EMU_1759-69", "46955", "11726", "11791", "", "", "edge - crosses projection boundary!", "", ""],
            ["EMU_1759-69", "46955", "11726", "11727", "", "", "edge - crosses projection boundary!", "999", ""],
            ["EMU_1759-69", "46955", "11791", "11852", "", "", "edge - crosses projection boundary!", "982", ""],
            ["EMU_1759-69", "46955", "11726", "11727", "11791", "11792", "corner - crosses projection boundary!", "12", ""],
            ["EMU_1759-69", "46955", "11791", "11792", "11852", "11853", "corner - crosses projection boundary!", "24", ""],
            ["EMU_1701-64", "46980", "11791", "11852", "", "", "edge - crosses projection boundary!", "", ""],
            ["EMU_1136-64", "46948", "11487", "11565", "", "", "edge - crosses projection boundary!", "123", ""],
            ["EMU_1136-64", "46948", "11487", "11488", "", "", "edge - crosses projection boundary!", "45", ""],
            ["EMU_1136-62", "46946", "11490", "11491", "11492", "11493", "corner - crosses projection boundary!", "77", ""], # complete if we ignore edge cases
            ["EMU_1136-62", "46946", "11491", "11492", "11493", "11494", "corner", "640", "Completed"], # partially completed
            ["EMU_1136-61", "46950", "11500", "", "", "", "center", "12", "Completed"], # all completed
            ["EMU_1136-61", "46950", "11500", "11501", "11502", "11503", "corner", "55", "Completed"], # all completed
            ["EMU_1136-60", "52000", "12345", "", "", "", "center", "20", "Completed"], # all completed
            ["EMU_0626-09B", "61077", "6754", "", "", "", "center", "26", "Completed"], # all completed
            ["EMU_0626-09B", "61077", "6755", "", "", "", "center", "59", "Completed"],
            ["EMU_0626-09B", "61077", "6881", "", "", "", "center", "316", "Completed"],
            ["EMU_0626-09B", "61077", "6882", "", "", "", "center", "838", "Completed"],
            ["EMU_0626-09B", "61077", "6883", "", "", "", "center", "411", "Completed"],
            ["EMU_0626-09B", "61077", "7009", "", "", "", "center", "27", "Completed"],
            ["EMU_0626-09B", "61077", "7010", "", "", "", "center", "772", "Completed"],
            ["EMU_0626-09B", "61077", "7011", "", "", "", "center", "1713", "Completed"],
            ["EMU_0626-09B", "61077", "7137", "", "", "", "center", "1133", "Completed"],
            ["EMU_0626-09B", "61077", "7138", "", "", "", "center", "1953", "Completed"],
            ["EMU_0626-09B", "61077", "7139", "", "", "", "center", "633", "Completed"],
            ["EMU_0626-09B", "61077", "7265", "", "", "", "center", "69", "Completed"],
            ["EMU_0626-09B", "61077", "7266", "", "", "", "center", "1799", "Completed"],
            ["EMU_0626-09B", "61077", "7267", "", "", "", "center", "1806", "Completed"],
            ["EMU_0626-09B", "61077", "7393", "", "", "", "center", "474", "Completed"],
            ["EMU_0626-09B", "61077", "7394", "", "", "", "center", "573", "Completed"],
            ["EMU_0626-09B", "61077", "7395", "", "", "", "center", "55", "Completed"],
            ["EMU_0626-09B", "61077", "6754", "6881", "", "", "edge", "6", "Completed"],
            ["EMU_0626-09B", "61077", "6754", "6882", "", "", "edge", "4", "Completed"],
            ["EMU_0626-09B", "61077", "6755", "6882", "", "", "edge", "17", "Completed"]
        ]
        for row in rows:
            db.insert_row_into_partial_tile_table(row, '1', self.conn)
        # insert observation data
        rows = [
            ["EMU_1136-60", "52000", "Completed", ""], # should be excluded because it's already been completed
            ["EMU_0626-09B", "61077", "", ""], # 1d_pipeline_validation not yet validated but each partial tile is completed
            ["EMU_1136-61", "46950", "", ""],
            ["EMU_1136-62", "46946", "", ""],
            ["EMU_1136-64",	"46948", "", ""],
            ["EMU_1701-64",	"46980", "", ""],
            ["EMU_1748-64",	"54926", "", ""],
            ["EMU_1759-69",	"46955", "", ""]
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

    def test_get_tiles_for_pipeline_run(self):
        (
         field_IDs, tile1, tile2, tile3, tile4, SBids, fields_to_validate, field_to_validate_boundaryissues
        ) = check_status.get_tiles_for_pipeline_run(self.conn, band_number=1)
        assert field_IDs == ['1748-64', '1759-69', '1759-69', '1759-69', '1759-69', '1136-64', '1136-64', '1136-62']
        assert tile1 == ['11791', '11726', '11791', '11726', '11791', '11487', '11487', '11490']
        assert tile2 == ['11792', '11727', '11852', '11727', '11792', '11565', '11488', '11491']
        assert tile3 == ['11852', '', '', '11791', '11852', '', '', '11492']
        assert tile4 == ['11853', '', '', '11792', '11853', '', '', '11493']
        assert SBids == ['54926', '46955', '46955', '46955', '46955', '46948', '46948', '46946']
        assert fields_to_validate == [('EMU_1136-61', '46950'), ('EMU_0626-09B', '61077')]
        assert field_to_validate_boundaryissues == {('EMU_1136-62', '46946')}

    def test_update_1d_pipeline_validation_status(self):
        """ Testing - update the status of the '1d_pipeline_validation' column to "Running"
        regardless of tile number
        """
        field = 'EMU_1748-64'
        sbid = 54926
        band_number = 1
        status = "Running"
        row_num = db_query.update_1d_pipeline_validation_status(field, sbid, band_number, status, self.conn)
        assert row_num == 1
        # Make sure the update works
        results = db_query.get_1d_pipeline_validation_status(field, band_number, self.conn)
        new_status = results[0][0]
        assert new_status == status


    def test_update_partial_tile_1d_pipeline_status(self):
        """ Testing - update the status of '1d_pipeline' column in partial tile table.
        """
        field = 'EMU_1748-64'
        tilenumbers = ['11791', '11792', '11852', '11853']
        band_number = '1'
        previous_status = db_query.get_1d_pipeline_status(field, tilenumbers, band_number, self.conn)
        db_query.update_partial_tile_1d_pipeline_status(field, tilenumbers, band_number, "Running", self.conn)
        new_status = db_query.get_1d_pipeline_status(field, tilenumbers, band_number, self.conn)
        assert previous_status[0][0] != new_status[0][0]
        assert new_status[0][0] == 'Running'

        # Test with None in tiles
        field = 'EMU_1136-60'
        tilenumbers = ['12345', None, None, None]
        band_number = '1'
        previous_status = db_query.get_1d_pipeline_status(field, tilenumbers, band_number, self.conn)
        db_query.update_partial_tile_1d_pipeline_status(field, tilenumbers, band_number, "Running", self.conn)
        new_status = db_query.get_1d_pipeline_status(field, tilenumbers, band_number, self.conn)
        assert previous_status[0][0] != new_status[0][0]
        assert new_status[0][0] == 'Running'
        
        # Test with empty tile number
        field = 'EMU_1136-60'
        tilenumbers = ['12345', '', '', '']
        band_number = '1'
        previous_status = db_query.get_1d_pipeline_status(field, tilenumbers, band_number, self.conn)
        db_query.update_partial_tile_1d_pipeline_status(field, tilenumbers, band_number, "Completed", self.conn)
        new_status = db_query.get_1d_pipeline_status(field, tilenumbers, band_number, self.conn)
        assert previous_status[0][0] != new_status[0][0]
        assert new_status[0][0] == 'Completed'
