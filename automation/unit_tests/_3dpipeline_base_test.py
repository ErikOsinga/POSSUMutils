"""
Base class for setting up 3D Pipeline test cases. This is to reduce repetitions across test cases
because the setup is all the same.
"""
from datetime import datetime
import csv
import unittest
from abc import ABC
from automation import insert_database_script as db
from automation import database_queries as db_query

class _3DPipelineBaseTest(unittest.TestCase, ABC):
    def setUp(self):
        self.conn = db_query.get_database_connection(True)
        db.drop_test_schema(self.conn)
        db.create_test_schema(self.conn)
        db.create_tile_state_tables(self.conn)

        # Columns: tile_id, 3d_pipeline, 3d_pipeline_val, 3d_pipeline_ingest
        _3d_data = 'automation/unit_tests/csv/tile_state_band1.csv'
        # Open and stream rows
        with open(_3d_data, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            # Skip header
            next(reader)
            for row in reader:
                #tile_id, 3d_pipeline, 3d_pipeline_val, 3d_pipeline_ingest
                timestamp = None
                if isinstance(row[1], datetime):
                # the spreadsheet has mixed values (state and timestamp)
                # we're going to separate them
                    timestamp = row[1]
                _3d_val = row[2]
                db.insert_3d_pipeline_test_data(row[0], timestamp, _3d_val, row[3], self.conn)

    def tearDown(self):
        db.drop_test_tables(self.conn)
        db.drop_test_schema(self.conn)
        self.conn.close()
