"""
Base class for setting up Partial Tile 1D test cases. This is to reduce repetitions across test cases
because the setup is all the same.
"""
import csv
import unittest
from abc import ABC
from automation import insert_database_script as db
from automation import database_queries as db_query

class PartialTile1DBaseTest(unittest.TestCase, ABC):
    "Setup the tables for partial_tile 1D pipeline cases"
    def setUp(self):
        self.conn = db_query.get_database_connection(test=True)
        db.drop_test_schema(self.conn)
        db.create_test_schema(self.conn)
        db.create_observation_1d_relation_tables(self.conn)
        db.create_partial_tile_pipeline_tables(self.conn)
        # insert partial tile data
        # Columns: observation, sbid, tile1, tile2, tile3, tile4, type, number_sources, 1d_pipeline
        partial_tile_csv = 'automation/unit_tests/csv/partial_tile_1d_pipeline_band1.csv'
        # Open and stream rows
        with open(partial_tile_csv, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            # Skip header
            next(reader)
            for row in reader:
                # observation, sbid, tile1, tile2, tile3, tile4, type, number_sources, 1d_pipeline
                db.insert_row_into_partial_tile_table(row, '1', self.conn)
        # insert observation data: name, sbid, 1d_pipeline_validation, single_sb_1d_pipeline
        observation_csv = 'automation/unit_tests/csv/observation_1d_pipeline_band1.csv'
        # Open and stream rows
        with open(observation_csv, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            # Skip header
            next(reader)
            for row in reader:
                # name, sbid, 1d_pipeline_validation
                db_query.update_1d_pipeline_table(row[0], row[1], '1', row[2], "1d_pipeline_validation", self.conn)
                # name, sbid, single_sb_1d_pipeline
                db_query.update_1d_pipeline_table(row[0], row[1], '1', row[3], "single_sb_1d_pipeline", self.conn)

    def tearDown(self):
        if self.conn:
            db.drop_test_tables(self.conn)
            db.drop_test_schema(self.conn)
            self.conn.close()
            self.conn = None
