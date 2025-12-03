"""
This is the base class to test database queries
"""
import unittest
from automation import database_queries as db_query
from automation import insert_database_script as db

class DatabaseTestBase(unittest.TestCase):
    "Base class for database related tests, to make setting up connections easier."

    def setUp(self):
        test_flag = True
        assert test_flag is True, "This test class should only be run against a test database!"
        self.conn = db_query.get_database_connection(test_flag)

    def tearDown(self):
        sql = []
        sql.extend(db.drop_test_tables())
        sql.append(db.drop_test_schema())
        # using with statement to auto commit and rollback if there's exception
        with self.conn:
            for query in sql:
                db_query.execute_query(query[0], self.conn, query[1], True)

        self.conn.close()
