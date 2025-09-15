"""
Handy scripts to update the database with the Google spreadsheets data.
"""
import os
import gspread
from dotenv import load_dotenv
import database_queries as db


def create_partial_tile_pipeline_table():
    """
    Create the partial_tile_1d_pipeline table in the database if it does not exist.
    """
    sql = """
        CREATE TABLE IF NOT EXISTS possum.partial_tile_1d_pipeline (
            id SERIAL PRIMARY KEY,
            observation TEXT,
            sbid CHARACTER VARYING,
            tile_1 BIGINT,
            tile_2 BIGINT,
            tile_3 BIGINT,
            tile_4 BIGINT,
            type TEXT,
            number_sources INT,
            "1d_pipeline" TEXT
        );
    """
    db.execute_query(sql)

def insert_partial_tile_data():
    """Stream the Google Sheet into the database table
    """
    load_dotenv(dotenv_path='config.env')
    google_api_token = os.getenv('GOOGLE_API_TOKEN_PATH')
    validation_sheet = os.getenv('POSSUM_PIPELINE_VALIDATION_SHEET')
    gc = gspread.service_account(filename=google_api_token)
    ps = gc.open_by_url(validation_sheet)
    tile_sheet = ps.worksheet('Partial Tile Pipeline - regions - Band 1')
    tile_data = tile_sheet.get_all_values()

    for row in tile_data[1:]:  # Skip header row
        sql = """
            INSERT INTO possum.partial_tile_1d_pipeline
            (observation, sbid, tile_1, tile_2, tile_3, tile_4, type, number_sources, "1d_pipeline")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        args = (
           row[0],  # observation
           row[1],  # sbid
           row[2] if row[2].isdigit() else None,  # tile_1
           row[3] if row[3].isdigit() else None,  # tile_2
           row[4] if row[4].isdigit() else None,  # tile_3
           row[5] if row[5].isdigit() else None,  # tile_4
           row[6],  # type
           row[7] if row[7].isdigit() else None,  # number_sources
           row[8] if len(row) > 8 else None  # 1d_pipeline
        )
        db.execute_query(sql, args)

if __name__ == "__main__":
    """Main function to create the table and insert data.
    """
    create_partial_tile_pipeline_table()
    insert_partial_tile_data()
    