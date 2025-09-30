"""
Handy scripts to update the database with the Google spreadsheets data.
Usage: python3 insert_database_script.py
"""
import os
import gspread
import pandas as pd
from dotenv import load_dotenv
from automation import database_queries as db

GC = None
VALIDATION_SHEET = None
STATUS_SHEET = None

def create_partial_tile_pipeline_tables(conn):
    """
    Create the partial_tile_1d_pipeline tables in the database if it does not exist.
    Prevent duplicates by adding unique constraints
    (similar to how update_partialtile_google_sheet does check_validation_sheet_integrity).
    """
    for band_number in (1,2):
        sql = f"""
            CREATE TABLE IF NOT EXISTS possum.partial_tile_1d_pipeline_band{band_number} (
                id BIGSERIAL PRIMARY KEY,
                observation TEXT,
                sbid CHARACTER VARYING,
                tile1 BIGINT, tile2 BIGINT, tile3 BIGINT, tile4 BIGINT,
                type TEXT,
                number_sources INT,
                "1d_pipeline" TEXT,
                -- Generated column for uniqueness even for null values
                -- (observation, sbid, tile1, tile2, tile3, tile4, type) must be unique
                generated_key TEXT GENERATED ALWAYS AS (
                    COALESCE(observation, '') || '|' ||
                    COALESCE(sbid, '') || '|' ||
                    COALESCE(tile1::text, '-1') || '|' || COALESCE(tile2::text, '-1') || '|' ||
                    COALESCE(tile3::text, '-1') || '|' || COALESCE(tile4::text, '-1') || '|' ||
                    COALESCE(type, '')
                ) STORED,
                -- Unique constraint on the generated key
                CONSTRAINT unique_generated_key{band_number} UNIQUE (generated_key),
                CHECK (
                -- none of the tile is the same, except for empty values
                -- Only block rows where two tile values are equal and at least one of them is not ''
                    NOT (
                        (tile1 IS NOT NULL AND tile1 = tile2) OR
                        (tile1 IS NOT NULL AND tile1 = tile3) OR
                        (tile1 IS NOT NULL AND tile1 = tile4) OR
                        (tile2 IS NOT NULL AND tile2 = tile3) OR
                        (tile2 IS NOT NULL AND tile2 = tile4) OR
                        (tile3 IS NOT NULL AND tile3 = tile4)
                    )
                ),
                CHECK (
                -- if 4 tile numbers are present, type == corner or type==corner - crosses projection boundary!
                    NOT(
                        tile1 IS NOT NULL AND tile2 IS NOT NULL AND tile3 IS NOT NULL AND tile4 IS NOT NULL
                    ) OR LOWER(type) IN ('corner', 'corner - crosses projection boundary!')
                ),
                CHECK (
                -- if 2 tile numbers are present (the first two are not null, the last two are null), type==edge or type==edge - crosses projection boundary!
                    NOT(
                        tile1 IS NOT NULL AND tile2 IS NOT NULL AND tile3 IS NULL AND tile4 IS NULL
                    ) OR LOWER(type) IN ('edge', 'edge - crosses projection boundary!')
                ),
                CHECK (
                -- if 1 tile number is present type == center
                   NOT(
                       tile1 IS NOT NULL AND tile2 IS NULL AND tile3 IS NULL AND tile4 IS NULL
                   ) OR LOWER(type) = 'center'
                ));
        """
        db.execute_query(sql, conn, None)

def insert_partial_tile_data(db_connection):
    """Stream the Google Sheet into the database table
    """
    ps = GC.open_by_url(VALIDATION_SHEET)
    tile_sheet = ps.worksheet('Partial Tile Pipeline - regions - Band 1')
    tile_data = tile_sheet.get_all_values()
    # We only have data for band 1 for now
    band_number = 1

    for row in tile_data[1:]:  # Skip header row
        insert_row_into_partial_tile_table(row, band_number, db_connection)

def insert_row_into_partial_tile_table(row, band_number, db_conn):
    """
    Insert data into partial tile table
    """
    sql = f"""
            INSERT INTO possum.partial_tile_1d_pipeline_band{band_number}
            (observation, sbid, tile1, tile2, tile3, tile4, type, number_sources, "1d_pipeline")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            -- If row already exists, then don't overwrite
            ON CONFLICT (generated_key) DO NOTHING;
        """
    args = (
       row[0],  # observation
       row[1],  # sbid
       row[2] if row[2].isdigit() else None,  # tile_1,
       row[3] if row[3].isdigit() else None,  # tile_2
       row[4] if row[4].isdigit() else None,  # tile_3
       row[5] if row[5].isdigit() else None,  # tile_4
       row[6],  # type
       row[7] if row[7].isdigit() else None,  # number_sources
       row[8] if len(row) > 8 else None # 1d_pipeline
    )
    # We don't want to see thousands of insert statements
    db.execute_query(sql, db_conn, args)

def create_observation_1d_relation_tables(db_conn):
    """Create observation_1d_pipeline_band1 and observation_1d_pipeline_band2 tables.
       These tables hold 1d_pipeline_validation and single_SB_1D_pipeline columns
       per observation from the Google spreadsheets.
    """
    add_column_sql = """
        CREATE TABLE IF NOT EXISTS possum.observation_1d_pipeline_band{} (
            name TEXT PRIMARY KEY,
            sbid CHARACTER VARYING,
            "1d_pipeline_validation" TEXT,
            single_sb_1d_pipeline TEXT,
            comments TEXT
        );
    """
    # 1 table per band since the footprints are different
    db.execute_query(add_column_sql.format('1'), db_conn, None, verbose=True)

def insert_observation_1d_data_from_spreadsheet(db_conn):
    """
    Insert observation 1d data
    """
    #POSSUM Pipeline Validation: Partial Tile Pipeline - regions - Band 1: 1d_pipeline_validation
    ps = GC.open_by_url(VALIDATION_SHEET)
    band_number = '1'
    obs_sheet = ps.worksheet(f'Partial Tile Pipeline - regions - Band {band_number}')
    obs_data = pd.DataFrame(obs_sheet.get_all_records())
    # Drop repeated observation rows so don't have to reinsert and only take the last row
    observation_unique = obs_data.drop_duplicates(subset='field_name', keep='last')
    for row in observation_unique.values:  # Skip header row
        sql = f"""
            INSERT INTO possum.observation_1d_pipeline_band{band_number}
            (name, sbid, "1d_pipeline_validation")
            VALUES (%s, %s, %s)
            -- If row already exists, then don't overwrite
            ON CONFLICT (name) DO NOTHING;
       """
        args = (
           row[0],  # field_name
           row[1],  # sbid
           row[9] #1d_pipeline_validation
        )
        db.execute_query(sql, db_conn, args, verbose=False)

    #POSSUM Status Sheet: Survey Fields - Band 1: single_SB_1D_pipeline
    ps = GC.open_by_url(STATUS_SHEET)
    obs_sheet = ps.worksheet('Survey Fields - Band 1')
    obs_data = obs_sheet.get_all_values()
    for row in obs_data[1:]:  # Skip header row
        upsert_observation_single_sb_1d_pipeline(row, '1', db_conn)

    #POSSUM Status Sheet: Survey Fields - Band 2: single_SB_1D_pipeline
    obs_sheet = ps.worksheet('Survey Fields - Band 2')
    obs_data = obs_sheet.get_all_values()
    for row in obs_data[1:]:  # Skip header row
        upsert_observation_single_sb_1d_pipeline(row, '1', db_conn)

def upsert_observation_single_sb_1d_pipeline(row, band_number, db_conn):
    """
    Set possum.observation_1d_pipeline_band{band_number} table with single_SB_1D_pipeline
    value from the spreadsheet
    """
    sql = f"""
        INSERT INTO possum.observation_1d_pipeline_band{band_number}
        (name, sbid, single_sb_1d_pipeline)
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE
        SET single_sb_1d_pipeline = %s
    """
    args = (
       row[0], #name,
       row[15], #sbid,
       row[19], # single_SB_1D_pipeline
       row[19] # single_SB_1D_pipeline if the row already exists
    )
    db.execute_query(sql, db_conn, args, verbose=False)

def create_tile_3d_pipeline_tables(db_conn):
    """Create tile_3d_pipeline_band{band_number} to hold info per tile, based on Google spreadsheet columns:
    - tile_id
    - 3d_pipeline (status to check before running 3d pipeline)
    - 3d_pipeline_val (status to check before running 3d ingest)
    - 3d_pipeline_ingest (status to indicate whether or not 3d ingest has been run)
    - 3d_val_link (validation link for validator)
    - 3d_pipeline_validator (person who validates)
    - 3d_val_comments (comments from the validator)
    """
    for band_number in (1,2):
        sql = f"""
            CREATE TABLE IF NOT EXISTS possum.tile_3d_pipeline_band{band_number} (
                tile_id BIGSERIAL PRIMARY KEY,
                "3d_pipeline" TEXT,
                "3d_pipeline_val" TEXT,
                "3d_pipeline_ingest" TEXT,
                "3d_val_link" TEXT,
                "3d_pipeline_validator" TEXT,
                "3d_val_comments" TEXT);
            """
        db.execute_query(sql, db_conn, None, True)

def insert_3d_pipeline_data_from_spreadsheet(conn):
    """
    POSSUM Status Sheet: Survey Tiles - Band 1 (and 2): 3d_pipeline
    """
    ps = GC.open_by_url(STATUS_SHEET)
    for band_number in ('1','2'):
        tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
        tile_data = tile_sheet.get_all_values()
        for row in tile_data[1:]: #skip header row
            sql = f"""
                INSERT INTO possum.tile_3d_pipeline_band{band_number}
                (tile_id, "3d_pipeline")
                VALUES (%s, %s)
                ON CONFLICT(tile_id) DO UPDATE
                SET "3d_pipeline" = %s;
            """
            db.execute_query(sql, conn, (row[0], row[10], row[10]), verbose=False)

    #POSSUM Pipeline Validation: Survey Tiles - Band 1: 3d_pipeline_val, 3d_pipeline_ingest
    ps = GC.open_by_url(VALIDATION_SHEET)
    band_number = '1' # band 2 is not available yet
    tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    for row in tile_data[1:]: #skip header row
        sql = f"""
            INSERT INTO possum.tile_3d_pipeline_band{band_number}
            (tile_id, "3d_pipeline_val", "3d_val_link", "3d_pipeline_validator", "3d_val_comments", "3d_pipeline_ingest")
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(tile_id) DO UPDATE SET
            "3d_pipeline_val" = %s,
            "3d_val_link" = %s,
            "3d_pipeline_validator" = %s,
            "3d_val_comments" = %s,
            "3d_pipeline_ingest" = %s;
        """
        db.execute_query(sql, conn, (row[0], row[7], row[8], row[9], row[10], row[11],
                               row[7], row[8], row[9], row[10], row[11]), verbose=False)

### TEST METHODS ###

def insert_3d_pipeline_test_data(tile_id, _3d_pipeline, val, ingest, conn):
    """
    Insert test data into tile_3d_pipeline_band1 table
    """
    sql = """
        INSERT INTO possum.tile_3d_pipeline_band1
        (tile_id, "3d_pipeline", "3d_pipeline_val", "3d_pipeline_ingest")
        VALUES(%s, %s, %s, %s)
    """
    db.execute_query(sql, conn, (tile_id, _3d_pipeline, val, ingest))

def create_test_schema(conn):
    """
    Create possum schema for tests
    """
    sql = "CREATE SCHEMA IF NOT EXISTS possum;"
    db.execute_query(sql, conn)

def drop_test_schema(conn):
    """
    Drop possum schema in test DB
    """
    sql = "DROP SCHEMA IF EXISTS possum CASCADE;"
    db.execute_query(sql, conn)

def drop_test_tables(conn):
    """
    Drop all tables in test DB
    """
    tables = ['tile_3d_pipeline_band1', 'tile_3d_pipeline_band2',
              'observation_1d_pipeline_band1', 'observation_1d_pipeline_band2',
              'partial_tile_1d_pipeline_band1', 'partial_tile_1d_pipeline_band2']
    for table in tables:
        sql = f"DROP TABLE IF EXISTS possum.{table} CASCADE;"
        db.execute_query(sql, conn, None, True)

if __name__ == "__main__":
    load_dotenv(dotenv_path='automation/config.env')
    google_api_token = os.getenv('POSSUM_STATUS_TOKEN')
    GC = gspread.service_account(filename=google_api_token)
    VALIDATION_SHEET = os.getenv('POSSUM_PIPELINE_VALIDATION_SHEET')
    STATUS_SHEET = os.getenv('POSSUM_STATUS_SHEET')

    connection = db.get_database_connection(test=False)
    create_partial_tile_pipeline_tables(connection)
    insert_partial_tile_data(connection)
    create_observation_1d_relation_tables(connection)
    insert_observation_1d_data_from_spreadsheet(connection)
    create_tile_3d_pipeline_tables(connection)
    insert_3d_pipeline_data_from_spreadsheet(connection)
    connection.close()
