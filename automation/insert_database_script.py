"""
Handy scripts to update the database with the Google spreadsheets data.
Usage: python3 insert_database_script.py

Hopefully this will only have to be run once to set up the database tables.
After that, the google sheet is deprecated, and we move to the database as the source of truth.
"""

from datetime import datetime
import os
from psycopg2.extras import execute_batch
import gspread
import pandas as pd
from dotenv import load_dotenv
# workaround depending on where this script is called from
try:
    from automation import database_queries as db
except ModuleNotFoundError:
    import database_queries as db

GC = None
VALIDATION_SHEET = None
STATUS_SHEET = None

def create_partial_tile_pipeline_tables():
    """
    Create the partial_tile_1d_pipeline tables in the database if it does not exist.
    Prevent duplicates by adding unique constraints
    (similar to how update_partialtile_google_sheet does check_validation_sheet_integrity).
    """
    query_l = []
    for band_number in (1,2):
        sql = f"""
            CREATE TABLE IF NOT EXISTS possum.partial_tile_1d_pipeline_band{band_number} (
                id BIGSERIAL PRIMARY KEY,
                observation TEXT,
                tile1 BIGINT, tile2 BIGINT, tile3 BIGINT, tile4 BIGINT,
                type TEXT,
                number_sources INT,
                "1d_pipeline" TEXT,
                -- Generated column for uniqueness even for null values
                -- (observation, tile1, tile2, tile3, tile4, type) must be unique
                generated_key TEXT GENERATED ALWAYS AS (
                    COALESCE(observation, '') || '|' ||
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
        query_l.append((sql, None))
    return query_l

def insert_partial_tile_data():
    """Stream the Google Sheet into the database table
    """
    ps = GC.open_by_url(VALIDATION_SHEET)
    tile_sheet = ps.worksheet('Partial Tile Pipeline - regions - Band 1')
    tile_data = tile_sheet.get_all_values()
    # We only have data for band 1 for now
    band_number = 1
    sql = f"""
            INSERT INTO possum.partial_tile_1d_pipeline_band{band_number}
            (observation, tile1, tile2, tile3, tile4, type, number_sources, "1d_pipeline")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            -- If row already exists, then don't overwrite
            ON CONFLICT (generated_key) DO NOTHING;
        """
    sql_data = []
    for row in tile_data[1:]:  # Skip header row
        sql_data.append((
           row[0],  # observation
           row[2] if row[2].isdigit() else None,  # tile_1,
           row[3] if row[3].isdigit() else None,  # tile_2
           row[4] if row[4].isdigit() else None,  # tile_3
           row[5] if row[5].isdigit() else None,  # tile_4
           row[6],  # type
           row[7] if row[7].isdigit() else None,  # number_sources
           row[8] if len(row) > 8 else None # 1d_pipeline
        ))
    return sql, sql_data

def insert_row_into_partial_tile_table(row, band_number):
    """
    Insert data into partial tile table
    """
    sql = f"""
            INSERT INTO possum.partial_tile_1d_pipeline_band{band_number}
            (observation, tile1, tile2, tile3, tile4, type, number_sources, "1d_pipeline")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            -- If row already exists, then don't overwrite
            ON CONFLICT (generated_key) DO NOTHING;
        """
    args = (
       row[0],  # observation
       row[2] if row[2].isdigit() else None,  # tile_1,
       row[3] if row[3].isdigit() else None,  # tile_2
       row[4] if row[4].isdigit() else None,  # tile_3
       row[5] if row[5].isdigit() else None,  # tile_4
       row[6],  # type
       row[7] if row[7].isdigit() else None,  # number_sources
       row[8] if len(row) > 8 else None # 1d_pipeline
    )
    return (sql, args)

def create_observation_state_tables():
    """Create observation_state_band1 and observation_state_band2 tables.
       These tables hold 1d_pipeline_validation and single_SB_1D_pipeline columns
       per observation from the Google spreadsheets.
    """
    add_column_sql = """
        CREATE TABLE IF NOT EXISTS possum.observation_state_band{} (
            name TEXT PRIMARY KEY,
            sbid CHARACTER VARYING,
            "1d_pipeline_validation" TEXT,
            single_sb_1d_pipeline TEXT,
            comments TEXT,
            mfs_update TIMESTAMP WITHOUT TIME ZONE,
            mfs_state TEXT,
            cube_update TIMESTAMP WITHOUT TIME ZONE,
            cube_state TEXT,
            cube_sent BOOLEAN DEFAULT FALSE,
            mfs_sent BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (sbid) REFERENCES possum.observation(sbid)
        );
    """
    # 1 table per band since the footprints are different
    return [(add_column_sql.format(band), None) for band in (1,2)]

def insert_observation_1d_data_from_spreadsheet():
    """
    Insert observation 1d data
    """
    query_l = []
    data_l = []
    #POSSUM Pipeline Validation: Partial Tile Pipeline - regions - Band 1: 1d_pipeline_validation
    ps = GC.open_by_url(VALIDATION_SHEET)
    band_number = '1'
    obs_sheet = ps.worksheet(f'Partial Tile Pipeline - regions - Band {band_number}')
    obs_data = pd.DataFrame(obs_sheet.get_all_records())
    # Drop repeated observation rows so don't have to reinsert and only take the last row
    observation_unique = obs_data.drop_duplicates(subset='field_name', keep='last')
    sql = f"""
            INSERT INTO possum.observation_state_band{band_number}
            (name, "1d_pipeline_validation")
            VALUES (%s, %s)
            -- If row already exists, then don't overwrite
            ON CONFLICT (name) DO UPDATE
            SET "1d_pipeline_validation" = %s;
       """
    query_l.append(sql)
    sql_data = []
    for row in observation_unique.values:  # Skip header row
        sql_data.append((
           row[0],  # field_name
           row[9], #1d_pipeline_validation
           row[9]  #1d_pipeline_validation if the row already exists
        ))
    data_l.append(sql_data)

    #POSSUM Status Sheet: Survey Fields - Band 1: single_SB_1D_pipeline
    for band_number in (1,2):
        sql = f"""
             INSERT INTO possum.observation_state_band{band_number}
             (name, single_sb_1d_pipeline)
             VALUES (%s, %s)
             ON CONFLICT (name) DO UPDATE
             SET single_sb_1d_pipeline = %s
         """
        query_l.append(sql)
    ps = GC.open_by_url(STATUS_SHEET)
    obs_sheet = ps.worksheet('Survey Fields - Band 1')
    obs_data = obs_sheet.get_all_values()
    sql_data = []
    for row in obs_data[1:]:  # Skip header row
        sql_data.append((
            row[0], row[19], row[19]
        )) #name, single_SB_1D_pipeline, single_SB_1D_pipeline if the row already exists
    data_l.append(sql_data)

    #POSSUM Status Sheet: Survey Fields - Band 2: single_SB_1D_pipeline
    obs_sheet = ps.worksheet('Survey Fields - Band 2')
    obs_data = obs_sheet.get_all_values()
    sql_data = []
    for row in obs_data[1:]:  # Skip header row
        sql_data.append((
            row[0], row[19], row[19]
        )) #name, single_SB_1D_pipeline, single_SB_1D_pipeline if the row already exists
    data_l.append(sql_data)

    return query_l, data_l

def create_tile_state_tables():
    """Create tile_state_band{band_number} to hold info per tile, based on Google spreadsheet columns:
    - tile_id
    - 3d_pipeline (status to check before running 3d pipeline)
    - 3d_pipeline_val (status to check before running 3d ingest)
    - 3d_pipeline_ingest (status to indicate whether or not 3d ingest has been run)
    - 3d_val_link (validation link for validator)
    - 3d_pipeline_validator (person who validates)
    - 3d_val_comments (comments from the validator)
    """
    query_l = []
    for band_number in (1,2):
        sql = f"""
            CREATE TABLE IF NOT EXISTS possum.tile_state_band{band_number} (
                tile BIGSERIAL PRIMARY KEY,
                "3d_pipeline" TIMESTAMP WITHOUT TIME ZONE,
                "3d_pipeline_val" TEXT,
                "3d_pipeline_ingest" TEXT,
                "3d_val_link" TEXT,
                "3d_pipeline_validator" TEXT,
                "3d_val_comments" TEXT,
                mfs_sent BOOLEAN DEFAULT FALSE,
                cube_sent BOOLEAN DEFAULT FALSE,
                cube_state TEXT,
                mfs_state TEXT);
            """
        query_l.append((sql, None))
    return query_l

def insert_3d_pipeline_data_from_spreadsheet():
    """
    POSSUM Status Sheet: Survey Tiles - Band 1 (and 2): 3d_pipeline
    """
    query_l = []
    data_l = []
    ps = GC.open_by_url(STATUS_SHEET)
    for band_number in ('1','2'):
        tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
        tile_data = tile_sheet.get_all_values()
        sql = f"""
                INSERT INTO possum.tile_state_band{band_number}
                (tile, "3d_pipeline")
                VALUES (%s, %s)
                ON CONFLICT(tile) DO UPDATE
                SET "3d_pipeline" = %s;
            """
        query_l.append(sql)
        timestamp_data = []
        for row in tile_data[1:]: #skip header row
            if isinstance(row[10], datetime):
                # the spreadsheet has mixed values (state and timestamp)
                # we're going to separate them
                timestamp_data.append((row[0], row[10], row[10]))
        data_l.append(timestamp_data)

    #POSSUM Pipeline Validation: Survey Tiles - Band 1: 3d_pipeline_val, 3d_pipeline_ingest
    ps = GC.open_by_url(VALIDATION_SHEET)
    band_number = '1' # band 2 is not available yet
    tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    # data without 3d_pipeline_val
    sql = f"""
            INSERT INTO possum.tile_state_band{band_number}
            (tile, "3d_val_link", "3d_pipeline_validator", "3d_val_comments", "3d_pipeline_ingest")
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(tile) DO UPDATE SET
            "3d_val_link" = %s,
            "3d_pipeline_validator" = %s,
            "3d_val_comments" = %s,
            "3d_pipeline_ingest" = %s;
        """
    query_l.append(sql)
    # data with 3d_pipeline_val
    sql = f"""
            INSERT INTO possum.tile_state_band{band_number}
            (tile, "3d_pipeline_val", "3d_val_link", "3d_pipeline_validator", "3d_val_comments", "3d_pipeline_ingest")
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(tile) DO UPDATE SET
            "3d_pipeline_val" = %s,
            "3d_val_link" = %s,
            "3d_pipeline_validator" = %s,
            "3d_val_comments" = %s,
            "3d_pipeline_ingest" = %s;
        """
    query_l.append(sql)
    no_validation_data = []
    validation_data = []
    for row in tile_data[1:]: #skip header row
        _3d_pipeline_val = row[7]
        if _3d_pipeline_val is None or _3d_pipeline_val.strip() == '':
            #Don't overwrite existing value with empty value
            no_validation_data.append((row[0], row[8], row[9], row[10], row[11], row[8],
                               row[9], row[10], row[11]))
        else:
            validation_data.append((row[0], _3d_pipeline_val, row[8], row[9], row[10], row[11],
                               _3d_pipeline_val, row[8], row[9], row[10], row[11]))
    data_l.append(no_validation_data)
    data_l.append(validation_data)
    return query_l, data_l

def upsert_observation_state_columns():
    """
    Add state columns to observation_state_band1 and observation_state_band2 tables
    """
    query_l = []
    for band_number in (1,2):
        sql = f"""
            INSERT INTO possum.observation_state_band{band_number} (
              name,
              cube_sent, mfs_sent,
              cube_state, mfs_state,
              cube_update, mfs_update)
            SELECT obs.name,
              obs.cube_sent, obs.mfs_sent,
              obs.cube_state, obs.mfs_state,
              obs.cube_update, obs.mfs_update
            FROM possum.observation obs
            WHERE obs.band = {band_number}
            ON CONFLICT (name) DO UPDATE
            SET cube_sent = EXCLUDED.cube_sent,
                mfs_sent = EXCLUDED.mfs_sent,
                cube_state = EXCLUDED.cube_state,
                mfs_state = EXCLUDED.mfs_state,
                cube_update = EXCLUDED.cube_update,
                mfs_update = EXCLUDED.mfs_update;
        """
        query_l.append((sql, None))
    return query_l

def upsert_tile_state_columns():
    """
    Add state columns to tile_state_band1 and tile_state_band2 tables
    """
    query_l = []
    for band_number in (1,2):
        sql = f"""
            INSERT INTO possum.tile_state_band{band_number} (
              tile,
              cube_sent, mfs_sent,
              cube_state, mfs_state)
            SELECT tile.tile,
              tile.band{band_number}_cube_sent, tile.band{band_number}_mfs_sent,
              tile.band{band_number}_cube_state, tile.band{band_number}_mfs_state
            FROM possum.tile tile
            ON CONFLICT (tile) DO UPDATE
            SET cube_sent = EXCLUDED.cube_sent,
                mfs_sent = EXCLUDED.mfs_sent,
                cube_state = EXCLUDED.cube_state,
                mfs_state = EXCLUDED.mfs_state;
        """
        query_l.append((sql, None))
    return query_l

def delete_original_state_columns():
    """
    Delete original state columns from observation and tile tables
    as they have been copied to observation_state_band{1,2} and tile_state_band{1,2} tables
    """
    sql = """
            ALTER TABLE possum.observation
            DROP COLUMN IF EXISTS cube_state,
            DROP COLUMN IF EXISTS mfs_state,
            DROP COLUMN IF EXISTS cube_sent,
            DROP COLUMN IF EXISTS mfs_sent,
            DROP COLUMN IF EXISTS mfs_update,
            DROP COLUMN IF EXISTS cube_update;
        """
    return ((sql, None))

### TEST METHODS ###

def create_observation_test_table():
    """
    Create possum.observation table for tests
    """
    sql = """
        CREATE TABLE IF NOT EXISTS possum.observation (
            name TEXT PRIMARY KEY,
            sbid CHARACTER VARYING UNIQUE
        );
    """
    return (sql, None)

def create_associated_tile_test_table(conn):
    """
    Create possum.associated_tile table for tests
    """
    sql = """
        CREATE TABLE IF NOT EXISTS possum.associated_tile (
            id BIGSERIAL PRIMARY KEY,
            name TEXT,
            tile BIGINT
        );
    """
    db.execute_query(sql, conn, None)

def insert_observation_row(name, sbid, conn):
    """
    Insert a row into possum.observation table for tests
    """
    sql = """
        INSERT INTO possum.observation (name, sbid)
        VALUES (%s, %s);
    """
    return db.execute_query(sql, conn, (name, sbid))

def insert_observation_state_data(name, band_number, _1d_val, single_sb, cube_state, conn):
	"""
	Insert test data into possum.observation_state_band1 table
	"""
	sql = f"""
		INSERT INTO possum.observation_state_band{band_number}
		(name, "1d_pipeline_validation", single_sb_1d_pipeline, cube_state)
		VALUES(%s, %s, %s, %s)
	"""
	return db.execute_query(sql, conn, (name, _1d_val, single_sb, cube_state))
	
def insert_associated_tile_data(name, tile, conn):
    """
    Insert test data into possum.associated_tile table
    """
    sql = """
        INSERT INTO possum.associated_tile (name, tile)
        VALUES (%s, %s);
    """
    return db.execute_query(sql, conn, (name, tile))

def insert_3d_pipeline_test_data(tile_id, _3d_pipeline, val, ingest, conn):
    """
    Insert test data into tile_state_band1 table
    """
    sql = """
        INSERT INTO possum.tile_state_band1
        (tile, "3d_pipeline", "3d_pipeline_val", "3d_pipeline_ingest")
        VALUES(%s, %s, %s, %s)
    """
    return db.execute_query(sql, conn, (tile_id, _3d_pipeline, val, ingest))

def create_test_schema():
    """
    Create possum schema for tests
    """
    sql = "CREATE SCHEMA IF NOT EXISTS possum;"
    return (sql, None)

def drop_test_schema():
    """
    Drop possum schema in test DB
    """
    sql = "DROP SCHEMA IF EXISTS possum CASCADE;"
    return (sql, None)

def drop_test_tables():
    """
    Drop all tables in test DB
    """
    tables = ['tile_state_band1', 'tile_state_band2', 'observation',
              'observation_state_band1', 'observation_state_band2',
              'partial_tile_1d_pipeline_band1', 'partial_tile_1d_pipeline_band2']
    sql_list = []
    for table in tables:
        sql = f"DROP TABLE IF EXISTS possum.{table} CASCADE;"
        sql_list.append((sql, None))
    return sql_list

if __name__ == "__main__":
    load_dotenv(dotenv_path='./automation/config.env')
    google_api_token = os.getenv('POSSUM_STATUS_TOKEN')
    GC = gspread.service_account(filename=google_api_token)
    VALIDATION_SHEET = 'https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k'
    STATUS_SHEET = os.getenv('POSSUM_STATUS_SHEET')

    connection = db.get_database_connection(test=False)
    queries = []

    if False:
        print("This code changes the database schema and inserts data from the Google spreadsheets.")
        print("But since we have already done that, the code is now disabled to prevent accidental re-runs.")
        queries.extend(create_partial_tile_pipeline_tables())
        queries.extend(create_observation_state_tables())
        queries.extend(upsert_observation_state_columns())
        queries.extend(create_tile_state_tables())
        queries.extend(upsert_tile_state_columns())
        queries.append(delete_original_state_columns())
    
    # using with statement to auto commit and rollback if there's exception
    with connection:
        # this is in case we do need to create the tables again
        for query in queries:
            db.execute_query(query[0], connection, query[1], True)
        
        # stream data into the tables
        query, data = insert_partial_tile_data()
        execute_batch(connection.cursor(), query, data)
        query_list, data_list = insert_observation_1d_data_from_spreadsheet()
        for query, data in zip(query_list, data_list):
            execute_batch(connection.cursor(), query, data)
        query_list, data_list = insert_3d_pipeline_data_from_spreadsheet()
        for query, data in zip(query_list, data_list):
            execute_batch(connection.cursor(), query, data)
    
    connection.close()

    print("Database update complete.")