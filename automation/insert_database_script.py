"""
Handy scripts to update the database with the Google spreadsheets data.
Usage: python3 insert_database_script.py
"""
import os
import gspread
import pandas as pd
from dotenv import load_dotenv
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
    sql = """
        CREATE TABLE IF NOT EXISTS possum.partial_tile_1d_pipeline_band{} (
            id SERIAL PRIMARY KEY,
            observation TEXT,
            sbid CHARACTER VARYING, -- Not using INT because we need to allow '' instead of NULL to enable Unique constraint
            tile1 CHARACTER VARYING, -- Not using INT because we need to allow '' instead of NULL to enable Unique constraint
            tile2 CHARACTER VARYING, -- Not using INT because we need to allow '' instead of NULL to enable Unique constraint
            tile3 CHARACTER VARYING, -- Not using INT because we need to allow '' instead of NULL to enable Unique constraint
            tile4 CHARACTER VARYING, -- Not using INT because we need to allow '' instead of NULL to enable Unique constraint
            type TEXT,
            number_sources INT,
            "1d_pipeline" TEXT,
            UNIQUE (observation, sbid, tile1, tile2, tile3, tile4, type), -- Unique constraint to prevent duplicates
            CHECK (
            -- none of the tile is the same, except for empty values
            -- Only block rows where two tile values are equal and at least one of them is not ''
                NOT (
                    (tile1 <> '' AND LOWER(tile1) = LOWER(tile2)) OR
                    (tile1 <> '' AND LOWER(tile1) = LOWER(tile3)) OR
                    (tile1 <> '' AND LOWER(tile1) = LOWER(tile4)) OR
                    (tile2 <> '' AND LOWER(tile2) = LOWER(tile3)) OR
                    (tile2 <> '' AND LOWER(tile2) = LOWER(tile4)) OR
                    (tile3 <> '' AND LOWER(tile3) = LOWER(tile4))
                )
            ),
            CHECK (
            -- if 4 tile numbers are present, type == corner or type==corner - crosses projection boundary!
                NOT(
                    tile1 != '' AND tile2 != '' AND tile3 != '' AND tile4 != ''
                ) OR LOWER(type) IN ('corner', 'corner - crosses projection boundary!')
            ),
            CHECK (
            -- if 2 tile numbers are present (the first two are not null, the last two are null), type==edge or type==edge - crosses projection boundary!
                NOT(
                    tile1 != '' AND tile2 != '' AND tile3 = '' AND tile4 = ''
                ) OR LOWER(type) IN ('edge', 'edge - crosses projection boundary!')
            ),
            CHECK (
            -- if 1 tile number is present type == center
               NOT(
                   tile1 != '' AND tile2 = '' AND tile3 = '' AND tile4 = ''
               ) OR LOWER(type) = 'center'
            ));
    """
    # Create 2 separate tables for each band 1 and band 2
    db.execute_query(sql.format('1'))
    db.execute_query(sql.format('2'))

def insert_partial_tile_data():
    """Stream the Google Sheet into the database table
    """
    ps = GC.open_by_url(VALIDATION_SHEET)
    tile_sheet = ps.worksheet('Partial Tile Pipeline - regions - Band 1')
    tile_data = tile_sheet.get_all_values()
    # We only have data for band 1 for now
    band_number = 1

    for row in tile_data[1:]:  # Skip header row
        insert_row_into_partial_tile_table(row, band_number)

def insert_row_into_partial_tile_table(row, band_number):
    sql = f"""
            INSERT INTO possum.partial_tile_1d_pipeline_band{band_number}
            (observation, sbid, tile1, tile2, tile3, tile4, type, number_sources, "1d_pipeline")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            -- If row already exists, then don't overwrite
            ON CONFLICT (observation, sbid, tile1, tile2, tile3, tile4, type) DO NOTHING;
        """
    args = (
       row[0],  # observation
       row[1],  # sbid
       row[2] if row[2].isdigit() else '',  # tile_1
       row[3] if row[3].isdigit() else '',  # tile_2
       row[4] if row[4].isdigit() else '',  # tile_3
       row[5] if row[5].isdigit() else '',  # tile_4
       row[6],  # type
       row[7] if row[7].isdigit() else None,  # number_sources
       row[8] if len(row) > 8 else None # 1d_pipeline
    )
    # We don't want to see thousands of insert statements
    db.execute_query(sql, args, verbose=False)

def create_observation_1d_relation_tables():
    """Create observation_1d_pipeline_band1 and observation_1d_pipeline_band2 tables.
       These tables hold 1d_pipeline_validation and single_SB_1D_pipeline columns 
       per observation from the Google spreadsheets. 
    """
    add_column_sql = """
        CREATE TABLE IF NOT EXISTS possum.observation_1d_pipeline_band{} (
            name TEXT PRIMARY KEY,
            sbid CHARACTER VARYING,
            "1d_pipeline_validation" TEXT,
            single_SB_1D_pipeline TEXT
        );
    """
    # 1 table per band since the footprints are different
    db.execute_query(add_column_sql.format('1'))
    db.execute_query(add_column_sql.format('2'))
    
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
        db.execute_query(sql, args, verbose=False)
        
    #POSSUM Status Sheet: Survey Fields - Band 1: single_SB_1D_pipeline
    ps = GC.open_by_url(STATUS_SHEET)
    obs_sheet = ps.worksheet('Survey Fields - Band 1')
    obs_data = obs_sheet.get_all_values()
    for row in obs_data[1:]:  # Skip header row
        upsert_observation_single_sb_1d_pipeline(row, band_number='1')

    #POSSUM Status Sheet: Survey Fields - Band 2: single_SB_1D_pipeline
    obs_sheet = ps.worksheet('Survey Fields - Band 2')
    obs_data = obs_sheet.get_all_values()
    for row in obs_data[1:]:  # Skip header row
        upsert_observation_single_sb_1d_pipeline(row, band_number='2')

def upsert_observation_single_sb_1d_pipeline(row, band_number):
    """
    Set possum.observation_1d_pipeline_band{band_number} table with single_SB_1D_pipeline 
    value from the spreadsheet
    """
    sql = f"""
        INSERT INTO possum.observation_1d_pipeline_band{band_number}
        (name, sbid, single_SB_1D_pipeline)
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE 
        SET single_SB_1D_pipeline = %s
    """
    args = (
       row[0], #name,
       row[15], #sbid,
       row[19], # single_SB_1D_pipeline
       row[19] # single_SB_1D_pipeline if the row already exists
    )
    db.execute_query(sql, args, verbose=False)
    
def create_tile_3d_pipeline_table():
    """Create tile_3d_pipeline to hold info per tile, based on Google spreadsheet columns:
    - tile_id
    - 3d_pipeline_band1, 3d_pipeline_band2 (status to check before running 3d pipeline)
    - 3d_pipeline_val_band1, 3d_pipeline_val_band2 (status to check before running 3d ingest)
    - 3d_pipeline_ingest_band1, 3d_pipeline_ingest_band2 (status to indicate whether or not 3d ingest has been run)    
    """
    sql = """
        CREATE TABLE IF NOT EXISTS possum.tile_3d_pipeline (
            tile_id BIGINT PRIMARY KEY,
            "3d_pipeline_band1" TEXT,
            "3d_pipeline_band2" TEXT,
            "3d_pipeline_val_band1" TEXT,
            "3d_pipeline_val_band2" TEXT,
            "3d_pipeline_ingest_band1" TEXT,
            "3d_pipeline_ingest_band2" TEXT
        );
    """
    db.execute_query(sql)
    
    #POSSUM Status Sheet: Survey Tiles - Band 1 (and 2): 3d_pipeline
    ps = GC.open_by_url(STATUS_SHEET)
    for band_number in ('1','2'):
        tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
        tile_data = tile_sheet.get_all_values()
        for row in tile_data[1:]: #skip header row
            sql = f"""
                INSERT INTO possum.tile_3d_pipeline 
                (tile_id, "3d_pipeline_band{band_number}")
                VALUES (%s, %s)
                ON CONFLICT(tile_id) DO UPDATE
                SET "3d_pipeline_band{band_number}" = %s;
            """
            db.execute_query(sql, (row[0], row[10], row[10]), verbose=False)

    #POSSUM Pipeline Validation: Survey Tiles - Band 1: 3d_pipeline_val, 3d_pipeline_ingest
    ps = GC.open_by_url(VALIDATION_SHEET)
    band_number = '1' # band 2 is not available yet
    tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    for row in tile_data[1:]: #skip header row
        sql = f"""
            INSERT INTO possum.tile_3d_pipeline 
            (tile_id, "3d_pipeline_val_band{band_number}", "3d_pipeline_ingest_band{band_number}")
            VALUES (%s, %s, %s)
            ON CONFLICT(tile_id) DO UPDATE SET 
            "3d_pipeline_val_band{band_number}" = %s,
            "3d_pipeline_ingest_band{band_number}" = %s;
        """
        db.execute_query(sql, (row[0], row[7], row[11],row[7], row[11]), verbose=False)

if __name__ == "__main__":
    load_dotenv(dotenv_path='config.env')
    google_api_token = os.getenv('GOOGLE_API_TOKEN_PATH')
    GC = gspread.service_account(filename=google_api_token)
    VALIDATION_SHEET = os.getenv('POSSUM_PIPELINE_VALIDATION_SHEET')
    STATUS_SHEET = os.getenv('POSSUM_STATUS_SHEET')

    create_partial_tile_pipeline_tables()
    insert_partial_tile_data()
    create_observation_1d_relation_tables()
    create_tile_3d_pipeline_table()
