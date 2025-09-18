"""
Handy scripts to update the database with the Google spreadsheets data.
Usage: python3 insert_database_script.py
"""
import os
import gspread
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
        );
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
        sql = f"""
            INSERT INTO possum.partial_tile_1d_pipeline_band{band_number}
            (observation, sbid, tile1, tile2, tile3, tile4, type, number_sources, "1d_pipeline")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        db.execute_query(sql, args)

def create_observation_1d_relation_tables():
    """Add additional columns to the observation table if they do not exist"""
    add_column_sql = """
        CREATE TABLE IF NOT EXISTS possum.observation_1d_pipeline_band{} (
            name PRIMARY KEY,
            sbid CHARACTER VARYING,
            1d_pipeline_validation TEXT,
            single_SB_1D_pipeline TEXT
        );
    """
    # 1 table per band since the footprints are different
    db.execute_query(add_column_sql.format('1'))
    db.execute_query(add_column_sql.format('2'))
    #POSSUM Status Sheet: Survey Fields - Band 1: single_SB_1D_pipeline
    ps = GC.open_by_url(STATUS_SHEET)
    tile_sheet = ps.worksheet('Survey Fields - Band 1')
    tile_data = tile_sheet.get_all_values()
    for row in tile_data[1:]:  # Skip header row
        set_observation_single_sb_1d_pipeline(row, band_number='1')

    #POSSUM Status Sheet: Survey Fields - Band 2: single_SB_1D_pipeline
    tile_sheet = ps.worksheet('Survey Fields - Band 2')
    tile_data = tile_sheet.get_all_values()
    for row in tile_data[1:]:  # Skip header row
        set_observation_single_sb_1d_pipeline(row, band_number='2')

    #POSSUM Pipeline Validation: Partial Tile Pipeline - regions - Band 1: 1d_pipeline_validation
    ps = GC.open_by_url(VALIDATION_SHEET)
    tile_sheet = ps.worksheet('Partial Tile Pipeline - regions - Band 1')
    tile_data = tile_sheet.get_all_values()
    band_number = '1'
    for row in tile_data[1:]:  # Skip header row
        if row[9] == '': # Skip empty cells
            continue
        sql = f"""
            UPDATE possum.observation_1d_pipeline_band{band_number} o
            SET "1d_pipeline_validation" = %s
            FROM possum.partial_tile_1d_pipeline_band{band_number} pt
            WHERE o.name= %s and o.name = pt.observation
            and pt.sbid = %s and pt.tile1 = %s
            and pt.tile2 = %s and pt.tile3 = %s and pt.tile4 = %s;
       """
        args = (
           row[9],  # 1d_pipeline_validation
           row[0],  # observation
           row[1],  # sbid
           row[2] if row[2].isdigit() else '',  # tile_1
           row[3] if row[3].isdigit() else '',  # tile_2
           row[4] if row[4].isdigit() else '',  # tile_3
           row[5] if row[5].isdigit() else ''   # tile_4
        )
        db.execute_query(sql, args)

def set_observation_single_sb_1d_pipeline(row, band_number):
    """
    Set single_SB_1d_pipeline_band{band_number} column in possum.observation table.
    """
    if row[19] == '': # Skip empty cells
        return
    sql = f"""
        INSERT INTO possum.observation_1d_pipeline_band{band_number}
        SET single_SB_1D_pipeline = %s
        WHERE name = %s;
        """
    args = (
       row[19],  # single_SB_1D_pipeline
       row[0]  # name
    )
    db.execute_query(sql, args)

def update_tile_table():
    """Add additional columns to the tile table if they do not exist
       and populate data from Google Sheets
    """
    add_column_sql = """
        ALTER TABLE possum.tile
        ADD COLUMN IF NOT EXISTS "3d_pipeline_band1_status" TEXT,
        ADD COLUMN IF NOT EXISTS "3d_pipeline_band2_status" TEXT;
    """
    db.execute_query(add_column_sql)
    #POSSUM Status Sheet: Survey Tiles - Band 1: 3d_pipeline
    ps = GC.open_by_url(STATUS_SHEET)
    tile_sheet = ps.worksheet('Survey Tiles - Band 1')
    tile_data = tile_sheet.get_all_values()
    for row in tile_data[1:]:  # Skip header row
        set_tile_3d_pipeline(row, band_number='1')
    #POSSUM Status Sheet: Survey Tiles - Band 2: 3d_pipeline
    tile_sheet = ps.worksheet('Survey Tiles - Band 2')
    tile_data = tile_sheet.get_all_values()
    for row in tile_data[1:]:  # Skip header row
        set_tile_3d_pipeline(row, band_number='2')

def set_tile_3d_pipeline(row, band_number):
    """
    Update 3d_pipeline_band{band_number} status in the database
    """
    if row[10] == '': # Skip empty cells
        return
    sql = f"""
        UPDATE possum.tile
        SET "3d_pipeline_band{band_number}_status" = %s
        WHERE tile = %s;
    """
    args = (
       row[10],  # 3d_pipeline
       row[0]  # tile_id
    )
    db.execute_query(sql, args)

if __name__ == "__main__":
    load_dotenv(dotenv_path='config.env')
    google_api_token = os.getenv('GOOGLE_API_TOKEN_PATH')
    GC = gspread.service_account(filename=google_api_token)
    VALIDATION_SHEET = os.getenv('POSSUM_PIPELINE_VALIDATION_SHEET')
    STATUS_SHEET = os.getenv('POSSUM_STATUS_SHEET')

    create_partial_tile_pipeline_tables()
    insert_partial_tile_data()
    create_observation_1d_relation_tables()
    update_tile_table()
