"""
Database query functions for interacting with the ausSRC database.
"""
import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

def execute_query(query, params=None):
    """
    Execute a SQL query and return the results.

    Args:
    query (str): The SQL query to execute.
    params (tuple): Optional parameters for the SQL query.

    Returns:
    list: The results of the query.
    """
    # Get database connection details from config.env file
    load_dotenv(dotenv_path='config.env')
    conn_params = {
        'dbname': os.getenv('DATABASE_NAME'),
        'user': os.getenv('DATABASE_USER'),
        'password': os.getenv('DATABASE_PASSWORD'),
        'host': os.getenv('DATABASE_HOST'),
        'port': os.getenv('DATABASE_PORT')
    }

    try:
        # Connect to the database
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Execute the query
        if params:
            print(f"Executing database query: {query % params}")
        else:
            print(f"Executing database query: {query}")
        cursor.execute(query, params)

        # Fetch all results
        results = []
        if cursor.description is not None:
            results = cursor.fetchall()

        # Commit changes if it's an insert/update
        if query.strip().lower().startswith(('insert', 'update', 'create', 'alter')):
            conn.commit()

        return results
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_3d_pipeline_status(tile_number, band, status):
    """
    Update the 'tile' table, setting either '3d_pipeline_band1' or '3d_pipeline_band2'
    with given status. Possible values:
        - Running (Job has been submitted and is currently running)
        - WaitingForValidation (Job has finished running successfully, waiting for human validation)
        - Good/Bad (Currently has to be manually set after human validation)
        - Failed (Job failed to run)

    Args:
    tile_number (str): The tile number to update.
    band (str): The band of the tile.
    status (str): The status to set in the respective column.
    """
    validate_band_number(band)
    column_name = "3d_pipeline_band2_status" if band == '2' else "3d_pipeline_band1_status"
    print(f"Updating POSSUM tile database table with {column_name} to {status}")
    query = f"""
        UPDATE possum.tile
        SET "{column_name}" = %s
        WHERE tile = %s;
    """
    #TODO: update the table with existing values from google sheet
    params = (status, tile_number)
    execute_query(query, params)

def update_1d_pipeline_validation_status(field_name, sbid, band_number, status):
    """
    Update the 1d_pipeline_validation_status column in the observation table.
    This is to replace updated POSSUM pipeline validation Google sheet: Partial Tile Pipeline - regions - Band 1,
    1d_pipeline_validation column.

    Args:
    field_name       : observation.field_name
    sbid             : observation.sbid
    band_number      : '1' (only 1 is supported currently)
    status (str): The status to set in the 'status_column' column.

    """
    print("Updating POSSUM observation table with 1D pipeline summary plot status")
    validate_band_number(band_number)
    column_name = "1d_pipeline_validation_band2" if band_number == '2' else "1d_pipeline_validation_band1"
    query = f"""
        UPDATE possum.observation
        SET {column_name} = %s
        WHERE field_name = %s AND sbid = %s;
    """
    params = (status, field_name, sbid)
    execute_query(query, params)

def update_single_SB_1d_pipeline_status(field_name, sbid, band_number, status):
    """
    Update the single_1d_pipeline_validation{band_number} column in the observation table.
    This is to the equivalent to POSSUM pipeline status sheet: Survey Fields - Band {band_number}

    Args:
    field_name       : observation.field_name
    sbid             : observation.sbid
    band_number      : '1' or '2'
    status (str): The status to set in the 'status_column' column.

    """
    validate_band_number(band_number)
    column_name = "single_SB_1D_pipeline_band1" if band_number == '2' else "single_SB_1D_pipeline_band2"
    print(f"Updating POSSUM observation table with {column_name} status")
    query = f"""
        UPDATE possum.observation
        SET {column_name} = %s
        WHERE field_name = %s AND sbid = %s;
    """
    params = (status, field_name, sbid)
    execute_query(query, params)

def find_boundary_issues(sbid, observation):
    """
    Check if there are any entries in partial_tile_1d_pipeline for the given sbid and observation
    where type indicates it crosses a projection boundary.
    This is to identify potential issues with tiles that cross projection boundaries.
    """
    print(f"Checking for projection boundary issues for SBID: {sbid}, Observation: {observation}")
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM possum.partial_tile_1d_pipeline
            WHERE sbid = %s AND observation = %s AND type like '%%crosses projection boundary%%'
        ) AS match_found;
    """
    params = (sbid, observation)
    results = execute_query(query, params)
    issues_found = results[0][0]
    if issues_found:
        print("Boundary issues found.")
    else:
        print("No boundary issues found.")
    return issues_found

def validate_band_number(band_number):
    if band_number not in ["1", "2"]:
        raise ValueError("band_number must be either 1 or 2")

def get_tiles_for_pipeline_run(band_number):
    """
    Get a list of tile numbers that should be ready to be processed by the 3D pipeline

    In the database, this is when:
    observation.cube_state = 'COMPLETED' and tile.3d_pipeline_band{band_number}_status = [null]
    In POSSUM pipeline status sheet, this is the equivalent of:
    'aus_src' column is not empty and '3d_pipeline' column is empty for the given band number.

    Args:
    band_number (int): The band number (1 or 2) to check.

    Returns:
    list: A list of tile numbers that satisfy the conditions.
    """
    validate_band_number(band_number)
    column_name = "3d_pipeline_band2_status" if band_number == 2 else "3d_pipeline_band1_status"
    print(f"Fetching tiles ready for 3D pipeline run for band {band_number} from the database.")
    query = f"""
        SELECT DISTINCT tile.tile
        FROM possum.tile
        INNER JOIN possum.associated_tile
        ON associated_tile.tile = tile.tile
        INNER JOIN possum.observation
        ON associated_tile.name = observation.name
        WHERE observation.cube_state = 'COMPLETED' AND tile."{column_name}" IS NULL;
    """
    #TODO: update the table with existing values from google sheet
    return execute_query(query)

def update_partial_tile_1d_pipeline_status(field_id, tile_numbers, band_number, status):
    """
    Update 1d_pipeline_band_{band_number} status for a set of tiles in the partial_tile_1d_pipeline table.
    This replaces the 1d_pipeline column in the POSSUM pipeline validation Google sheet:
    Partial Tile Pipeline - regions - Band {band_number}

    Args:
    field_ID (str): The field ID.
    tile_numbers (tuple): The tile numbers to update.
    status (str): The new validation status to set.
    """
    print(f"Updating POSSUM partial_tile_1d_pipeline.1d_pipeline_band_{band_number} in the database")
    t1, t2, t3, t4 = tile_numbers
    query = f"""
        UPDATE possum.partial_tile_1d_pipeline
        SET 1d_pipeline_band{band_number} = %s
        WHERE field_ID = %s AND tile1 = %s AND tile2 = %s AND tile3 = %s AND tile4 = %s;
    """
    params = (status, field_id, t1, t2, t3, t4)
    return execute_query(query, params)

def get_partial_tiles_for_1d_pipeline_run(band_number):
    """
    Get a list of partial tiles that should be ready to be processed by the 1D pipeline
    In the database, this is when:
    partial_tile_1d_pipeline.sbid is not NULL, number_sources is not NULL,
    and 1d_pipeline_band_{band_number} is NULL

    Args:
    band_number (int): The band number (1 or 2) to check.

    Returns:
    list: A list of tuples containing (field_ID, sbid, (tile1, tile2, tile3, tile4))
    that satisfy the conditions.
    """
    validate_band_number(band_number)
    print(f"""Fetching partial tiles ready for 1D pipeline run for band {band_number}
          from the database.""")
    query = f"""
        SELECT pt.observation, pt.sbid, pt.tile1, pt.tile2, pt.tile3, pt.tile4
        FROM possum.partial_tile_1d_pipeline pt
        WHERE pt.sbid IS NOT NULL
          AND pt.number_sources IS NOT NULL
          AND pt."1d_pipeline_band{band_number}" IS NULL;
    """
    return execute_query(query)

#    def check_3d_pipeline_complete():
#        SELECT *
#FROM possum.tile
#where "3d_pipeline_band1"::text is not null

def update_aussrc_status(tile_number, band):
    """
    Update the status of a tile in the database.

    Args:
    tile_number (str): The tile number to update.
    band (str): The band of the tile.
    """
    query = """
        UPDATE tile
        SET "aus_src" = %s
        WHERE tile = %s;
    """
    #make sure to do this to local copy
    # au