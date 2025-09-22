"""
Database query functions for interacting with the ausSRC database.
"""
import os
import psycopg2
from dotenv import load_dotenv

def execute_query(query, params=None, verbose=True):
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
        if verbose:
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

def update_3d_pipeline_ingest(tile_number, band_number, status):
    """
    Update the 'tile_3d_pipeline' table, setting either '3d_pipeline_ingest_band1' 
    or '3d_pipeline_ingest_band2' with given status. Possible values:
        - Ingested
        - IngestFailed
        _ IngestRunning

    Args:
    tile_number (str): The tile number to update.
    band_number (str): 1 or 2
    status (str): The status to set in the respective column.
    """
    validate_band_number(band_number)
    print(f"Updating POSSUM tile database table with 3d_pipeline_ingest_band{band_number} to {status}")
    query = f"""
        UPDATE possum.tile_3d_pipeline
        SET "3d_pipeline_ingest_band{band_number}" = %s
        WHERE tile_id = %s;
    """
    params = (status, tile_number)
    execute_query(query, params)

def update_3d_pipeline_val(tile_number, band_number, status):
    """
    Update the 'tile_3d_pipeline' table, setting either '3d_pipeline_val_band1' 
    or '3d_pipeline_val_band2' with given status. Possible values:
        - WaitingForValidation (Job has finished running successfully, waiting for human validation)
        - Good/Bad (Currently has to be manually set after human validation)

    Args:
    tile_number (str): The tile number to update.
    band_number (str): 1 or 2
    status (str): The status to set in the respective column.
    """
    validate_band_number(band_number)
    print(f"Updating POSSUM tile database table with 3d_pipeline_val_band{band_number} to {status}")
    query = f"""
        UPDATE possum.tile_3d_pipeline
        SET "3d_pipeline_val_band{band_number}" = %s
        WHERE tile_id = %s;
    """
    params = (status, tile_number)
    execute_query(query, params)

def update_3d_pipeline(tile_number, band_number, status):
    """
    Update the 'tile_3d_pipeline' table, setting either '3d_pipeline_band1' or '3d_pipeline_band2'
    with given status. Possible values:
        - Running (Job has been submitted and is currently running)
        - WaitingForValidation (Job has finished running successfully, waiting for human validation)        
        - Failed (Job failed to run)

    Args:
    tile_number (str): The tile number to update.
    band_number (str): 1 or 2
    status (str): The status to set in the respective column.
    """
    validate_band_number(band_number)
    print(f"Updating POSSUM tile database table with 3d_pipeline_band{band_number} to {status}")
    query = f"""
        UPDATE possum.tile_3d_pipeline
        SET "3d_pipeline_band{band_number}" = %s
        WHERE tile_id = %s;
    """
    params = (status, tile_number)
    execute_query(query, params)

def update_1d_pipeline_validation_status(field_name, sbid, band_number, status):
    """
    Update the 1d_pipeline_validation status column in the observation_1d_pipeline_band{band_number} table.
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
    query = f"""
        UPDATE possum.observation_1d_pipeline_band{band_number}
        SET 1d_pipeline_validation = %s
        WHERE name = %s AND sbid = %s;
    """
    params = (status, field_name, sbid)
    results = execute_query(query, params)
    rows_num = len(results)
    if rows_num > 0:
        print(f"""Updated all {rows_num} rows for field {field_name} and SBID {sbid}
              to status '{status}' in
              observation_1d_pipeline_band{band_number}.1d_pipeline_validation' column.""")
    else:
        print(f"No rows found for field {field_name} and SBID {sbid}.")

def update_single_sb_1d_pipeline_status(field_name, sbid, band_number, status):
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
    print(f"Updating POSSUM observation_1d_pipeline_band{band_number} table with 'single_SB_1D_pipeline' status")
    query = f"""
        UPDATE possum.observation_1d_pipeline_band{band_number}
        SET single_SB_1D_pipeline = %s
        WHERE name = %s AND sbid = %s;
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
            WHERE sbid = %s AND observation = %s AND LOWER(type) like '%%crosses projection boundary%%'
        ) AS match_found;
    """
    params = (sbid, observation)
    results = execute_query(query, params)
    issues_found = results[0][0]
    if issues_found is True:
        print("Boundary issues found.")
    else:
        print("No boundary issues found.")
    return issues_found

def validate_band_number(band_number):
    """
    Making sure band number is valid
    """
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
    print(f"Fetching tiles ready for 3D pipeline run for band {band_number} from the database.")
    query = f"""
        SELECT DISTINCT tile_id
        FROM possum.tile_3d_pipeline tile_3d
        INNER JOIN possum.associated_tile ON associated_tile.tile = tile_3d.tile_id
        INNER JOIN possum.observation ON observation.name = associated_tile.name 
        WHERE observation.cube_state = 'COMPLETED' 
        AND (tile_3d."3d_pipeline_band{band_number}" IS NULL OR tile_3d."3d_pipeline_band{band_number}" = '')
        ORDER BY tile_id
    """
    return execute_query(query)

def get_tiles_for_ingest(band_number):
    """
    Get a list of 3D pipeline tile numbers that should be ready to be ingested.
    i.e. tile_3d_pipeline.'3d_pipeline_val_band{band_number}' = 'Good' and 
    tile_3d_pipeline.'3d_pipeline_ingest_band{band_number}' is NULL
    
    Args:
    band_number (int): The band number (1 or 2) to check.
    
    Returns:
    list: A list of tile numbers that satisfy the conditions.
    """
    validate_band_number(band_number)
    print(f"Fetching tiles ready for 3D pipeline run for band {band_number} from the database.")
    query = f"""
        SELECT DISTINCT tile_id
        FROM possum.tile_3d_pipeline tile_3d
        WHERE LOWER(tile_3d."3d_pipeline_val_band{band_number}") = 'good' AND
        (tile_3d."3d_pipeline_ingest_band{band_number} IS NULL OR 
        tile_3d."3d_pipeline_ingest band{band_number}" = '')
        ORDER BY tile_id
    """
    return execute_query(query)

def update_partial_tile_1d_pipeline_status(field_id, tile_numbers, band_number, status):
    """
    Update 1d_pipeline in partial_tile_1d_pipeline_band{band_number} table for a set of tiles.
    This replaces the 1d_pipeline column in the POSSUM pipeline validation Google sheet:
    Partial Tile Pipeline - regions - Band {band_number}

    Args:
    field_ID (str): The field ID.
    tile_numbers (tuple): The tile numbers to update.
    status (str): The new validation status to set.
    """
    print(f"Updating POSSUM partial_tile_1d_pipeline_band{band_number}.1d_pipeline in the database")
    t1, t2, t3, t4 = tile_numbers
    query = f"""
        UPDATE possum.partial_tile_1d_pipeline_band{band_number}
        SET 1d_pipeline = %s
        WHERE field_ID = %s AND tile1 = %s AND tile2 = %s AND tile3 = %s AND tile4 = %s;
    """
    params = (status, field_id, t1, t2, t3, t4)
    results = execute_query(query, params)
    if len(results) > 0:
        print(f"Updated row with tiles {tile_numbers} status to {status} in '1d_pipeline' column.")
    else:
        print(f"Field {field_id} with tiles {tile_numbers} not found in the sheet.")

def get_partial_tiles_for_1d_pipeline_run(band_number):
    """
    Get a list of partial tiles that should be ready to be processed by the 1D pipeline
    In the database, this is when:
    partial_tile_1d_pipeline_band{band_number}.sbid is not NULL, number_sources is not NULL,
    and 1d_pipeline is NULL

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
        FROM possum.partial_tile_1d_pipeline_band{band_number} pt
        WHERE pt.sbid IS NOT NULL
          AND pt.number_sources IS NOT NULL
          AND pt."1d_pipeline" IS NULL;
    """
    return execute_query(query)

def get_observations_with_complete_partial_tiles(band_number):
    """
    For each observation, check if all '1d_pipeline' is "Completed" and 1d_pipeline_validation' is empty
    Return rows of: (observation, sbid, all_complete)
    for which all_complete is True if all partial tiles for that observation and sbid
    have '1d_pipeline' marked as "Completed" and '1d_pipeline_validation' for the observation is NULL
    """
    sql = f"""
        SELECT pt.observation, pt.sbid,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM possum.partial_tile_1d_pipeline_band{band_number} pt2
                WHERE pt2.observation = pt.observation
                AND LOWER(pt2."1d_pipeline") != 'completed'
                ) THEN false
            WHEN ob."1d_pipeline_validation" IS NULL AND LOWER(pt."1d_pipeline") = 'completed'
                THEN true
            ELSE
                false
        END AS all_complete
        FROM possum.partial_tile_1d_pipeline_band{band_number} pt, possum.observation_1d_pipeline_band{band_number} ob
        WHERE ob.name = pt.observation
        GROUP BY pt.observation, pt.sbid, ob."1d_pipeline_validation", pt."1d_pipeline";
    """
    return execute_query(sql)

def get_observations_non_edge_rows(band_number):
    """
    For each observation, check if all '1d_pipeline' is "Completed" and 1d_pipeline_validation' is empty,
    and if any partial tile type is an edge case (includes "crosses projection boundary").
    Return rows of: (observation, sbid, non_edge_complete)
    for which non_edge_complete is True if all partial tiles for that observation and sbid
    have '1d_pipeline' marked as "Completed" and '1d_pipeline_validation' for the observation is NULL
    and it does not contain a partial tile that has "crosses projection boundary" in its type.
    """
    sql = f"""
        SELECT pt.observation, pt.sbid,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM possum.partial_tile_1d_pipeline_band{band_number} pt2
                WHERE pt2.observation = pt.observation
                AND (LOWER(pt2."1d_pipeline_band") != 'completed' OR LOWER(pt2.type) like '%crosses projection boundary%')
                ) THEN false
            WHEN ob."1d_pipeline_validation" IS NULL AND LOWER(pt."1d_pipeline_band") = 'completed'
                THEN true
            ELSE
                false
        END AS all_complete
        FROM possum.partial_tile_1d_pipeline_band{band_number} pt, possum.observation_1d_pipeline_band{band_number} ob
        WHERE ob.name = pt.observation
        GROUP BY pt.observation, pt.sbid, ob."1d_pipeline_validation", pt."1d_pipeline_band";
    """
    return execute_query(sql)
