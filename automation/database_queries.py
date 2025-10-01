"""
Database query functions for interacting with the ausSRC database.
"""
import os
import psycopg2
from dotenv import load_dotenv


def get_database_connection(test) :
    """
    Initiate a database connection.
    Args:
    - test: True if this should connect to the test database set up in test.env
    """
    conn_params = get_database_parameters(test)
    return psycopg2.connect(**conn_params)

def get_database_parameters(test=False):
    """
    Get database parameters from env file (test.env for test, config.env otherwise)
    """
    if test:
        load_dotenv(dotenv_path='automation/unit_tests/test.env')
    else:
        # Get database connection details from config.env file
        load_dotenv(dotenv_path='automation/config.env')

    return {
        'dbname': os.getenv('DATABASE_NAME'),
        'user': os.getenv('DATABASE_USER'),
        'password': os.getenv('DATABASE_PASSWORD'),
        'host': os.getenv('DATABASE_HOST'),
        'port': os.getenv('DATABASE_PORT')
    }

def execute_update_query(query, conn, params=None, verbose=True):
    """
    Execute an update SQL query and return the number of rows affected.

    Args:
    query (str): The SQL query to execute.
    params (tuple): Optional parameters for the SQL query.

    Returns:
    list: The number of rows affected.
    """
    cursor = conn.cursor()
    rows_affected = 0
    try:
        # Execute the query
        if verbose:
            print(f"Executing database query: {query}")
            if params:
                print(f"With parameters: {params}")
        cursor.execute(query, params)
        conn.commit()
        rows_affected = cursor.rowcount
        if verbose:
            print(f"{rows_affected} rows affected.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor
        if cursor:
            cursor.close()
    return rows_affected

def execute_query(query, database_connection, params=None, verbose=True):
    """
    Execute a SQL query and return the results.

    Args:
    query (str): The SQL query to execute.
    params (tuple): Optional parameters for the SQL query.

    Returns:
    list: The results of the query.
    """
    cursor = database_connection.cursor()
    results = []

    try:
        # Execute the query
        if verbose:
            if params:
                print(f"Executing database query: {query} with {params}")
            else:
                print(f"Executing database query: {query}")
        cursor.execute(query, params)

        # Fetch all results
        if cursor.description is not None:
            results = cursor.fetchall()
        else:
            database_connection.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor
        if cursor:
            cursor.close()
    return results

def update_3d_pipeline_ingest(tile_number, band_number, status, conn):
    """
    Update the 'tile_3d_pipeline_band{band_number}' table, setting '3d_pipeline_ingest'
    with given status. Possible values:
        - Ingested
        - IngestFailed
        _ IngestRunning

    Args:
    tile_number (str): The tile number to update.
    band_number (str): 1 or 2
    status (str): The status to set in the respective column.

    Return: 0 if tile was not found, 1 if executed
    """
    validate_band_number(band_number)
    print(f"Updating POSSUM tile database table for band{band_number} with 3d_pipeline_ingest to {status}")
    query = f"""
        UPDATE possum.tile_3d_pipeline_band{band_number}
        SET "3d_pipeline_ingest" = %s
        WHERE tile_id = %s;
    """
    params = (status, tile_number)
    return execute_update_query(query, conn, params)

def update_3d_pipeline_val(tile_number, band_number, status, val_link, conn):
    """
    Update the 'tile_3d_pipeline_band{band_number}' table, setting '3d_pipeline_val'
    with given status, and '3d_val_link' with validation link. Possible status values:
        - WaitingForValidation (Job has finished running successfully, waiting for human validation)
        - Good/Bad (Currently has to be manually set after human validation)

    Args:
    tile_number (str): The tile number to update.
    band_number (str): 1 or 2
    status (str): The status to set in the respective column.
    val_link (str): Validation link
    """
    validate_band_number(band_number)
    print(f"Updating POSSUM tile database table for band{band_number} with 3d_pipeline_val to {status}")
    print(f"Updating POSSUM tile database table for band{band_number} with 3d_val_link to {val_link}")
    query = f"""
        UPDATE possum.tile_3d_pipeline_band{band_number}
        SET "3d_pipeline_val" = %s, "3d_val_link" = %s
        WHERE tile_id = %s;
    """
    params = (status, tile_number, val_link)
    return execute_update_query(query, conn, params)

def update_3d_pipeline(tile_number, band_number, status, conn):
    """
    Update the 'tile_3d_pipeline_band{band_number}' table, setting '3d_pipeline'
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
    print(f"Updating POSSUM tile database table for band {band_number} with 3d_pipeline to {status}")
    query = f"""
        UPDATE possum.tile_3d_pipeline_band{band_number}
        SET "3d_pipeline" = %s
        WHERE tile_id = %s;
    """
    params = (status, tile_number)
    return execute_update_query(query, conn, params)

def update_1d_pipeline_validation_status(field_name, sbid, band_number, status, conn):
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
        INSERT INTO possum.observation_1d_pipeline_band{band_number}
        (name, sbid, "1d_pipeline_validation") -- IN CASE the entry doesn't exist yet
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE
        SET "1d_pipeline_validation" = %s;
    """
    params = (field_name, sbid, status, status)
    return execute_update_query(query, conn, params)

def update_single_sb_1d_pipeline_status(field_name, sbid, band_number, status, conn):
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
        INSERT INTO possum.observation_1d_pipeline_band{band_number}
        (name, sbid, single_sb_1d_pipeline)
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE
        SET single_sb_1d_pipeline = %s;
    """
    params = (field_name, sbid, status, status)
    return execute_update_query(query, conn, params)

def find_boundary_issues(sbid, observation, band_number, conn):
    """
    Check if there are any entries in partial_tile_1d_pipeline for the given sbid and observation
    where type indicates it crosses a projection boundary.
    This is to identify potential issues with tiles that cross projection boundaries.
    """
    print(f"Checking for projection boundary issues for SBID: {sbid}, Observation: {observation}")
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM possum.partial_tile_1d_pipeline_band{band_number}
            WHERE sbid = %s AND observation = %s AND LOWER(type) like '%%crosses projection boundary%%'
        ) AS match_found;
    """
    params = (sbid, observation)
    results = execute_query(query, conn, params)
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
    if str(band_number) not in ["1", "2"]:
        raise ValueError("band_number must be either 1 or 2")

def get_tiles_for_pipeline_run(conn, band_number):
    """
    Get a list of tile numbers that should be ready to be processed by the 3D pipeline

    In the database, this is when:
    observation.cube_state = 'COMPLETED' and tile.3d_pipeline = [null]
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
        FROM possum.tile_3d_pipeline_band{band_number} tile_3d
        INNER JOIN possum.associated_tile ON associated_tile.tile = tile_3d.tile_id
        INNER JOIN possum.observation ON observation.name = associated_tile.name
        WHERE observation.cube_state = 'COMPLETED'
        AND (tile_3d."3d_pipeline" IS NULL OR tile_3d."3d_pipeline" = '')
        ORDER BY tile_id
    """
    return execute_query(query, conn)

def get_tiles_for_ingest(band_number, conn):
    """
    Get a list of 3D pipeline tile numbers that should be ready to be ingested.
    i.e. tile_3d_pipeline_band1.'3d_pipeline_val' = 'Good' and
    tile_3d_pipeline_band1.'3d_pipeline_ingest' is NULL

    Args:
    band_number (int): The band number (1 or 2) to check.

    Returns:
    list: A list of tile numbers that satisfy the conditions.
    """
    validate_band_number(band_number)
    print(f"Fetching tiles ready for 3D pipeline run for band {band_number} from the database.")
    query = f"""
        SELECT DISTINCT tile_id
        FROM possum.tile_3d_pipeline_band{band_number} tile_3d
        WHERE LOWER(tile_3d."3d_pipeline_val") = 'good' AND
        (tile_3d."3d_pipeline_ingest" IS NULL OR
        tile_3d."3d_pipeline_ingest" = '')
        ORDER BY tile_id
    """
    results = execute_query(query, conn)
    # flatten tile ids into an array
    return [row[0] for row in results]

def update_partial_tile_1d_pipeline_status(field_name, tile_numbers, band_number, status, conn):
    """
    Update 1d_pipeline in partial_tile_1d_pipeline_band{band_number} table for a set of tiles.
    This replaces the 1d_pipeline column in the POSSUM pipeline validation Google sheet:
    Partial Tile Pipeline - regions - Band {band_number}

    Args:
    field_name (str): The field ID with 'EMU_' or 'WALLABY_' prefix.
    tile_numbers (tuple): The tile numbers to update.
    band_number: '1' or '2'
    status (str): The new validation status to set.
    """
    print(f"Updating POSSUM partial_tile_1d_pipeline_band{band_number}.1d_pipeline in the database")
    t1, t2, t3, t4 = tile_numbers
    query = f"""
        UPDATE possum.partial_tile_1d_pipeline_band{band_number}
        SET "1d_pipeline" = %s
        WHERE observation = %s"""

    params = [status, field_name]  # Initial params
    # Check for NULLS in tile numbers and make sure the query says IS NULL and not = NULL so it works
    for i, tile in enumerate([t1, t2, t3, t4], start=1):
        if tile is None or tile == '':  # If tile is None, use IS NULL
            query += f" AND tile{i} IS NULL"
        else:  # Otherwise, use equality
            query += f" AND tile{i} = %s"
            params.append(tile)

    row_num = execute_update_query(query, conn, params)
    if row_num > 0:
        print(f"Updated row with tiles {tile_numbers} status to {status} in '1d_pipeline' column.")
    else:
        print(f"Field {field_name} with tiles {tile_numbers} not found in the database d.")

def get_partial_tiles_for_1d_pipeline_run(band_number, conn):
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
        WHERE pt.sbid IS NOT NULL AND pt.sbid != ''
          AND pt.number_sources IS NOT NULL
          AND (pt."1d_pipeline" IS NULL or pt."1d_pipeline" = '');
    """
    return execute_query(query, conn)

def get_observations_with_complete_partial_tiles(band_number, conn):
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
            WHEN (ob."1d_pipeline_validation" IS NULL OR ob."1d_pipeline_validation" = '') AND LOWER(pt."1d_pipeline") = 'completed'
                THEN true
            ELSE
                false
        END AS all_complete
        FROM possum.partial_tile_1d_pipeline_band{band_number} pt, possum.observation_1d_pipeline_band{band_number} ob
        WHERE ob.name = pt.observation
        GROUP BY pt.observation, pt.sbid, ob."1d_pipeline_validation", pt."1d_pipeline";
    """
    return execute_query(sql, conn)

def get_observations_non_edge_rows(band_number, conn):
    """
    For each observation, check if all '1d_pipeline' is "Completed" and 1d_pipeline_validation' is empty,
    except if any partial tile type is an edge case (includes "crosses projection boundary").
    Return rows of: (observation, sbid, non_edge_complete)
    for which non_edge_complete is True if all partial tiles for that observation and sbid
    have '1d_pipeline' marked as "Completed" and '1d_pipeline_validation' for the observation is NULL
    disregarding those that have "crosses projection boundary" in its type.
    """
    sql = f"""
        SELECT pt.observation, pt.sbid,
            CASE
               WHEN EXISTS (
                   SELECT 1
                   FROM possum.partial_tile_1d_pipeline_band{band_number} pt_inner
                   JOIN possum.observation_1d_pipeline_band{band_number} ob_inner
                       ON ob_inner.name = pt_inner.observation
                   WHERE LOWER(pt_inner.type) NOT LIKE '%crosses projection boundary%' 
                   AND LOWER(pt_inner."1d_pipeline") = 'completed'
                   AND (ob_inner."1d_pipeline_validation" IS NULL OR ob_inner."1d_pipeline_validation" = '')
                   AND pt_inner.observation = pt.observation  -- Ensure we match the outer observation
               )
               THEN true
               ELSE false
           END AS all_complete
        FROM possum.partial_tile_1d_pipeline_band{band_number} pt;
    """
    return execute_query(sql, conn)

#### TEST METHODS ###

def get_3d_tile_data(tile_id, band_number, conn):
    """
    Utility method to get 3d tile information (useful for tests)
    Args:
    - tile_id: tile number
    - band_number: 1 or 2
    """
    sql = f"""SELECT tile_id, "3d_pipeline_val", "3d_val_link", "3d_pipeline_ingest", "3d_pipeline"
              from possum.tile_3d_pipeline_band{band_number} WHERE tile_id = {tile_id}"""
    return execute_query(sql, conn)

def get_1d_pipeline_validation_status(field_name, band_number, conn):
    """
    Handy method to get 1d_pipeline_validation status (useful for tests):
    """
    sql = f"""
        SELECT "1d_pipeline_validation" FROM possum.observation_1d_pipeline_band{band_number}
        WHERE name = '{field_name}';
        """
    return execute_query(sql, conn)

def get_single_sb_1d_pipeline_status(field_name, band_number, conn):
    """
    Handy method to get single_sb_1d_pipeline status (useful for tests):
    """
    sql = f"""
        SELECT single_sb_1d_pipeline FROM possum.observation_1d_pipeline_band{band_number}
        WHERE name = '{field_name}';
        """
    return execute_query(sql, conn)

def get_1d_pipeline_status(field_name, tilenumbers, band_number, conn):
    """
    Handy method to get 1d_pipeline status (useful for tests):
    """
    sql = f"""
        SELECT "1d_pipeline" FROM possum.partial_tile_1d_pipeline_band{band_number}
        WHERE observation = '{field_name}'
        """
    params = []  # Initial params
    # Check for NULLS in tile numbers and make sure the query says IS NULL and not = NULL so it works
    for i, tile in enumerate(tilenumbers, start=1):
        if tile is None or tile == '':  # If tile is None, use IS NULL
            sql += f" AND tile{i} IS NULL"
        else:  # Otherwise, use equality
            sql += f" AND tile{i} = %s"
            params.append(tile)

    return execute_query(sql, conn, params)
