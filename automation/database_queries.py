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
        if query.strip().lower().startswith(('insert', 'update', 'create')):
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
    validate_band_number(band_number)
    column_name = "1d_pipeline_validation_band2" if band_number == '2' else "1d_pipeline_validation_band1"
    query = f"""
        UPDATE possum.observation
        SET {column_name} = %s
        WHERE field_name = %s AND sbid = %s;
    """
    params = (status, field_name, sbid)
    execute_query(query, params)

def find_boundary_issues():
    query = f"""
        SELECT *
    """
    band_number = '1' if band == '943MHz' else '2'
    tile_sheet = ps.worksheet(f'Partial Tile Pipeline - regions - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    # Build field name prefix
    fieldname = "EMU_" if band == '943MHz' else 'WALLABY_'  # TODO: verify WALLABY_ fieldname
    full_field_name = f"{fieldname}{field_ID}"

    # Update all rows belonging to field and sbid
    rows_to_update = [
        idx + 2  # +2 because gspread index is 1-based and skips the header row
        for idx, row in enumerate(tile_table)
        if row['field_name'] == full_field_name and row['sbid'] == str(SBid)
    ]

    if not rows_to_update:
        print(f"No rows found for field {full_field_name} and SBID {SBid}")
        return False
    
    # Determine the column letter of the status column.
    # col_letter = gspread.utils.rowcol_to_a1(1, column_names.index(status_column) + 1)[0]
    
    # Determine the index (1-based) for the column to update
    col_index = column_names.index(status_column) + 1
    
    # Prepare a list of cell updates.
    cells = [Cell(r, col_index, status) for r in rows_to_update]

    # Check if any row has "crosses projection boundary" in the type column
    boundary_issue = False
    for row_index in rows_to_update:
        # Check for the projection boundary issue
        if "crosses projection boundary" in tile_table['type'][row_index - 2].lower():
            boundary_issue = True    
    
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
    
def update_validation_in_db(field_ID, tile_numbers, band, status):
    """
    Update the validation status of a tile in the database.
    We don't need to do this for 3D because it's for manual validation.
    
    Args:
    field_ID (str): The field ID.
    tile_numbers (tuple): The tile numbers to update.
    band (str): The band of the tile.
    status (str): The new validation status to set.
    """
    t1, t2, t3, t4 = tile_numbers
    query = """
        UPDATE validation_tiles
        SET validation_status = %s
        WHERE field_ID = %s AND tile1 = %s AND tile2 = %s AND tile3 = %s AND tile4 = %s AND band = %s;
    """
    params = (status, field_ID, t1, t2, t3, t4, band)
    execute_query(query, params)	
    
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