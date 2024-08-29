from vos import Client
import gspread
import astropy.table as at
import numpy as np
from time import sleep
import pandas as pd

from skaha.session import Session
session = Session()


"""
Checks POSSUM validation status ("POSSUM Pipeline validation" google sheet) if 3D pipeline outputs can
be ingested.

Should be executed on p1


If the "3d_pipeline_val" column is marked as "Good", we can launch an ingest job. 
    In that case, the status of 3d_pipeline_ingest will be changed to IngestRunning


@author: Erik Osinga
"""

def get_open_sessions():
    """Return a table with information about currently open sessions"""
    # Fetch open sessions
    open_sessions = session.fetch()

    # Convert the list of dictionaries to a pandas DataFrame
    df_sessions = pd.DataFrame([{
        'type': s['type'],
        'status': s['status'],
        'startTime': s['startTime']
    } for s in open_sessions])

    return df_sessions

def get_tiles_for_ingest(band_number, Google_API_token):
    """
    Get a list of 3D pipeline tile numbers that should be ready to be ingested.
    
    i.e.  '3d_pipeline_val' column is equal to "Good", meaning that it has been validated by a human.
    
    Args:
    band_number (int): The band number (1 or 2) to check.
    Google_API_token (str): The path to the Google API token JSON file.
    
    Returns:
    list: A list of tile numbers that satisfy the conditions.
    """
    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    # URL for POSSUM Pipeline Validation sheet
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k')

    # Select the worksheet for the given band number
    tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    # Find the tiles that satisfy the conditions
    tiles_to_run = [row['tile_id'] for row in tile_table if row['3d_pipeline_val'] == 'Good']

    return tiles_to_run

def get_canfar_tiles(band_number):
    client = Client()
    # force=True to not use cache
    # assumes directory structure doesnt change and symlinks are created
    if band_number == 1:
        canfar_tilenumbers = client.listdir("vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/Tiles/943MHz/",force=True)
    elif band_number == 2:
        canfar_tilenumbers = client.listdir("vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/Tiles/1367MHz/",force=True)
    else:
        raise ValueError(f"Band number {band_number} not defined")
    return canfar_tilenumbers

def launch_ingest(tilenumber, band):
    """Launch 3D pipeline ingest script"""

    band_number = '1' if band == '943MHz' else '2'

    run_name = f"ingest{tilenumber}"
    # optionally :latest for always the latest version
    image = "images.canfar.net/cirada/possumpipelineprefect-3.12:latest"
    # good default values for ingest script
    cores = 2
    ram = 32  # Check allowed values at canfar.net/science-portal

    # Template bash script to run
    args = f"/arc/projects/CIRADA/polarimetry/software/ingest_3Dpipeline_band{band_number}_prefect.sh {tilenumber} {band}"

    print("Launching session")
    print(f"Command: bash {args}")

    session_id = session.create(
        name=run_name.replace('_', '-'),  # Prevent Error 400: name can only contain alpha-numeric chars and '-'
        image=image,
        cores=cores,
        ram=ram,
        kind="headless",
        cmd="bash",
        args=args,
        replicas=1,
    )

    print("Check sessions at https://ws-uv.canfar.net/skaha/v0/session")
    print(f"Check logs at https://ws-uv.canfar.net/skaha/v0/session/{session_id[0]}?view=logs")    

    return
    
def update_status(tile_number, band, Google_API_token, status):
    """
    Update the status of the specified tile in the Google Sheet.
    
    Args:
    tile_number (str): The tile number to update.
    band (str): The band of the tile.
    Google_API_token (str): The path to the Google API token JSON file.
    status (str): The status to set in the '3d_pipeline' column.
    """
    # Make sure its not int
    tile_number = str(tile_number)

    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    # URL for POSSUM Pipeline Validation sheet
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k')

    # Select the worksheet for the given band number
    band_number = '1' if band == '943MHz' else '2'
    tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    # Find the row index for the specified tile number
    tile_index = None
    for idx, row in enumerate(tile_table):
        if row['tile_id'] == tile_number:
            tile_index = idx + 2  # +2 because gspread index is 1-based and we skip the header row
            break
    
    if tile_index is not None:
        # Update the status in the '3d_pipeline' column
        col_letter = gspread.utils.rowcol_to_a1(1, column_names.index('3d_pipeline_ingest') + 1)[0]
        # as of >v6.0.0 .update requires a list of lists
        tile_sheet.update(range_name=f'{col_letter}{tile_index}', values=[[status]])
        print(f"Updated tile {tile_number} status to {status} in '3d_pipeline_ingest' column.")
    else:
        print(f"Tile {tile_number} not found in the sheet.")

def ingest_3Dpipeline(band_number=1):
    if band_number == 1:
        band = "943MHz"
    elif band_number == 2:
        band = "1367MHz"

    # on p1, API token for POSSUM Pipeline Validation sheet
    Google_API_token = "/home/erik/.ssh/neural-networks--1524580309831-c5c723e2468e.json"

    # Check google Validation sheet for band 1 tiles that have been processed AND validated
    tile_numbers = get_tiles_for_ingest(band_number=band_number, Google_API_token=Google_API_token)

    # Check whether tile indeed available on CANFAR (should be)
    canfar_tilenumbers = get_canfar_tiles(band_number=band_number)
    sleep(1)

    if len(tile_numbers) > 0:
        print(f"Found {len(tile_numbers)} tiles in Band {band_number} ready to be ingested")
        print(f"On CANFAR, found {len(canfar_tilenumbers)} tiles in Band {band_number}")

        if len(tile_numbers) > len(canfar_tilenumbers):
            tiles_in_cadc_not_canfar = set(tile_numbers) - set(canfar_tilenumbers)
            print(f"Tiles in CADC but not on CANFAR: {tiles_in_cadc_not_canfar}")
        else:
            tiles_on_canfar_not_cadc = set(canfar_tilenumbers) - set(tile_numbers)
            print(f"Tiles on CANFAR but not in CADC: {tiles_on_canfar_not_cadc}")

        tiles_on_both = set(tile_numbers) & set(canfar_tilenumbers)
        # print(f"Tiles on both CADC and CANFAR: {tiles_on_both}")

        if tiles_on_both:
            # Launch the first tile number (assumes this function will be called many times)
            tilenumber = list(tiles_on_both)[0]
            print(f"\nLaunching headless job for 3D pipeline with tile {tilenumber}")

            # Launch the pipeline
            launch_ingest(tilenumber, band)
            
            # Update the status of 3d_pipeline_ingest to "IngestRunning"
            update_status(tilenumber, band, Google_API_token, "IngestRunning")
            
        else:
            print("No tiles are available on both CADC and CANFAR.")
    else:
        print("Found no tiles ready to be processed.")

if __name__ == "__main__":
    # Band number 1 (943MHz) or 2 ("1367MHz")
    band_number = 1


    ## Assumes this script is called by run_3D_pipeline_intermittently.py
    ingest_3Dpipeline(band_number=band_number)
