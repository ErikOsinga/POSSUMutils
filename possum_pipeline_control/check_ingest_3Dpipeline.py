import os
from dotenv import load_dotenv
from vos import Client
import argparse
import gspread
import astropy.table as at
import numpy as np
from time import sleep
import pandas as pd
from possum_pipeline_control import util
from skaha.session import Session
from automation import database_queries as db

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

def get_tiles_for_ingest(band_number, conn):
    """
    Get a list of 3D pipeline tile numbers that should be ready to be ingested.

    i.e.  '3d_pipeline_val' column is equal to "Good", meaning that it has been validated by a human.
    and   '3d_pipeline_ingest' column is empty, meaning that it has not yet been tried to ingest.

    Args:
    band_number (int): The band number (1 or 2) to check.

    Returns:
    list: A list of tile numbers that satisfy the conditions.
    """
    # Find the tiles that satisfy the conditions
    return db.get_tiles_for_ingest(band_number, conn)

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

    band_number = util.get_band_number(band)

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

def update_status(tile_number, band, status, conn):
    """
    Update the status of the specified tile in the database.

    Args:
    tile_number (str): The tile number to update.
    band (str): The band of the tile.
    status (str): The status to set in the '3d_pipeline_ingest' column.
    """
    band_no = util.get_band_number(band)
    return db.update_3d_pipeline_table(tile_number, band_no, status, '3d_pipeline_ingest', conn)

def ingest_3Dpipeline(band_number=1):
    if band_number == 1:
        band = "943MHz"
    elif band_number == 2:
        band = "1367MHz"

    # Check database for band 1 tiles that have been processed AND validated
    conn = db.get_database_connection(test=False)
    tile_numbers = get_tiles_for_ingest(band_number, conn)    
    conn.close()

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
            conn = db.get_database_connection(test=False)
            row_count = update_status(tilenumber, band, "IngestRunning", conn)
            conn.close()
            if row_count == 0:
                print(f"Tile {tilenumber} not found in the sheet.")

        else:
            print("No tiles are available on both CADC and CANFAR.")
    else:
        print("Found no tiles ready to be processed.")

if __name__ == "__main__":
    # on p1, API token for POSSUM Pipeline Validation sheet
    # DEPRECATED
    Google_API_token = "/home/erik/.ssh/neural-networks--1524580309831-c5c723e2468e.json"

    parser = argparse.ArgumentParser(description="Checks POSSUM validation status ('POSSUM Pipeline validation' google sheet) if 3D pipeline outputs can be ingested.")
    parser.add_argument("--psm_val_api_token", type=str, default=Google_API_token, help="Path to POSSUM validation sheet Google API token JSON file")
    args = parser.parse_args()

    # Band number 1 (943MHz) or 2 ("1367MHz")
    band_number = 1

    # load env for google spreadsheet constants
    load_dotenv(dotenv_path='../automation/config.env')

    ## Assumes this script is called by run_3D_pipeline_intermittently.py
    ingest_3Dpipeline(band_number=band_number)
