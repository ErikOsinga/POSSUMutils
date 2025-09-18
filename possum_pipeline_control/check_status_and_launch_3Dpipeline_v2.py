from vos import Client
import subprocess
import gspread
import astropy.table as at
import numpy as np
from time import sleep
from automation import database_queries as db
import util

"""
Checks POSSUM tile status (Cameron's survey overview google sheet) if 3D pipeline can be started.
Updates the POSSUM tile status (Cameron's survey overview google sheet) to "running" if 3D pipeline is submitted.

Should be executed on p1


A 3D pipeline run can be started if on the Survey Tiles - Band <number> sheet:

the aus_src column is not empty, and the 3d_pipeline column is empty

meaning that IQU cubes + MFS have been ingested into the CADC but 3D pipeline data not yet


Also checks CANFAR directory
/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/
for the existence of the input IQU cubes + MFS image. For this, the script
create_symlinks.py should be run after possum_run_remote is executed that downloads the data
into time-blocked directories.



@author: Erik Osinga
"""

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

def launch_pipeline(tilenumber, band):
    # Launch the appropriate 3D pipeline script based on the band
    if band == "943MHz":
        command = ["python", "launch_3Dpipeline_band1.py", str(tilenumber)]
    elif band == "1367MHz":
        command = ["python", "launch_3Dpipeline_band2.py", str(tilenumber)]
        command = ""
        print("Temporarily disabled launching band 2 because need to write that run script")
    else:
        raise ValueError(f"Unknown band: {band}")

    print(f"Running command: {' '.join(command)}")
    subprocess.run(command, check=True)

def update_status(tile_number, band, Google_API_token, status):
    """
    Update the status of the specified tile in the Google Sheet.
    
    Args:
    tile_number (str): The tile number to update.
    band (str): The band of the tile. ('943MHz' or '1367MHz')
    Google_API_token (str): The path to the Google API token JSON file.
    status (str): The status to set in the '3d_pipeline' column.
    """
    # Make sure its not int
    tile_number = str(tile_number)

    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0')

    # Select the worksheet for the given band number
    band_number = util.get_band_number(band)
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
        col_letter = gspread.utils.rowcol_to_a1(1, column_names.index('3d_pipeline') + 1)[0]
        # as of >v6.0.0 .update requires a list of lists
        tile_sheet.update(range_name=f'{col_letter}{tile_index}', values=[[status]])
        print(f"Updated tile {tile_number} status to {status} in '3d_pipeline' column.")
        # Also update the DB
        db.update_3d_pipeline_status(tile_number, band_number, status)
    else:
        print(f"Tile {tile_number} not found in the sheet.")

# Update the launch_band1_3Dpipeline function to pass the correct arguments
def launch_band1_3Dpipeline():
    band = "943MHz"
    # on p1
    Google_API_token = "/home/erik/.ssh/psm_gspread_token.json"
    
    # Check google sheet for band 1 tiles that have been ingested into CADC 
    # (and thus available on CANFAR) but not yet processed with 3D pipeline
    tile_numbers = db.get_tiles_for_pipeline_run(band_number=1)
    canfar_tilenumbers = get_canfar_tiles(band_number=1)
    sleep(1)

    if len(tile_numbers) > 0:
        print(f"Found {len(tile_numbers)} tiles in Band 1 ready to be processed with 3D pipeline")
        print(f"On CANFAR, found {len(canfar_tilenumbers)} tiles in Band 1")

        if len(tile_numbers) > len(canfar_tilenumbers):
            tiles_in_cadc_not_canfar = set(tile_numbers) - set(canfar_tilenumbers)
            print(f"Tiles in CADC but not on CANFAR: {tiles_in_cadc_not_canfar}")
        else:
            tiles_on_canfar_not_cadc = set(canfar_tilenumbers) - set(tile_numbers)
            print(f"Tiles on CANFAR but not in CADC: {tiles_on_canfar_not_cadc}")

        tiles_on_both = set(tile_numbers) & set(canfar_tilenumbers)
        # print(f"Tiles on both CADC and CANFAR: {tiles_on_both}")

        if tiles_on_both:
            # Launch the first tile number (assumes this script will be called many times)
            tilenumber = list(tiles_on_both)[0]
            print(f"\nLaunching headless job for 3D pipeline with tile {tilenumber}")

            # Launch the pipeline
            launch_pipeline(tilenumber, band)
            
            # Update the status to "Running"
            update_status(tilenumber, band, Google_API_token, "Running")
            
        else:
            print("No tiles are available on both CADC and CANFAR.")
    else:
        print("Found no tiles ready to be processed.")

if __name__ == "__main__":
    launch_band1_3Dpipeline()
