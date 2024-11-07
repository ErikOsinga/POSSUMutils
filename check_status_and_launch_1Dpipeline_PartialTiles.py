from vos import Client
import subprocess
import gspread
import astropy.table as at
import numpy as np
from time import sleep

"""
Should be executed on p1

Checks POSSUM Partial Tile status (google sheet)if 1D pipeline can be started.

https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k/edit?usp=sharing

Updates the POSSUM tile status (google sheet) to "running" if 1D pipeline is submitted.


A 1D pipeline run can be started if on the sheet called "Partial Tile Pipeline - [centers/edges] - Band [number]":

1. the "SBID" column is not empty (indicating it has been observed), and
2. the "number_sources" column is not empty (indicating the sourcelist has been created)
3. the "1d_pipeline" column is empty (indicating it hasn't been run yet)


Also checks CANFAR directory
/arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/

for the existence of the input sourcelists.

Cameron is in charge of generating the source lists per partial tile and separately for the centers/edges.

@author: Erik Osinga
"""

def get_tiles_for_pipeline_run(band_number, Google_API_token, whichpart='centers'):
    """
    Get a list of tile numbers that should be ready to be processed by the 1D pipeline 
    
    i.e.  'SBID' column is not empty, 'number_sources' is not empty, and '1d_pipeline' column is empty
    
    Args:
    band_number (int): The band number (1 or 2) to check.
    Google_API_token (str): The path to the Google API token JSON file.
    
    Returns:
    list: A list of tile numbers that satisfy the conditions.
    """

    correct_options = ['centers']#, 'edges']
    # TODO: include 'edges' as option for whichpart 
    if whichpart not in correct_options:
        raise ValueError(f"{whichpart=} but valid options are {correct_options}")

    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    # "POSSUM Pipeline Validation" sheet (maintained by Erik)
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k')

    # Select the worksheet for the given band number
    tile_sheet = ps.worksheet(f'Partial Tile Pipeline - {whichpart} - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    # Find the tiles that satisfy the conditions
    if whichpart == 'centers':
        fields_to_run = [row['name'] for row in tile_table if row['sbid'] != '' and row['number_sources'] != '' and row['1d_pipeline'] == '']
        tiles_to_run = [row['associated_tile'] for row in tile_table if row['sbid'] != '' and row['number_sources'] != '' and row['1d_pipeline'] == '']
    elif whichpart == 'edges':
        raise NotImplementedError("TODO")

    return fields_to_run, tiles_to_run

def get_canfar_sourcelists(band_number):
    client = Client()
    # force=True to not use cache
    # assumes directory structure doesnt change and symlinks are created
    if band_number == 1:
        canfar_sourcelists = client.listdir("vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/",force=True)
    elif band_number == 2:
        # TODO
        raise NotImplementedError("TODO")
        canfar_sourcelists = client.listdir("vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/Tiles/1367MHz/",force=True)
    else:
        raise ValueError(f"Band number {band_number} not defined")
    return canfar_sourcelists

def field_from_sourcelist_string(srclist_str):
    """
    Extract field ID from sourcelist string

    e.g. 

    selavy-image.i.EMU_1441+04B.SB59835.cont.taylor.0.restored.conv.components.xml

    returns 

    1441+04
    """
    if "EMU" in srclist_str:
        field_ID = srclist_str.split("EMU_")[1][:7]
    elif "WALLABY" in srclist_str:
        field_ID = srclist_str.split("WALLABY_")[1][:7]
    else:
        field_ID = None
        print(f"Warning, could not find field_ID for sourcelist {srclist_str}")
    return field_ID

def launch_pipeline(field_ID, tilenumber, band):
    """
    # Launch the appropriate 3D pipeline script based on the band

    field_ID   -- str/int -- 7 char fieldID, e.g. 1412-28
    tilenumber -- str/int -- 4 or 5 digit tilenumber, e.g. 8972
    band       -- str     -- '943MHz' or '1367MHz' for Band 1 or Band 2

    """
    if band == "943MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band1.py", str(tilenumber)]
    elif band == "1367MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band2.py", str(tilenumber)]
        command = ""
        raise NotImplementedError("TODO: Temporarily disabled launching band 2 because need to write that run script")
    else:
        raise ValueError(f"Unknown band: {band}")

    print(f"Running command: {' '.join(command)}")
    subprocess.run(command, check=True)

def update_status(field_ID, tile_number, band, Google_API_token, status, whichpart='centers'):
    """
    Update the status of the specified partial tile in the Google Sheet.

    A Partial Tile is uniquely defined by field_ID + tile_number
    
    Args:
    field_ID (str): The field_ID to update, e.g. "1412-28"
    tile_number (str): The tile number to update. e.g. "8971"
    band (str): The band of the tile.
    Google_API_token (str): The path to the Google API token JSON file.
    status (str): The status to set in the '1d_pipeline' column.
    """
    correct_options = ['centers']#, 'edges']
    # TODO: include 'edges' as option for whichpart 
    if whichpart not in correct_options:
        raise ValueError(f"{whichpart=} but valid options are {correct_options}")

    # Make sure its not int
    tile_number = str(tile_number)

    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    # POSSUM Pipeline Validation sheet, maintained by Erik
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k')

    # Select the worksheet for the given band number
    band_number = '1' if band == '943MHz' else '2'
    tile_sheet = ps.worksheet(f'Partial Tile Pipeline - {whichpart} - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    fieldname = "EMU_" if band == '943MHz' else 'WALLABY_' # TODO: verify WALLABY_ fieldname
    # Find the row index for the specified tile number
    tile_index = None
    for idx, row in enumerate(tile_table):
        if row['associated_tile'] == tile_number and row['name'] == f"{fieldname}{field_ID}":
            tile_index = idx + 2  # +2 because gspread index is 1-based and we skip the header row
            break
    
    if tile_index is not None:
        # Update the status in the '1d_pipeline' column
        col_letter = gspread.utils.rowcol_to_a1(1, column_names.index('1d_pipeline') + 1)[0]
        # as of >v6.0.0 .update requires a list of lists
        tile_sheet.update(range_name=f'{col_letter}{tile_index}', values=[[status]])
        print(f"Updated tile {tile_number} status to {status} in '1d_pipeline' column.")
    else:
        print(f"Field {fieldname}{field_ID} with tile {tile_number} not found in the sheet.")

def launch_band1_1Dpipeline():
    """
    Launch a headless job to CANFAR for a 1D pipeline Partial Tile
    """
    band = "943MHz"
    # on p1, token for accessing Erik's google sheets 
    # consider chmod 600 <file> to prevent access
    Google_API_token = "/home/erik/.ssh/neural-networks--1524580309831-c5c723e2468e.json"
    
    # Check google sheet for band 1 tiles that have been ingested into CADC 
    # (and thus available on CANFAR) but not yet processed with 3D pipeline
    field_IDs, tile_numbers = get_tiles_for_pipeline_run(band_number=1, Google_API_token=Google_API_token)
    # list of full sourcelist filenames
    canfar_sourcelists = get_canfar_sourcelists(band_number=1)
    # list of only the field IDs e.g. "1428-12"
    sourcelist_fieldIDs = [field_from_sourcelist_string(srl) for srl in canfar_sourcelists]
    sleep(1)

    assert len(field_IDs) == len(tile_numbers) # need these two numbers to define what to run

    if len(field_IDs) > 0:
        print(f"Found {len(field_IDs)} partial tiles in Band 1 ready to be processed with 1D pipeline")
        print(f"On CANFAR, found {len(sourcelist_fieldIDs)} sourcelists for Band 1")

        if len(field_IDs) > len(sourcelist_fieldIDs):
            tiles_ready_but_not_canfar = set(field_IDs) - set(sourcelist_fieldIDs)
            print(f"Field IDs ready according to the sheet but sourcelist not on CANFAR: {tiles_ready_but_not_canfar}")
        else:
            tiles_canfar_not_ready = set(sourcelist_fieldIDs) - set(field_IDs)
            print(f"Field ID sourcelists on CANFAR but not ready to run: {tiles_canfar_not_ready}")

        fields_on_both = set(field_IDs) & set(sourcelist_fieldIDs)
        # print(f"Fields ready on both CADC and CANFAR: {tiles_on_both}")

        if fields_on_both:
            # Launch the first field_ID that has a sourcelist (assumes this script will be called many times)
            for i in range(len(field_IDs)):
                field_ID = field_IDs[i]
                tilenumber = tile_numbers[i]
                if field_ID not in fields_on_both:
                    print(f"Skipping {field_ID} as it doesnt have a sourcelist on CANFAR")
                    continue
                else:
                    print(f"\nLaunching headless job for 1D pipeline for field {field_ID} and tile {tilenumber}")

                    # Launch the pipeline
                    launch_pipeline(field_ID, tilenumber, band)
                    
                    # Update the status to "Running"
                    update_status(field_ID, tilenumber, band, Google_API_token, "Running")
            
        else:
            print("No tiles are available on both CADC and CANFAR.")
    else:
        print("Found no tiles ready to be processed.")

if __name__ == "__main__":
    launch_band1_1Dpipeline()
