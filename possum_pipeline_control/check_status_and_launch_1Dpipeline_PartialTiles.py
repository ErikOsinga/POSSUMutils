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

def get_results_per_field_sbid(tile_table, verbose=False):
    """
    Group google validation sheet by field name and sbid
    
    Returns a dict of type {('fieldname','sbid'):boolean}
    
    If all Partial tiles for a fieldname have been completed boolean=True, otherwise false.
    """
    # Group the table by 'field_name' and 'sbid'
    grouped_table = tile_table.group_by(['field_name', 'sbid'])

    # Check conditions for each group
    results = {}
    for group in grouped_table.groups:
        field_name = group['field_name'][0]  # Get the field name for the group
        sbid = group['sbid'][0]  # Get the sbid for the group
        # Check if all '1d_pipeline_validation' are empty and '1d_pipeline' is "Completed"
        all_conditions_met = (
            all(group['1d_pipeline_validation'] == '') and 
            all(group['1d_pipeline'] == 'Completed')
        )
        results[(field_name, sbid)] = all_conditions_met

    if verbose:
        for (field, sbid), conditions_met in results.items():
            status = "all conditions met" if conditions_met else "conditions not met"
            print(f"Field name '{field}', SBID '{sbid}' has 1d_pipeline_validation and 1d_pipeline {status}.")

    return results

def get_tiles_for_pipeline_run(band_number, Google_API_token):
    """
    Get a list of tile numbers that should be ready to be processed by the 1D pipeline 
    
    i.e.  'SBID' column is not empty, 'number_sources' is not empty, and '1d_pipeline' column is empty
    
    Args:
    band_number (int): The band number (1 or 2) to check.
    Google_API_token (str): The path to the Google API token JSON file.
    
    Returns:
    list: A list of tile numbers that satisfy the conditions.
    """

    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    # "POSSUM Pipeline Validation" sheet (maintained by Erik)
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k')

    # Select the worksheet for the given band number
    tile_sheet = ps.worksheet(f'Partial Tile Pipeline - regions - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    # Find the tiles that satisfy the conditions (i.e. has an SBID and not yet a '1d_pipeline' status)
    fields_to_run = [row['field_name'].strip("EMU_").strip("WALLABY_") for row in tile_table if row['sbid'] != '' and row['number_sources'] != '' and row['1d_pipeline'] == '']
    tile1_to_run = [row['tile1'] for row in tile_table if row['sbid'] != '' and row['number_sources'] != '' and row['1d_pipeline'] == '']
    tile2_to_run = [row['tile2'] for row in tile_table if row['sbid'] != '' and row['number_sources'] != '' and row['1d_pipeline'] == '']
    tile3_to_run = [row['tile3'] for row in tile_table if row['sbid'] != '' and row['number_sources'] != '' and row['1d_pipeline'] == '']
    tile4_to_run = [row['tile4'] for row in tile_table if row['sbid'] != '' and row['number_sources'] != '' and row['1d_pipeline'] == '']
    SBids_to_run = [row['sbid'] for row in tile_table if row['sbid'] != '' and row['number_sources'] != '' and row['1d_pipeline'] == '']

    # Also find fields that have all partial tiles completed, but validation plot not yet made
    results_grouped = get_results_per_field_sbid(tile_table)
    # return only a list of (fieldnames,sbid) pairs for which we can start a validation job
    can_make_validation = [key for (key,value) in results_grouped.items() if value]

    return fields_to_run, tile1_to_run, tile2_to_run, tile3_to_run, tile4_to_run, SBids_to_run, can_make_validation

def get_canfar_sourcelists(band_number):
    client = Client()
    # force=True to not use cache
    # assumes directory structure doesnt change and symlinks are created
    if band_number == 1:
        # canfar_sourcelists = client.listdir("vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/",force=True)
        # disabled above command due to issue with client.listdir for many files https://github.com/opencadc/vostools/issues/228
        
        cmd = "vls vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/"
        result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
        if result.returncode == 0:
            output = result.stdout
            canfar_sourcelists = output.splitlines()
        else:
            print("Error running command. Perhaps CANFAR is down?")
            raise ValueError(f"Error running command: {cmd}\n{result.stderr}")

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

def launch_pipeline(field_ID, tilenumbers, SBid, band):
    """
    # Launch the appropriate 1D pipeline script based on the band

    field_ID    -- str/int         -- 7 char fieldID, e.g. 1412-28
    tilenumbers -- list of str/int -- list of up to 4 tile numbers: a tile number is a 4 or 5 digit tilenumber, e.g. 8972, if no number it's an empty string '' 
    SBid        -- str/int         -- 5 (?) digit SBid, e.g. 50413
    band        -- str             -- '943MHz' or '1367MHz' for Band 1 or Band 2

    """
    if band == "943MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band1.py", str(field_ID), str(tilenumbers), str(SBid)]
    elif band == "1367MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band2.py", str(field_ID), str(tilenumbers), str(SBid)]
        command = ""
        raise NotImplementedError("TODO: Temporarily disabled launching band 2 because need to write that run script")
    else:
        raise ValueError(f"Unknown band: {band}")

    print(f"Running command: {' '.join(command)}")
    subprocess.run(command, check=True)

def launch_pipeline_summary(field_ID, SBid, band):
    """
    # Launch the appropriate 1D pipeline summary script based on the band

    field_ID    -- str/int         -- 7 char fieldID, e.g. 1412-28
    SBid        -- str/int         -- 5 (?) digit SBid, e.g. 50413
    band        -- str             -- '943MHz' or '1367MHz' for Band 1 or Band 2

    """
    if band == "943MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band1_pre_or_post.py", str(field_ID), str(SBid), "post"]
    elif band == "1367MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band2_pre_or_post.py", str(field_ID), str(SBid)]
        command = ""
        raise NotImplementedError("TODO: Temporarily disabled launching band 2 because need to write that run script")
    else:
        raise ValueError(f"Unknown band: {band}")

    print(f"Running command: {' '.join(command)}")
    subprocess.run(command, check=True)    

def update_status(field_ID, tile_numbers, SBid, band, Google_API_token, status, status_column='1d_pipeline'):
    """
    Update the status of the specified partial tile or all rows for a given field_name and sbid in the Google Sheet.

    A Partial Tile is uniquely defined by field_ID + sbid + tile_number.
    If tile_numbers is None, update all rows that match the field_ID and sbid.
    
    Args:
    field_ID (str): The field_ID to update, e.g. "1412-28".
    tile_numbers (list[str|int]|None): List of up to 4 tile numbers, or None to update all matching rows.
    band (str): The band of the tile.
    Google_API_token (str): The path to the Google API token JSON file.
    status (str): The status to set in the specified column.
    status_column (str): The column to update in the sheet. Defaults to '1d_pipeline'.
    """
    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    # POSSUM Pipeline Validation sheet, maintained by Erik
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k')

    # Select the worksheet for the given band number
    band_number = '1' if band == '943MHz' else '2'
    tile_sheet = ps.worksheet(f'Partial Tile Pipeline - regions - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    # Build field name prefix
    fieldname = "EMU_" if band == '943MHz' else 'WALLABY_'  # TODO: verify WALLABY_ fieldname
    full_field_name = f"{fieldname}{field_ID}"

    # Update all rows belonging to field and sbid
    if tile_numbers is None:
        # Update all rows that match field_name and sbid
        rows_to_update = [
            idx + 2  # +2 because gspread index is 1-based and skips the header row
            for idx, row in enumerate(tile_table)
            if row['field_name'] == full_field_name and row['sbid'] == SBid
        ]
        if rows_to_update:
            col_letter = gspread.utils.rowcol_to_a1(1, column_names.index(status_column) + 1)[0]
            for row_index in rows_to_update:
                sleep(1) # 60 writes per minute only
                tile_sheet.update(range_name=f'{col_letter}{row_index}', values=[[status]])
            print(f"Updated all {len(rows_to_update)} rows for field {full_field_name} and SBID {SBid} to status '{status}' in '{status_column}' column.")
        else:
            print(f"No rows found for field {full_field_name} and SBID {SBid}.")
    # Update one specific row
    else:
        # Extract individual tile numbers
        t1, t2, t3, t4 = tile_numbers
        # Find the row index for the specified tile numbers
        tile_index = None
        for idx, row in enumerate(tile_table):
            if (row['tile1'] == t1
                and row['tile2'] == t2
                and row['tile3'] == t3
                and row['tile4'] == t4
                and row['field_name'] == full_field_name
                and row['sbid'] == SBid):
                tile_index = idx + 2  # +2 because gspread index is 1-based and we skip the header row
                break
        if tile_index is not None:
            # Update the status in the specified column
            col_letter = gspread.utils.rowcol_to_a1(1, column_names.index(status_column) + 1)[0]
            tile_sheet.update(range_name=f'{col_letter}{tile_index}', values=[[status]])
            print(f"Updated row with tiles {tile_numbers} to status '{status}' in '{status_column}' column.")
        else:
            print(f"Field {full_field_name} with tiles {tile_numbers} not found in the sheet.")


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
    field_IDs, tile1, tile2, tile3, tile4, SBids, fields_to_validate = get_tiles_for_pipeline_run(band_number=1, Google_API_token=Google_API_token)
    assert len(tile1) == len(tile2) == len(tile3) == len(tile4), "Need to have 4 tile columns in google sheet. Even if row can be empty."
    # list of full sourcelist filenames
    canfar_sourcelists = get_canfar_sourcelists(band_number=1) 
    # canfar_sourcelists = ['selavy-image.i.EMU_0314-46.SB59159.cont.taylor.0.restored.conv.components.15sig.xml',
    #                       'selavy-image.i.EMU_0052-37.SB46971.cont.taylor.0.restored.conv.components.15sig.xml']
    # list of only the field IDs e.g. "1428-12"
    sourcelist_fieldIDs = [field_from_sourcelist_string(srl) for srl in canfar_sourcelists]
    sleep(1) # google sheet shenanigans

    assert len(field_IDs) == len(tile1) == len(SBids) # need fieldID, up to 4 tileIDs and SBID to define what to run

    # First launch ALL 1D pipeline summary plot jobs if any fields are fully finished
    if len(fields_to_validate) > 0:
        print(f"Found {len(fields_to_validate)} fields that are fully finshed: {fields_to_validate}\n")

        for (field, sbid) in fields_to_validate:
            print(f"Launching job to create summary plot for field {field} with sbid {sbid}")
            if band == "943MHz":
                field = field.strip("EMU_")

            launch_pipeline_summary(field, sbid, band)

            # Update the status of the '1d_pipeline_validation' column to "Running" regardless of tile number
            update_status(field, None, sbid, band, Google_API_token, "Running", status_column='1d_pipeline_validation')

    if len(field_IDs) > 0:
        print(f"Found {len(field_IDs)} partial tiles in Band 1 ready to be processed with 1D pipeline")
        print(f"On CANFAR, found {len(sourcelist_fieldIDs)} sourcelists for Band 1")

        # if len(field_IDs) > len(sourcelist_fieldIDs):
        tiles_ready_but_not_canfar = set(field_IDs) - set(sourcelist_fieldIDs)
        print(f"Field IDs ready according to the sheet but sourcelist not on CANFAR: {tiles_ready_but_not_canfar}")
        # else:
        tiles_canfar_not_ready = set(sourcelist_fieldIDs) - set(field_IDs)
        print(f"Field ID sourcelists on CANFAR but not ready to run: {tiles_canfar_not_ready}")

        fields_on_both = set(field_IDs) & set(sourcelist_fieldIDs)
        # print(f"Fields ready on both CADC and CANFAR: {tiles_on_both}")

        if fields_on_both:
            # Launch the first field_ID that has a sourcelist (assumes this script will be called many times)
            for i in range(len(field_IDs)):
                field_ID = field_IDs[i]
                t1 = tile1[i]
                t2 = tile2[i]
                t3 = tile3[i]
                t4 = tile4[i]
                tilenumbers = [t1, t2, t3, t4] # up to four tilenumbers, or less with '' (empty strings)
                SBid = SBids[i]
                if field_ID not in fields_on_both:
                    print(f"Skipping {field_ID} as it doesnt have a sourcelist on CANFAR")
                    continue
                else:
                    print(f"\nLaunching headless job for 1D pipeline for field {field_ID} observed in SBid {SBid} covering partial tiles {tilenumbers} ")

                    # Launch the pipeline
                    launch_pipeline(field_ID, tilenumbers, SBid, band)
                    
                    # Update the status to "Running"
                    update_status(field_ID, tilenumbers, SBid, band, Google_API_token, "Running", status_column="1d_pipeline")

                    break
            
        else:
            print("No tiles are available on both CADC and CANFAR.")
    else:
        print("Found no tiles ready to be processed.")

if __name__ == "__main__":
    launch_band1_1Dpipeline()
