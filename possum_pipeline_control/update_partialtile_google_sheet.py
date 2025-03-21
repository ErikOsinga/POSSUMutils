#!/usr/bin/env python
import sys
import argparse
import gspread
import numpy as np
import subprocess
import astropy.table as at
sys.path.append('../cirada_software/') # to import update_status_spreadsheet
from log_processing_status_1D_PartialTiles_summary import update_status_spreadsheet

"""
Run "check_status_and_launch_1Dpipeline_PartialTiles.py 'pre' " based on Camerons' POSSUM Pipeline Status google sheet.
That will download the tiles in a CANFAR job and populate the google sheet.

"""

def get_ready_fields(band):
    """
    Connects to the POSSUM Status Monitor Google Sheet and returns a sub-table
    containing only the rows where the 'sbid' and 'aus_src' fields are not empty.
    
    Uses the following Google API token on p1:
        /home/erik/.ssh/psm_gspread_token.json
    
    And the sheet URL:
        https://docs.google.com/spreadsheets/d/1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0
    
    The worksheet is selected based on the band:
        '1' if band == '943MHz', otherwise '2'.
    """
    # POSSUM Status Monitor
    Google_API_token = "/home/erik/.ssh/psm_gspread_token.json"
    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0')
    
    # Select the worksheet for the given band number
    band_number = '1' if band == '943MHz' else '2'
    tile_sheet = ps.worksheet(f'Survey Fields - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)
    
    # Find all rows that have "sbid" not empty and "aus_src" not empty and 'single_SB_1D_pipeline' empty
    mask = (tile_table['sbid'] != '') & (tile_table['aus_src'] != '') & (tile_table['single_SB_1D_pipeline'] == '')
    ready_table = tile_table[mask]
    return ready_table

def launch_pipeline_command(fieldname, SBID):
    """
    Launches the 1D pipeline pre-or-post script for a given field and SBID.
    
    The command executed is:
        python launch_1Dpipeline_PartialTiles_band1_pre_or_post.py {fieldname} {SBID} pre
    """
    command = f"python launch_1Dpipeline_PartialTiles_band1_pre_or_post.py {fieldname} {SBID} pre"
    print(f"Executing command: {command}")
    subprocess.run(command, shell=True, check=True)

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Update Partial Tile Google Sheet")
    # parser.add_argument("band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile")
    # args = parser.parse_args()
    # band = args.band
    
    band = "943MHz" # hardcode for now

    # Update the POSSUM Pipeline Status spreadsheet as well. A complete field is being processed!
    Google_API_token = "/home/erik/.ssh/psm_gspread_token.json"
    # put the status as PartialTiles - Running
    
    
    ready_table = get_ready_fields(band)
    
    # Loop over each row in the returned table to launch the pipeline command.
    # The 'fieldname' is taken from the column "name" with "EMU_" stripped if present.
    # The SBID is taken from the column "sbid".
    for row in ready_table:
        fieldname = row["name"]
        if fieldname.startswith("EMU_"):
            fieldname = fieldname[len("EMU_"):]
        SBID = row["sbid"]
        # Launch the job that downloads the tiles and populates Erik's google sheet
        launch_pipeline_command(fieldname, SBID)
        # update the status in Cameron's spreadsheet
        status_to_put = "PartialTiles - Running"
        update_status_spreadsheet(fieldname, SBID, band, Google_API_token, status_to_put, 'single_SB_1D_pipeline')
    
        break # only do one every time the script is called
