#!/usr/bin/env python
import sys
import os
import gspread
import numpy as np
import subprocess
import astropy.table as at
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time
from datetime import datetime
sys.path.append('../cirada_software/') # to import update_status_spreadsheet
from log_processing_status_1D_PartialTiles_summary import update_status_spreadsheet

"""
Run "check_status_and_launch_1Dpipeline_PartialTiles.py 'pre' " based on Camerons' POSSUM Pipeline Status google sheet.
That will download the tiles in a CANFAR job and populate the google sheet.

"""

def get_ready_fields(band):
    """
    Connects to the POSSUM Status Monitor Google Sheet and returns a sub-table
    containing only the rows where the 'sbid' and 'aus_src' fields are not empty and 'single_SB_1D_pipeline' is empty.
    
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
    return ready_table, tile_table

def launch_pipeline_command(fieldname, SBID):
    """
    Launches the 1D pipeline pre-or-post script for a given field and SBID.
    
    The command executed is:
        python launch_1Dpipeline_PartialTiles_band1_pre_or_post.py {fieldname} {SBID} pre
    """
    command = f"python launch_1Dpipeline_PartialTiles_band1_pre_or_post.py {fieldname} {SBID} pre"
    print(f"Executing command: {command}")
    subprocess.run(command, shell=True, check=True)

def check_validation_sheet_integrity(band_number=1, verbose=False):
    """
    Check if each field+sbid+tile combination is present in the sheet exactly once. 
    This should be the case, but sometimes I have seen identical rows with different 'number_sources'
     
    """
    # grab google sheet POSSUM validation

    # on p1, token for accessing Erik's google sheets 
    # consider chmod 600 <file> to prevent access
    # check for each row if it is present exactly once, irrespetive of the number of sources
    Google_API_token = "/home/erik/.ssh/neural-networks--1524580309831-c5c723e2468e.json"
    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    # "POSSUM Pipeline Validation" sheet (maintained by Erik)
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k')

    # Select the worksheet for the given band number
    tile_sheet = ps.worksheet(f'Partial Tile Pipeline - regions - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    sheet_is_ok = True
    for i, row in enumerate(tile_table):
        field_name, sbid, tile1, tile2, tile3, tile4, region_type, num_sources, pstatus, vstatus = row
        mask = (tile_table['field_name'] == field_name) & (tile_table['sbid'] == sbid) & \
                (tile_table['tile1'] == tile1) & (tile_table['tile2'] == tile2) & \
                (tile_table['tile3'] == tile3) & (tile_table['tile4'] == tile4) & \
                (tile_table['type'] == region_type) # & (tile_table['number_sources'] == num_sources)
        
        if np.sum(mask) == 1:
            continue
        elif np.sum(mask) == 0:
            print(f"row index={i} not found in google sheet") # shouldnt happpen
            sheet_is_ok = False
        else:
            print(f"Found row index={i} multiple times in Google Sheet. In location (index) {np.where(mask)[0]}")
            sheet_is_ok = False
            # raise ValueError(f"Somehow found this row multiple times in Google Sheet, check rows {np.where(mask)[0]}")
    
    if sheet_is_ok:
        print("Google Sheet is OK. No duplicate rows found.")
    else:
        print("Google Sheet is NOT OK. Duplicate rows found.")

def extract_date(entry):
    # Remove extra whitespace
    entry = entry.strip()
    # Use regex to match the exact pattern "PartialTiles(BI) - YYYY-MM-DD"
    # This regex allows for optional whitespace around the hyphen
    match = re.match(r'^PartialTiles(?:BI)?\s*-\s*(\d{4}-\d{2}-\d{2})$', entry)

    if match:
        return match.group(1)
    else:
        return np.nan
    
def create_progress_plot(full_table):
    """ to be run on p1

    Create progress plots for the 1D pipeline:
     - A scatter plot per field.
     - A cumulative plot showing the total number of fields processed vs. date.
    """
    # Extract finish times using your helper (make sure extract_date is defined)
    full_table['pipeline_finish_time'] = [extract_date(x) for x in full_table['single_SB_1D_pipeline']]
    
    # Remove rows where pipeline_finish_time is NaN
    # Note: If extract_date returns np.nan for invalid entries, use pd.isna to filter
    full_table = full_table[(full_table['pipeline_finish_time'] != 'nan')]
    
    # Convert finish times to datetime objects
    times = [datetime.strptime(t, '%Y-%m-%d') for t in full_table['pipeline_finish_time']]
    
    # ----------------------------
    # 1. Per-field Scatter Plot
    # ----------------------------
    plt.figure(figsize=(10, 6))
    plt.scatter(times, full_table['name'], c='blue', alpha=0.5)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.title('1D Pipeline Progress')
    plt.xlabel('Finish Time')
    plt.ylabel('Field Name')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.grid()
    plt.savefig('./plots/1D_pipeline_progress_per_field.png')
    plt.show()
    plt.close()
    
    # ----------------------------
    # 2. Cumulative Progress Plot
    # ----------------------------
    # Create a DataFrame with the finish times
    df = pd.DataFrame({'date': times})
    # Normalize dates to midnight (in case there are time differences)
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    # Count how many fields were processed each day
    daily_counts = df.groupby('date').size().sort_index()
    # Calculate the cumulative sum of fields processed over time
    cumulative_counts = daily_counts.cumsum()
    
    plt.figure(figsize=(10, 6))
    plt.plot(cumulative_counts.index, cumulative_counts.values, marker='o', linestyle='-')
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.title(f'Cumulative 1D Pipeline Progress as of {datetime.now().strftime("%Y-%m-%d")}')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Fields Processed')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.grid()
    plt.savefig('./plots/1D_pipeline_progress_cumulative.png')
    plt.show()
    plt.close()

def launch_collate_job():
    """
    Launches the collate job for the 1D pipeline. Once per day
    """
    from skaha.session import Session
    session = Session()

    # e.g. for band 1
    basedir = "/arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/"
    # Template bash script to run
    args = f"/arc/projects/CIRADA/polarimetry/software/POSSUMutils/cirada_software/collate_1Dpipeline_PartialTiles.sh {basedir}"

    print("Launching collate job")
    print(f"Command: bash {args}")

    run_name = "collate"
    image = "images.canfar.net/cirada/possumpipelineprefect-3.12:v1.11.0" # v1.12.1 has astropy issue https://github.com/astropy/astropy/issues/17497
    # good default values
    cores = 4
    ram = 40 # Check allowed values at canfar.net/science-portal

    session_id = session.create(
        name=run_name.replace('_', '-'),  # Prevent Error 400: name can only contain alpha-numeric chars and '-'
        image=image,
        cores=cores,
        ram=ram,
        kind="headless",
        cmd="bash",
        args=args,
        replicas=1,
        env={},
    )

    print("Check sessions at https://ws-uv.canfar.net/skaha/v0/session")
    print(f"Check logs at https://ws-uv.canfar.net/skaha/v0/session/{session_id[0]}?view=logs")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description="Update Partial Tile Google Sheet")
    # parser.add_argument("band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile")
    # args = parser.parse_args()
    # band = args.band
    
    band = "943MHz" # hardcode for now

    # Update the POSSUM Pipeline Status spreadsheet as well. A complete field is being processed!
    Google_API_token = "/home/erik/.ssh/psm_gspread_token.json"
    # put the status as PartialTiles - Running
    
    ready_table, full_table = get_ready_fields(band)

    print(f"Found {len(ready_table)} fields ready for single SB partial tile pipeline processing in band {band}")
    
    # If the plot hasnt been updated in a day, re-run the plot
    local_file = "./plots/1D_pipeline_progress_cumulative.png"
    if not os.path.exists(local_file):
        print("Creating 1D pipeline progress plot")
        create_progress_plot(full_table)
        print("Collating all the 1D pipeline outputs")
        launch_collate_job()        
    else:
        file_mod_time = os.path.getmtime(local_file)
        if (time.time() - file_mod_time) > 86400:  # 86400 seconds = 1 day
            print("Updating 1D pipeline progress plot")
            create_progress_plot(full_table)
            print("Collating all the 1D pipeline outputs")
            launch_collate_job()

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
        
        ## NOTE: actually better to do in the launched script
        # # update the status in Cameron's spreadsheet
        # status_to_put = "PartialTiles - Running"
        # update_status_spreadsheet(fieldname, SBID, band, Google_API_token, status_to_put, 'single_SB_1D_pipeline')
    
        break # only do one every time the script is called
