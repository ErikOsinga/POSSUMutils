"""
To be put on CANFAR /software/

Calls Sharon Goliath's ingest script ("possum_run") for a specific tile number and band
to ingest 3D pipeline products.


Arguments

tilenumber -- int -- which tile number to process
band       -- str -- either "943MHz" or "1367MHz" for band 1 or band 2 data

"""

import argparse
import os
from prefect import flow, task
import glob
import os
import gspread
import numpy as np
import astropy.table as at
import astroquery.cadc as cadc
import datetime
from possum2caom2.composable import run as possum_run


# 17 products for the 3D pipeline
all_3dproducts = [ 
   'FDF_im_dirty_3d_pipeline',
 'FDF_real_dirty_3d_pipeline',
  'FDF_tot_dirty_3d_pipeline',
        'FDF_im_dirty_p3d_v1',
      'FDF_real_dirty_p3d_v1',
           'RMSF_FWHM_p3d_v1',
            'RMSF_tot_p3d_v1',
         'amp_peak_pi_p3d_v1',
            'frac_pol_p3d_v1',
             'i_model_p3d_v1',
                'misc_p3d_v1',
     'phi_peak_pi_fit_p3d_v1',
     'pol_angle_0_fit_p3d_v1',
          'snr_pi_fit_p3d_v1',
       'FDF_tot_dirty_p3d_v1',
             'RMSF_im_p3d_v1',
           'RMSF_real_p3d_v1'
]

@task
def replace_working_directory_and_save(file_path, tile_workdir):
    # Read the content of the original file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Path to the new file
    new_file_path = os.path.join(tile_workdir, 'config.yml')
    print(f"Creating config file {new_file_path}")

    # Write the updated content to the new file
    with open(new_file_path, 'w') as new_file:
        for line in lines:
            if line.startswith("working_directory:"):
                new_file.write(f"working_directory: {tile_workdir}\n")
            else:
                new_file.write(line)

    return new_file_path

@task
def launch_ingestscript(tile_workdir):
    """change workdir and launch ingest script"""

    # Start possum_run in correct workdir
    os.chdir(tile_workdir)
    possum_run()
    return

@task
def check_report(tile_workdir):
    """Check the report file for the number of inputs and successes"""
    report_file = f"{tile_workdir}_report.txt"
    
    if not os.path.exists(report_file):
        print(f"Report file {report_file} does not exist.")
        return False
    
    inputs, successes = None, None

    with open(report_file, 'r') as file:
        for line in file:
            if "Number of Inputs" in line:
                inputs = int(line.split(":")[1].strip())
            if "Number of Successes" in line:
                successes = int(line.split(":")[1].strip())

    # Expect 24 files for the 3D pipeline data.
    if inputs == 24 and successes == 24:
        print("Ingest was successful.")
        return True
    else:
        print("Something has gone wrong with the ingest.")
        return False

@task
def update_validation_spreadsheet(tile_number, band, Google_API_token, status):
    """
    Update the status of the specified tile in the VALIDATION Google Sheet.
    (see also log_processing_status.py)
    
    Args:
    tile_number (str): The tile number to update.
    band (str): The band of the tile.
    Google_API_token (str): The path to the Google API token JSON file.
    status (str): The status to set in the '3d_pipeline' column.
    """
    print("Updating POSSUM pipeline validation sheet")

    # Make sure its not int
    tile_number = str(tile_number)
    
    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    # POSSUM Validation spreadsheet
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
        # Status should be "IngestRunning" otherwise something went wrong
        if row['3d_pipeline'] != "IngestRunning":
            raise ValueError(f"Found status {row['3d_pipeline']} while it should be 'IngestRunning'")

        # Update the status in the '3d_pipeline' column
        col_letter = gspread.utils.rowcol_to_a1(1, column_names.index('3d_pipeline') + 1)[0]
        # as of >v6.0.0 the .update function requires a list of lists
        tile_sheet.update(range_name=f'{col_letter}{tile_index}', values=[[status]])
        print(f"Updated tile {tile_number} status to {status} in '3d_pipeline' column.")

    else:
        raise ValueError(f"Tile {tile_number} not found in the sheet.")

@task
def check_CADC(tilenumber, band):
    """
    query CADC for 3D pipeline products
    
    Based on Cameron's update_CADC_tile_status.py
    """
    ## cadc-get-cert on CANFAR
    CADC_cert_file = "/arc/home/ErikOsinga/.ssl/cadcproxy.pem"
    CADC_session = cadc.Cadc()
    CADC_session.login(certificate_file=CADC_cert_file)

    query=CADC_session.create_async("""SELECT observationID,Plane.productID,Observation.lastModified FROM caom2.Plane AS Plane 
	JOIN caom2.Observation AS Observation ON Plane.obsID = Observation.obsID 
    WHERE  (Observation.collection = 'POSSUM') AND (observationID NOT LIKE '%pilot1') """)
    query.run().wait()  
    query.raise_if_error()
    result=query.fetch_result().to_table()
    result.add_column([x.split('_')[-2] for x in result['observationID']], name='tile_number')
    result.add_column([x.split('MHz')[0] for x in result['observationID']], name='freq') # str4
    
    freq = band.replace("MHz","")

    # get all observationIDs, productIDs, etc (CADC output) for this tile number
    result_tile = result[result["tile_number"] == tilenumber]

    # get all products that have the correct frequency
    result_tile_band = result_tile[result_tile["freq"] == freq]

    # For 3D pipeline, there should be 17 products (and 3 inputs)
    for product in all_3dproducts:
        if product not in result_tile_band['productID']:
            print(f"Missing product {product}")
            return False, None
        
    dt=[datetime.datetime.fromisoformat(x) for x in result_tile_band['lastModified']]
    last_modified=max(dt)
    date = last_modified.date().isoformat()

    return True, date

@task
def update_status_spreadsheet(tile_number, band, Google_API_token, status):
    """
    Update the status of the specified tile in the Google Sheet.
    
    Args:
    tile_number (str): The tile number to update.
    band (str): The band of the tile.
    Google_API_token (str): The path to the Google API token JSON file.
    status (str): The status to set in the '3d_pipeline' column.
    """
    print("Updating POSSUM status sheet")

    # Make sure its not int
    tile_number = str(tile_number)
    
    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0')

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
        col_letter = gspread.utils.rowcol_to_a1(1, column_names.index('3d_pipeline') + 1)[0]
        # as of >v6.0.0 the .update function requires a list of lists
        tile_sheet.update(range_name=f'{col_letter}{tile_index}', values=[[status]])
        print(f"Updated tile {tile_number} status to {status} in '3d_pipeline' column.")
    else:
        print(f"Tile {tile_number} not found in the sheet.")

@flow
def do_ingest(tilenumber, band):
    """Does the ingest script
    
    1. Create config.yml based on template
    2. Execute "possum_run" in the correct directory
    """

    config_template = "/arc/projects/CIRADA/polarimetry/ASKAP/Pipeline_logs/config_templates/config_ingest.yml"
    tile_workdir = f"/arc/projects/CIRADA/polarimetry/pipeline_runs/{band}/tile{tilenumber}/"

    # Create config file and put it in the correct directory
    replace_working_directory_and_save(config_template, tile_workdir)

    # Launch 'possum_run'
    launch_ingestscript(tile_workdir)

    # Check the ingest report file
    success = check_report(tile_workdir)

    CADCsuccess, date = check_CADC(tilenumber, band)

    # Check the CADC also if indeed all files are there
    status = "IngestFailed"
    if success:
        if CADCsuccess:
            status = "Ingested"
        else:
            print("_report.txt reports succesful ingest, but CADC ingest failed")
    else:
        print("_report.txt reports that ingestion failed")

    # Record the status in the POSSUM Validation spreadsheet
    Google_API_token = "/arc/home/ErikOsinga/.ssh/neural-networks--1524580309831-c5c723e2468e.json"
    update_validation_spreadsheet(tilenumber, band, Google_API_token, status)

    # If succesful, also record the date of ingestion in POSSUM status spreadsheet
    # Update the POSSUM status monitor google sheet (see also log_processing_status.py)
    Google_API_token = "/arc/home/ErikOsinga/.ssh/psm_gspread_token.json"
    update_status_spreadsheet(tilenumber, band, Google_API_token, date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Do a 3D pipeline ingest on CANFAR")
    parser.add_argument("tilenumber", type=int, help="The tile number to ingest")
    parser.add_argument("band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile")

    args = parser.parse_args()
    tilenumber = args.tilenumber
    band = args.band

    do_ingest(tilenumber, band)