import argparse
import glob
import gspread
import numpy as np
import astropy.table as at
import ast

"""
Usage: python log_processing_status.py fieldstr sbid tilestr

where tilestr = always a list of 4 tiles e.g. ['8716','8891', '', '']

Log the processing status to Erik's POSSUM status monitor

https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k/edit?gid=1133887512#gid=1133887512

In the Google sheet, either record
    "Completed"            - 1D pipeline completed succesfully. Waiting for human validation
    "Failed"               - pipeline started but failed
    "NotStarted"           - pipeline not started for some reason
"""

def arg_as_list(s):
    v = ast.literal_eval(s)
    if type(v) is not list:
        raise argparse.ArgumentTypeError("Argument \"%s\" is not a list" % (s))
    return v

def check_pipeline_complete(log_file_path):
    with open(log_file_path, 'r') as file:
        log_contents = file.read()
        
    if "Pipeline complete." in log_contents:
        return "Completed"
    else:
        return "Failed"

def update_validation_spreadsheet(field_ID, tile_numbers, band, Google_API_token, status):
    """
    Update the status of the specified tile in the VALIDATION Google Sheet.
    
    Args:
    tile_number (str): The tile number to update.
    band (str): The band of the tile.
    Google_API_token (str): The path to the Google API token JSON file.
    status (str): The status to set in the '1d_pipeline' column.
    """
    print("Updating POSSUM pipeline validation sheet")

    t1, t2, t3, t4 = tile_numbers

    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    ps = gc.open_by_url('https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k')

    # Select the worksheet for the given band number
    band_number = '1' if band == '943MHz' else '2'
    tile_sheet = ps.worksheet(f'Partial Tile Pipeline - regions - Band {band_number}')
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    fieldname = "EMU_" if band == '943MHz' else 'WALLABY_' # TODO: verify WALLABY_ fieldname

    # Find the row index for the specified tile number
    tile_index = None
    for idx, row in enumerate(tile_table):
        if ( row['tile1'] == t1
            and row['tile2'] == t2
            and row['tile3'] == t3
            and row['tile4'] == t4
            and row['field_name'] == f"{fieldname}{field_ID}"
        ):
            tile_index = idx + 2  # +2 because gspread index is 1-based and we skip the header row
            break
    
    if tile_index is not None:
        # Update the status in the '1d_pipeline' column
        col_letter = gspread.utils.rowcol_to_a1(1, column_names.index('1d_pipeline') + 1)[0]
        # as of >v6.0.0 the .update function requires a list of lists
        tile_sheet.update(range_name=f'{col_letter}{tile_index}', values=[[status]])
        print(f"Updated row with tiles {tile_numbers} status to {status} in '1d_pipeline' column.")

        ## TODO: validation in case all tiles have been completed
        
        # # Find the validation file path
        # psm_val = glob.glob(f"/arc/projects/CIRADA/polarimetry/pipeline_runs/{band}/tile{tilenumber}/*validation.html")
        # if len(psm_val) == 1:
        #     psm_val = os.path.basename(psm_val[0])
        #     validation_link = f"https://ws-uv.canfar.net/arc/files/projects/CIRADA/polarimetry/pipeline_runs/{band}/tile{tilenumber}/{psm_val}"
        # elif len(psm_val) > 1:
        #     validation_link = "MultipleHTMLFiles"
        # else:
        #     validation_link = "HTMLFileNotFound"
        # # Update the '3d_val_link' column
        # col_link_letter = gspread.utils.rowcol_to_a1(1, column_names.index('3d_val_link') + 1)[0]
        # tile_sheet.update(range_name=f'{col_link_letter}{tile_index}', values=[[validation_link]])
        # print(f"Updated tile {tilenumber} validation link to {validation_link}")

    else:
        print(f"Field {fieldname}{field_ID} with tiles {tile_numbers} not found in the sheet.")

def tilenumbers_to_tilestr(tilenumbers):
    """
    Parse a list of 4 to a single tilestr
    
    e.g. ['8716','8891', '', '']
    
    becomes "8716+8891"
    """
    tilestr = ("+").join([ t for t in tilenumbers if t != ''])
    return tilestr

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check pipeline status and update CSV file")
    parser.add_argument(
        "field_ID",
        metavar="field",
        help="Field ID. e.g. 1412-28",
    )
    parser.add_argument(
        "SB_num",
        metavar="SB",
        type=int,
        help="SB number. e.g. 50413",
    )
    parser.add_argument(
        "tilenumbers",
        type=arg_as_list,
        help="A list of 4 tile numbers to process. Empty strings for less tilenumbers. e.g. ['8843','8971','',''] "
    )

    parser.add_argument("band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile")

    args = parser.parse_args()
    field_ID = args.field_ID
    SB_num = args.SB_num
    tilenumbers = args.tilenumbers
    tilestr = tilenumbers_to_tilestr(tilenumbers)
    band = args.band

    # Where to find pipeline outputs
    basedir = f"/arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/{band}"
    basedir = f"{basedir}/{field_ID}/{SB_num}/{tilestr}"
    # e.g. /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/1412-28/50413/8715/

    # Find the POSSUM pipeline log file for the given tilenumber
    log_files = sorted(glob.glob(f"{basedir}/*pipeline_config*_{tilestr}.log"))

    if len(log_files) > 1:
        log_file_path = log_files[-1]

        print("WARNING: Multiple log files found. Please remove failed run log files.")
        print("Taking the last log file")

        # Write the warning to file
        with open(f"{basedir}/log_processing_status.log", "a") as log_file:
            log_file.write("WARNING: Multiple log files found. Taking the last log file.\n")
            log_file.write(f"{log_file_path} \n")

        status = check_pipeline_complete(log_file_path)

    elif len(log_files) < 1:
        status = "NotStarted"
    else:
        log_file_path = log_files[0]
        status = check_pipeline_complete(log_file_path)

    print(f"Tilenumbers {tilestr} status: {status}, band: {band}")

    # Update the POSSUM Validation spreadsheet
    Google_API_token = "/arc/home/ErikOsinga/.ssh/neural-networks--1524580309831-c5c723e2468e.json"
    update_validation_spreadsheet(field_ID, tilenumbers, band, Google_API_token, status)