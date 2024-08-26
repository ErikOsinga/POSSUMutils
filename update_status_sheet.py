import argparse
import gspread
import astropy.table as at
import numpy as np

"""
Updates the POSSUM tile status (google sheet) to a specific value input by user

Should be executed on p1

@author: Erik Osinga
"""

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
        # as of >v6.0.0 .update requires a list of lists
        tile_sheet.update(range_name=f'{col_letter}{tile_index}', values=[[status]])
        print(f"Updated tile {tile_number} status to {status} in '3d_pipeline' column.")
    else:
        print(f"Tile {tile_number} not found in the sheet.")


if __name__ == "__main__":
    # on p1
    Google_API_token = "/home/erik/.ssh/psm_gspread_token.json"

    parser = argparse.ArgumentParser(description="Update status sheet 'manually'")
    parser.add_argument("tilenumber", type=int, help="The tile number to process")
    parser.add_argument("band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile")
    parser.add_argument("status", type='str', required=True)

    args = parser.parse_args()
    tilenumber = args.tilenumber
    band = args.band
    status = args.status

    if status == 'None' or status == 'none':
        status = ''

    update_status(tilenumber, band, Google_API_token, "Running")
