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
After downloading the tiles in a CANFAR job and populating the POSSUM Pipeline Validation sheet, log the processing status
to the main POSSUM Status Monitor sheet.
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check pipeline status and update POSSUM Status sheet")
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
    parser.add_argument("band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile")

    args = parser.parse_args()
    field_ID = args.field_ID # without EMU_
    SBID = args.SB_num
    band = args.band
        
    # update the status in Cameron's spreadsheet
    status_to_put = "PartialTiles - Running"
    Google_API_token = "/arc/home/ErikOsinga/.ssh/psm_gspread_token.json"

    print(f"Updating status to {status_to_put} for field {field_ID} SBID {SBID} band {band} in column 'single_SB_1D_pipeline'")

    update_status_spreadsheet(field_ID, SBID, band, Google_API_token, status_to_put, 'single_SB_1D_pipeline')
