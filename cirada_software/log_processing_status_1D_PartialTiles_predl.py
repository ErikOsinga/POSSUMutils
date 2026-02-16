#!/usr/bin/env python
import argparse
import os

from dotenv import load_dotenv

from possum_pipeline_control import util

from .log_processing_status_1D_PartialTiles_summary import update_status_spreadsheet

"""
While downloading the tiles in a CANFAR job and populating the POSSUM Pipeline Validation sheet, log the processing status
to the main POSSUM Status Monitor sheet.
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check pipeline status and update POSSUM Status sheet"
    )
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
        "band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile"
    )
    parser.add_argument(
        "--database_config_path",
        type=str,
        default="automation/config.env",
        help="Path to .env file with database connection parameters.",
    )
    # consider chmod 600 <file> to prevent access

    args = parser.parse_args()
    field_ID = args.field_ID  # without EMU_
    SBID = args.SB_num
    band = args.band
    database_config_path = args.database_config_path

    load_dotenv(dotenv_path=database_config_path)
    Google_API_token = util.initiate_possum_status_sheet_and_token()
    if not os.path.isfile(Google_API_token):
        raise FileNotFoundError(
            f"Google API token file not found at {Google_API_token}"
        )

    # update the status in Cameron's spreadsheet
    status_to_put = "PartialTiles - Running"

    print(
        f"Updating status to {status_to_put} for field {field_ID} SBID {SBID} band {band} in column 'single_SB_1D_pipeline'"
    )

    update_status_spreadsheet(
        field_ID,
        SBID,
        band,
        Google_API_token,
        status_to_put,
        "single_SB_1D_pipeline",
        database_config_path=database_config_path,
    )
