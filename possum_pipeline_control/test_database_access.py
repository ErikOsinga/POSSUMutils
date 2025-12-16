"""
A test module to check whether a user has set up access to the POSSUM AUSSRC database & Google Spreadsheet

This script:

- Checks whether it can access the POSSUM database (only reads)
- Checks whether it can access the POSSUM Status Sheet Google Sheet (only reads)

Should be executed on p1 and CANFAR. 

But this script is called by test_3dpipeline_job.py, so see that module instead.

@author: Erik Osinga
"""

import os
import argparse
from astropy.table import Table
import numpy as np
from dotenv import load_dotenv
import gspread
from pathlib import Path
from automation import database_queries as db
from prefect import flow, task

# expected number of tiles in POSSUM band 1
NTILES_BAND1_AUSSRC = 6497  # total expected number of tiles for Band 1
NTILES_BAND1_GSPREAD = 6390  # have to check with Cameron why he has less tiles


def check_acces_to_prod_db():
    """
    Check whether the user has access to the production database
    by querying how many tiles there are in the possum.tile_state_band1 table

    This should be the total number of band 1 tiles: 6497

    If this doesn't work, there's likely an error in the file ./automation/config.env
    """
    #
    conn = db.get_database_connection(test=False)
    all_tiles = db.get_3d_tile_data(tile_id=None, band_number=1, conn=conn)
    assert len(all_tiles) == NTILES_BAND1_AUSSRC, (
        f"Found {len(all_tiles)} instead of the expected {NTILES_BAND1_AUSSRC} tiles in band 1."
    )

    print("Access to AUSSRC production database verified.")

    return


def check_acces_to_google_spread():
    """
    Check whether user has access to Cameron's Status Spreadsheet.

    If this doesn't work, there's likely an error in the file ./automation/config.env
    """

    # read the location of the API token according to ./automation/config.env
    # this will be  e.g. ~/.ssh/psm_gspread_token.json
    Google_API_token = os.getenv("POSSUM_STATUS_TOKEN")

    # it should exist
    assert Path(Google_API_token).exists(), f"{Google_API_token=} not found!"

    # then authenticate and grab the POSSUM Status Google Sheet
    gc = gspread.service_account(filename=Google_API_token)
    ps = gc.open_by_url(os.getenv("POSSUM_STATUS_SHEET"))

    # Select the worksheet tiles in band 1. There should be 6497 tiles
    band_number = 1
    tile_sheet = ps.worksheet(f"Survey Tiles - Band {band_number}")
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = Table(np.array(tile_data)[1:], names=column_names)

    assert len(tile_table) == NTILES_BAND1_GSPREAD, (
        f"Found {len(tile_table)} instead of the expected {NTILES_BAND1_GSPREAD} tiles in band 1."
    )

    print("Access to Cameron's POSSUM Status Sheet verified.")

    return

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test access to AUSSRC Database and POSSUM Status Sheet"
        )
    )
    parser.add_argument(
        "--run_as_flow",
        action="store_true",
        help="Run as a prefect flow?",
    )
    return parser.parse_args()

@flow(name="test_db_access", log_prints=True)
def mainflow():

    # load env for database credentials and google spreadsheet credential
    load_dotenv_task = task(load_dotenv)
    load_dotenv_task(dotenv_path="./automation/config.env")

    # Check access to production database
    check_acces_to_prod_db_task = task(check_acces_to_prod_db)
    check_acces_to_prod_db_task()

    # Check access to Cameron's sheet
    check_acces_to_google_spread_task = task(check_acces_to_google_spread)
    check_acces_to_google_spread_task()


if __name__ == "__main__":

    args = parse_args()

    if not args.run_as_flow:
        # load env for database credentials and google spreadsheet credential
        load_dotenv(dotenv_path="./automation/config.env")

        # Check access to production database
        check_acces_to_prod_db()

        # Check access to Cameron's sheet
        check_acces_to_google_spread()

    else:

        mainflow()
