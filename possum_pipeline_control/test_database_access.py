"""
A test module to check whether a user has set up access to the POSSUM AUSSRC database & Google Spreadsheet

This script:

- Checks whether it can access the POSSUM database (only reads)
- Checks whether it can access the POSSUM Status Sheet Google Sheet (only reads)
- Checks whether it can access files in pawsey data storage (only reads)

Should be executed on CANFAR, it will be send there by test_3dpipeline_job.py, so see that module instead

(The module test_3dpipeline_job.py should be executed on p1.)

@author: Erik Osinga
"""

import argparse
import os
import subprocess
from pathlib import Path

import astropy.units as u
import gspread
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import Table
from dotenv import load_dotenv
from prefect import flow, task

from automation import database_queries as db
from cirada_software.download_all_MFS_images import get_casda_username_password
from possum_pipeline_control import util

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
    or in the Prefect secrets
    """

    # read the location of the API token from the Prefect secret, and the prefect server access from ./automation/config.env
    Google_API_token = util.initiate_possum_status_sheet_and_token()

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


def check_access_to_pawsey():
    """
    Check whether user can see files in the pawsey file storage

    This is where the POSSUM data lives that's processed by AUSSRC
    """

    rclone_test_cmd = "rclone ls pawsey0980:possum --include tiles/*/11708/*"

    try:
        result = subprocess.run(rclone_test_cmd.split(" "), capture_output=True)

        assert result.returncode == 0, (
            f"Found returncode {result.returncode} for rclone command."
        )

    except Exception as e:
        print(
            "Failed to access pawsey. Did you configure rclone correctly (on CANFAR)?"
        )
        print("Failed with exception:")
        print(e)

    print("Rclone workings verified. ")
    print(f"Ran the command {rclone_test_cmd}")
    print("Found the following output:")
    print(result.stdout.decode())

    return


def check_access_to_casda():
    """
    Check whether user can see download files from CASDA

    This should be configured via ~/.ssh/casdapass on CANFAR
    which is a file that stores the CASDA username and password on separate lines
    """

    import astroquery
    from astroquery.casda import Casda

    print("Checking access to CASDA from CANFAR...")

    print(
        f"     astroquery version {astroquery.__version__}, {astroquery.__version__.split('.')}"
    )
    print(f"     astroquery file {astroquery.__file__}")

    if int(astroquery.__version__.split(".")[2]) < 7:
        # before 0.4.7, it required putting a password directly
        # this is the only way to interact with headless jobs

        print("Attempting to grab credentials from ~/.ssh/casdapass")
        username, pw = (
            get_casda_username_password()
        )  # get password from ~/.ssh/casdapass
        casda = Casda(username, pw)

        print("Trying a random query...")
        # try a random query
        sc = SkyCoord(103 * u.deg, -60 * u.deg)
        result = casda.query_region(sc, radius=30 * u.arcmin)

        if len(result) == 0:
            raise ValueError("Found empty result for CASDA query. Something is wrong")
        else:
            print(f"Found {len(result)} results for CASDA query.")

    else:
        msg = f"Found astroquery version {astroquery.__version__}. Has to be <0.4.7 to log in headlessly."
        print(msg)
        raise ValueError(msg)

    print("CASDA connection verified. ")

    return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Test access to AUSSRC Database and POSSUM Status Sheet")
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

    # Check access to pawsey
    check_access_to_pawsey_task = task(check_access_to_pawsey)
    check_access_to_pawsey_task()

    # Check access to CASDA
    check_acces_to_casda_task = task(check_access_to_casda)
    check_acces_to_casda_task()


if __name__ == "__main__":
    args = parse_args()

    if not args.run_as_flow:
        # load env for database credentials and google spreadsheet credential
        load_dotenv(dotenv_path="./automation/config.env")

        # Check access to production database
        check_acces_to_prod_db()

        # Check access to Cameron's sheet
        check_acces_to_google_spread()

        # Check access to pawsey for file download
        check_access_to_pawsey()

    else:
        mainflow()
