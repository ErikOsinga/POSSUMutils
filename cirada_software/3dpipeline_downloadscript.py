"""
To be put on CANFAR /software/

Calls Sharon Goliath's download/ingest script ("possum_run_remote") to download
tiles from Australia to Canada (pawsey to CANFAR)


WORKFLOW:

1. Download files off pawsey and ingest them into CADC. This downloads the IQU cubes and MFS images
        possum_run_remote

        possum_run_remote checks the CASDA archive (pawsey) for new observations and downloads them into the timeblocked directories

2. Go to the parent directory: /arc/projects/CIRADA/polarimetry/ASKAP/Tiles and create symbolic links from the timeblocked directories
        python create_symlinks.py

3. Processing will be done by run_3D_pipeline_intermittently.py
"""

import os
import sys
from dotenv import load_dotenv
from prefect import flow, task

# important to grab _run_remote() because run_remote() is wrapped in sys.exit()
from possum2caom2.composable import _run_remote as possum_run_remote  # type: ignore


@task
def launch_possum_run_remote():
    """change workdir and launch download script

    return number of succesfully downloaded+ingested tiles
    """

    download_workdir = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads/"
    # Start possum_run in correct workdir
    os.chdir(download_workdir)

    result = possum_run_remote()  # noqa: F841

    # Print the results to view them in Prefect
    report_file = f"{download_workdir}/download_logs/downloads_report.txt"

    if not os.path.exists(report_file):
        print(f"Report file {report_file} does not exist.")
        return 0

    with open(report_file, "r") as file:
        for line in file:
            print(line)
            if "Number of Successes" in line:
                successes = int(line.split(":")[1].strip())

    return successes


@task
def launch_make_symlinks():
    """change workdir and launch symbolic link script"""

    tile_workdir = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/"
    os.chdir(tile_workdir)

    # should be found in the tile_workdir
    sys.path.append(tile_workdir)
    from create_symlinks import create_symlinks

    create_symlinks()


@flow(log_prints=True)
def do_download():
    """Does the download and ingest script for the MFS images and cubes

    1. Execute "possum_run_remote" in the correct directory
    2. Execute "create_symlinks" in the correct directory

    """

    successes = launch_possum_run_remote()

    if successes > 0:
        launch_make_symlinks()
    else:
        print("No sucessful tile downloads. Not running symlinks")


if __name__ == "__main__":
    load_dotenv(dotenv_path="./automation/config.env")
    do_download()
