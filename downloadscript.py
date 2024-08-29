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
from prefect import flow, task
# important to grab _run_remote() because run_remote() is wrapped in sys.exit()
from possum2caom2.composable import _run_remote as possum_run_remote # type: ignore

@task
def launch_possum_run_remote():
    """change workdir and launch download script"""

    download_workdir = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads/"
    # Start possum_run in correct workdir
    os.chdir(download_workdir)
    
    result = possum_run_remote()

    return result

@task
def launch_make_symlinks():
    """change workdir and launch symbolic link script"""

    tile_workdir = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/"
    os.chdir(tile_workdir)
    # should be found in the tile_workdir
    from create_symlinks import create_symlinks
    create_symlinks()

@flow(log_prints=True)
def do_download():
    """Does the download and ingest script for the MFS images and cubes
    
    1. Execute "possum_run_remote" in the correct directory
    2. Execute "create_symlinks" in the correct directory

    """

    launch_possum_run_remote()

    launch_make_symlinks()


if __name__ == "__main__":
    do_download()