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
from possum2caom2.composable import run as possum_run

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
    possum_run()
    # # Start possum_run in correct workdir
    # process = subprocess.Popen("possum_run",cwd=tile_workdir)

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Do a 3D pipeline ingest on CANFAR")
    parser.add_argument("tilenumber", type=int, help="The tile number to ingest")
    parser.add_argument("band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile")

    args = parser.parse_args()
    tilenumber = args.tilenumber
    band = args.band

    do_ingest(tilenumber, band)