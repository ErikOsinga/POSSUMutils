import os
import argparse
import glob
import time
from possum_pipeline_control.util import TemporaryWorkingDirectory

"""
Python script to run after downloading tiles with "possum_run_remote"
Looks through the timeblocked directories in the ASKAP/Tiles/downloads/ folder to
create symbolic links in the Tiles/ directory for easy access per tile.
"""


def create_symlinks():
    # Meant to be run in the following directory on CANFAR
    assert os.getcwd() == "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles", (
        "Meant to be run in the current wd"
    )

    print("Creating symbolic links for the tile files from the timeblocked directories")

    # Define the base path
    base_path = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads/"

    # Find all time-blocked directories. This line will work until 2030.
    # Hopefully survey is done by then.
    timeblocks = glob.glob(os.path.join(base_path, "202*"))

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    # Log file path
    log_file_path = f"./symlink_logs/symbolic_links_log_{timestamp}.txt"

    # Create a dictionary to store tile numbers and their associated files
    tile_files = {}

    # Initialize logging lists
    created_links = []
    skipped_tiles = []

    # Iterate over each time-blocked directory to index which fits files we have
    for timeblock in timeblocks:
        # Find all FITS files in the survey subdirectories
        survey_files = glob.glob(os.path.join(timeblock, "survey/*/*fits"))

        # Index survey files
        for survey_file in survey_files:
            # Extract tile number from the file path
            parts = survey_file.split("/")
            tile_number = parts[-2]

            # Ensure the tile number directory exists in the current working directory
            if tile_number not in tile_files:
                tile_files[tile_number] = []

            # Add the survey file to the tile number directory
            tile_files[tile_number].append(survey_file)

    # Find all FITS files in the mfs subdirectories
    mfs_files = glob.glob(
        os.path.join(base_path, "202*/mfs/*/*fits")
    )  # This line will work until 2030. Hopefully survey is done by then.

    # Index MFS files
    for mfs_file in mfs_files:
        # Extract tile number from the file path
        parts = mfs_file.split("/")
        tile_number = parts[-2]

        # Add the MFS file to the tile number directory if it exists
        if tile_number in tile_files:
            tile_files[tile_number].append(mfs_file)

    # Create symbolic links for each tile number
    for tile_number, files in tile_files.items():
        # Check that there are 4 files for every tile in band 1 and 2: IQU + MFS
        for band in [943, 1367]:  # correct values
            bandfiles = []
            for file in files:
                if f"{band}MHz" in file:
                    bandfiles.append(file)
            if len(bandfiles) == 4:  # then we have IQU and MFS for this band
                # Create a directory for the tile number in the current working directory if it does not exist
                # To symbolic links for the files, either
                # ./943MHz/tilenumber/
                # ./1367MHz/tilenumber/
                tiledir = f"{band}MHz/{tile_number}"
                if not os.path.exists(tiledir):
                    os.makedirs(tiledir)

                for file in files:
                    link_name = os.path.join(tiledir, os.path.basename(file))
                    if os.path.islink(link_name) or os.path.exists(link_name):
                        print(
                            f"File or symbolic link {link_name} already exists. Skipping..."
                        )
                    else:
                        # Create a symbolic link
                        os.symlink(file, link_name)
                        created_links.append(link_name)

            elif (
                len(bandfiles) > 0
            ):  # if there are zero files it doesnt have to be logged
                # tile number, how many files, which band
                skipped_tiles.append((tile_number, len(bandfiles), band))

    # Log the results to a file
    with open(log_file_path, "w") as log_file:
        log_file.write("Symbolic Links Log\n")
        log_file.write("==================\n\n")
        log_file.write("Created Symbolic Links:\n")
        log_file.write("-----------------------\n")
        for link in created_links:
            log_file.write(f"{link}\n")

        log_file.write("\nSkipped Tiles:\n")
        log_file.write("----------------\n")
        for tile, count, band in skipped_tiles:
            log_file.write(
                f"Tile {tile} skipped, band: {band}MHz found {count} files instead of 4.\n"
            )

        # log summary
        log_file.write("\nSummary:")
        log_file.write(
            f"\nTotal symbolic links created: {len(created_links)} or {len(created_links) / 4} tiles."
        )
        if skipped_tiles:
            log_file.write(
                f"\nTiles skipped due to insufficient files (can be band 1 or 2): {len(skipped_tiles)}"
            )
        else:
            log_file.write("\nNo tiles were skipped due to insufficient files.")
        log_file.write("\n")

    # Print completion message
    print("Symbolic links created successfully.")

    # Print summary
    print("\nSummary:")
    print(
        f"Total symbolic links created: {len(created_links)}, or {len(created_links) / 4} tiles."
    )
    if skipped_tiles:
        print(f"Tiles skipped due to insufficient files: {len(skipped_tiles)}")
        for tile, count, band in skipped_tiles:
            print(
                f"Tile {tile} skipped, band: {band}MHz found {count} files instead of 4."
            )
    else:
        print("No tiles were skipped due to insufficient files.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create symlinks to the tiles in the 'Tiledir' "
    )
    parser.add_argument(
        "--tiledir",
        type=str,
        help="Where the symbolic links should be created",
        default="/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/",
    )
    args = parser.parse_args()

    assert os.path.exists(args.tiledir), f"Input {args.tiledir=} does not exist"

    with TemporaryWorkingDirectory(args.tiledir):
        create_symlinks()
