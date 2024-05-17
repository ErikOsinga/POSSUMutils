import argparse
import glob
import os
import csv

"""
Usage: python log_processing_status.py tilenumber band

Log the processing status to 
/arc/projects/CIRADA/polarimetry/pipeline_runs/pipeline_status.csv

Either record
"Completed" - pipeline completed succesfully
"Failed"    - pipeline started but failed
"NotStarted"- pipeline not started for some reason

"""

def check_pipeline_complete(log_file_path):
    with open(log_file_path, 'r') as file:
        log_contents = file.read()
        
    if "Pipeline complete." in log_contents:
        return "Completed"
    else:
        return "Failed"

def update_status_csv(tilenumber, status, band, csv_file_path, all_tiles):
    # Check if the CSV file exists
    file_exists = os.path.isfile(csv_file_path)

    # Read existing data
    data = {}
    if file_exists:
        with open(csv_file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Skip header row
            for row in reader:
                if len(row) == 3:
                    data[f"{row[0]}_{row[2]}"] = (row[1], row[2])

    # Add all tile numbers with status "ReadyToProcess" and the correct band if not already present
    for tile, tile_band in all_tiles:
        key = f"{tile}_{tile_band}"
        if key not in data:
            data[key] = ("ReadyToProcess", tile_band)

    # Update the status for the given tilenumber and band
    key = f"{tilenumber}_{band}"
    data[key] = (status, band)

    # Write updated data to the CSV file
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header row
        writer.writerow(["#tilenumber", "status", "band"])
        # Write data rows
        for key, value in data.items():
            tilenumber, band = key.split("_")  # Extract the tilenumber and band from the key
            writer.writerow([tilenumber, value[0], value[1]])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check pipeline status and update CSV file")
    parser.add_argument("tilenumber", type=int, help="The tile number to check")
    parser.add_argument("band", choices=["943MHz", "1367MHz"], help="The frequency band of the tile")

    args = parser.parse_args()
    tilenumber = args.tilenumber
    band = args.band

    # Get all tile numbers from the directories
    base_tile_dir_943 = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/943MHz/"
    base_tile_dir_1367 = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/1367MHz/"

    all_tile_dirs_943 = [d for d in os.listdir(base_tile_dir_943) if os.path.isdir(os.path.join(base_tile_dir_943, d)) and d.isdigit()]
    all_tile_dirs_1367 = [d for d in os.listdir(base_tile_dir_1367) if os.path.isdir(os.path.join(base_tile_dir_1367, d)) and d.isdigit()]

    all_tiles = [(str(tile), "943MHz") for tile in all_tile_dirs_943] + [(str(tile), "1367MHz") for tile in all_tile_dirs_1367]

    # Find the log file for the given tilenumber
    log_files = glob.glob(f"/arc/projects/CIRADA/polarimetry/pipeline_runs/{band}/tile{tilenumber}/*log")

    if len(log_files) > 1:
        raise ValueError("Multiple log files found. Please remove failed run log files.")
    elif len(log_files) < 1:
        status = "NotStarted"
    else:
        log_file_path = log_files[0]
        status = check_pipeline_complete(log_file_path)

    # Update the status CSV file
    csv_file_path = "/arc/projects/CIRADA/polarimetry/pipeline_runs/pipeline_status.csv"
    update_status_csv(tilenumber, status, band, csv_file_path, all_tiles)

    print(f"Tilenumber {tilenumber} status: {status}, band: {band}")

