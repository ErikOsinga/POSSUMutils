import argparse
import pandas as pd
from vos import Client
import subprocess
import os

# URL of the status file in the VOSpace
status_file_url = "vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/pipeline_runs/pipeline_status.csv"
local_file_path = "./pipeline_status.csv"

def copy_status_file(client, remote_url, local_path):
    # Copy the file from VOSpace to the local file system
    client.copy(remote_url, local_path)
    print(f"Copied {remote_url} to {local_path}")

def read_status_file_to_dataframe(local_path):
    # Read the CSV file from the local file system into a pandas DataFrame
    df = pd.read_csv(local_path)
    return df

def merge_status_files(remote_df, local_df):
    # Merge the remote and local DataFrames based on the tilenumber and band
    merged_df = local_df.set_index(['#tilenumber', 'band']).combine_first(remote_df.set_index(['#tilenumber', 'band'])).reset_index()

    # Iterate over the rows to apply specific rules for merging
    for index, row in merged_df.iterrows():
        local_status = local_df[(local_df['#tilenumber'] == row['#tilenumber']) & (local_df['band'] == row['band'])]['status']
        remote_status = remote_df[(remote_df['#tilenumber'] == row['#tilenumber']) & (remote_df['band'] == row['band'])]['status']
        if not local_status.empty:
            local_status = local_status.values[0]
            if local_status == "Running" and not remote_status.empty:
                remote_status = remote_status.values[0]
                if remote_status != "ReadyToProcess":
                    merged_df.at[index, 'status'] = remote_status
                else:
                    merged_df.at[index, 'status'] = "Running"
            elif row['status'] == "ReadyToProcess":
                merged_df.at[index, 'status'] = local_status

    return merged_df

def update_status(df, local_path, tilenumber, band, new_status):
    # Update the status for the given tilenumber and band
    df.loc[(df['#tilenumber'] == tilenumber) & (df['band'] == band), 'status'] = new_status
    # Write the updated DataFrame back to the local CSV file
    df.to_csv(local_path, index=False)

def launch_pipeline(tilenumber, band):
    # Launch the appropriate 3D pipeline script based on the band
    if band == "943MHz":
        command = ["python", "launch_3Dpipeline_band1.py", str(tilenumber)]
    elif band == "1367MHz":
        command = ["python", "launch_3Dpipeline_band2.py", str(tilenumber)]
        print("Temporarily disabled launching band 2 because need to write that run script")
    else:
        raise ValueError(f"Unknown band: {band}")

    print(f"Running command: {' '.join(command)}")
    subprocess.run(command)

if __name__ == "__main__":
    client = Client()

    try:
        # Copy the remote status file to a temporary local file
        remote_temp_file = "./remote_pipeline_status.csv"
        copy_status_file(client, status_file_url, remote_temp_file)
        
        # Read the remote status file into a pandas DataFrame
        remote_df = read_status_file_to_dataframe(remote_temp_file)
        
        # Read the local status file into a pandas DataFrame if it exists
        if os.path.exists(local_file_path):
            local_df = read_status_file_to_dataframe(local_file_path)
        else:
            local_df = pd.DataFrame(columns=["#tilenumber", "status", "band"])

        # Merge the remote and local DataFrames
        merged_df = merge_status_files(remote_df, local_df)
        
        # Write the merged DataFrame back to the local CSV file
        merged_df.to_csv(local_file_path, index=False)
        
        # Print the DataFrame
        print(merged_df)
        
        # Find the first tilenumber with the status "ReadyToProcess"
        # if local status == Completed then even if remote is ReadyToProcess it wont go
        ready_to_process = merged_df[merged_df['status'] == 'ReadyToProcess']
        if not ready_to_process.empty:
            i = 0
            tilenumber = ready_to_process.iloc[i]['#tilenumber']
            band = ready_to_process.iloc[i]['band']
            
            # temporarily disable running band 2 for now
            while band != "943MHz":
                print(f"Skipping tilenumber {tilenumber} because band {band}")
                i += 1  # find next tile number
                if i >= len(ready_to_process):
                    print("No more tiles with status 'ReadyToProcess' found.")
                    break
                tilenumber = ready_to_process.iloc[i]['#tilenumber']
                band = ready_to_process.iloc[i]['band']

            if band == "943MHz":
                # Update the status to "Running"
                update_status(merged_df, local_file_path, tilenumber, band, "Running")
                
                # Launch the pipeline
                launch_pipeline(tilenumber, band)

        else:
            print("No tiles with status 'ReadyToProcess' found.")
            
    except Exception as err:
        print(f"An error occurred: {err}")

