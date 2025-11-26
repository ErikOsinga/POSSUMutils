import subprocess
import time
import pandas as pd
# from skaha.session import Session
from canfar.sessions import Session
from print_all_open_sessions import get_open_sessions


def run_script_intermittently(script_paths, interval, max_runs=None, max_pending=20, max_running=50):
    run_count = 0

    ### a chron job executes POSSUM_run_remote and create_symlinks.py every week on CANFAR.
    ### see p1: /home/erik/CIRADA/polarimetry/ASKAP/pipeline_runs/cronlogs/gocronjob.sh
    ### TODO: add update_CADC_tile_status.py to the cron job that runs every week
    ### such that downloaded and ingested tiles are updated in the spreadsheet. 

    while max_runs is None or run_count < max_runs:
        try:
            # Get information about currently open sessions
            df_sessions = get_open_sessions()
            print("Open sessions:")
            print(df_sessions)
            print("\n")

            # Count the number of headless sessions with status 'Pending'
            mask_pending = (df_sessions['type'] == 'headless') & (df_sessions['status'] == 'Pending')
            n_headless_pending = df_sessions[mask_pending].shape[0]
            print(f"Number of headless sessions with status 'Pending': {n_headless_pending}")
            # and only the ones that are related to the 3D pipeline
            mask_pending = mask_pending & (df_sessions['name'].str.contains('tile') | df_sessions['name'].str.contains('ingest'))
            n_headless_pending = df_sessions[mask_pending].shape[0]
            print(f"Number of *3D pipeline* headless sessions with status 'Pending': {n_headless_pending}")

            # Count the number of headless sessions with status 'Running'
            mask_running = (df_sessions['type'] == 'headless') & (df_sessions['status'] == 'Running')
            n_headless_running = df_sessions[mask_running].shape[0]
            print(f"Number of headless sessions with status 'Running': {n_headless_running}")
            # and only the ones that are related to the 3D pipeline
            mask_running = mask_running & (df_sessions['name'].str.contains('tile') | df_sessions['name'].str.contains('ingest'))
            n_headless_running = df_sessions[mask_running].shape[0]
            print(f"Number of *3D pipeline* headless sessions with status 'Running': {n_headless_running}")

            # If the number of pending headless sessions is less than e.g. 10, run the script
            if n_headless_pending < max_pending and n_headless_running < max_running:
                for script_path in script_paths:
                    print(f"Running script: {script_path}")
                    subprocess.run(["python", "-m", script_path], check=True)
            else:
                if n_headless_pending >= max_pending:
                    print("Too many pending headless sessions. Skipping this run.")
                if n_headless_running >= max_running:
                    print("Too many running headless sessions. Skipping this run.")

        except subprocess.CalledProcessError as e:
            print(f"Error occurred while running the script: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        
        run_count += 1
        if max_runs is not None and run_count >= max_runs:
            break
        
        print(f"Sleeping for {interval} seconds...")
        time.sleep(interval)

if __name__ == "__main__":
    # Path to the script to be run intermittently
    script_paths = ["possum_pipeline_control.check_status_and_launch_3Dpipeline_v2"
                    ,"possum_pipeline_control.check_ingest_3Dpipeline"]
    
    # Interval between each run in seconds
    interval = 600  # 10 minutes

    # Maximum number of runs for this script, set to None for infinite
    max_runs = None

    # Maximum number of headless jobs pendings, will not submit a session if theres more
    max_pending = 5 # 3d pipeline jobs are quite heavy, so 5 pending is enough

    # Maximum number of headless jobs running. will not submit if theres more
    max_running = 10 # only for 3D pipeline jobs

    run_script_intermittently(script_paths, interval, max_runs, max_pending, max_running)

