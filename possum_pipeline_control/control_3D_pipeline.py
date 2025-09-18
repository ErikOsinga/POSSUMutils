import subprocess
import time
import pandas as pd
from skaha.session import Session

session = Session()

def get_open_sessions():
    """Return a table with information about currently open sessions"""
    # Fetch open sessions
    open_sessions = session.fetch()

    # Convert the list of dictionaries to a pandas DataFrame
    df_sessions = pd.DataFrame([{
        'type': s['type'],
        'status': s['status'],
        'startTime': s['startTime']
    } for s in open_sessions])

    return df_sessions

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

            # Count the number of headless sessions with status 'Pending'
            n_headless_pending = df_sessions[(df_sessions['type'] == 'headless') & (df_sessions['status'] == 'Pending')].shape[0]
            print(f"Number of headless sessions with status 'Pending': {n_headless_pending}")

            # Count the number of headless sessions with status 'Running'
            n_headless_running = df_sessions[(df_sessions['type'] == 'headless') & (df_sessions['status'] == 'Running')].shape[0]
            print(f"Number of headless sessions with status 'Running': {n_headless_running}")

            # If the number of pending headless sessions is less than e.g. 10, run the script
            if n_headless_pending < max_pending and n_headless_running < max_running:
                for script_path in script_paths:
                    print(f"Running script: {script_path}")
                    subprocess.run(["python", script_path], check=True)
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
    script_paths = ["check_status_and_launch_3Dpipeline_v2.py"
                    ,"check_ingest_3Dpipeline.py"]
    
    # Interval between each run in seconds
    interval = 600  # 10 minutes

    # Maximum number of runs for this script, set to None for infinite
    max_runs = None

    # Maximum number of headless jobs pendings, will not submit a session if theres more
    max_pending = 5 # 3d pipeline jobs are quite heavy, so 5 pending is enough

    # Maximum number of headless jobs running. will not submit if theres more
    max_running = 10

    run_script_intermittently(script_paths, interval, max_runs, max_pending, max_running)

