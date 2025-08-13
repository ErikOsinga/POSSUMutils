import subprocess
import time
import pandas as pd
from skaha.session import Session

session = Session()

def get_open_sessions():
    """Return a table with information about currently open sessions"""
    # Fetch open sessions
    open_sessions = session.fetch()

    if len(open_sessions) == 0:
        return pd.DataFrame()

    # Convert the list of dictionaries to a pandas DataFrame
    df_sessions = pd.DataFrame([{
        'type': s['type'],
        'status': s['status'],
        'startTime': s['startTime'],
        'name': s['name'],
        'id': s['id'],
    } for s in open_sessions])

    # sort by startTime
    df_sessions = df_sessions.sort_values(by='startTime', ascending=False)
    # Reset the index
    df_sessions = df_sessions.reset_index(drop=True)

    return df_sessions

def run_script_intermittently(script_paths, interval, max_runs=None, max_pending=20, max_running=50):
    """
    Execute all scripts in script_paths intermittently
    """
    run_count = 0

    while max_runs is None or run_count < max_runs:
        print("================================")
        try:
            # Get information about currently open sessions
            df_sessions = get_open_sessions()
            if len(df_sessions) == 0:
                print("No open sessions.")
                n_headless_pending = 0
                n_headless_running = 0 

            else:
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
        print("\n ============================== \n")
        time.sleep(interval)

if __name__ == "__main__":
    # Path to the script to be run intermittently
    script_paths = ["update_partialtile_google_sheet.py" # Check POSSUM Pipeline Status sheet and create queue of jobs in POSSUM Pipeline Validation sheet. 
                                                         # This is done via "check_status_and_launch_1Dpipeline_PartialTiles.py 'pre'"
                                                         # which also downloads the tiles in a CANFAR job.
                    ,"check_status_and_launch_1Dpipeline_PartialTiles.py" # Check POSSUM Pipeline Validation sheet and launch jobs
                    # ,"check_ingest_1Dpipeline_PartialTiles.py" # TODO: Check POSSUM Pipeline Validation sheet and ingest results
                                                            # actually, Craig will validate, and Cameron will ingest into YouCat
                    ]  
    
    # Interval between each run in seconds
    interval = 300  # 5 minutes
    #interval = 60 #1min
    #interval = 10*60 # 10 min
    # interval = 1*60*60 # 1 hours (download is slow atm)

    # Maximum number of runs for this script, set to None for infinite
    max_runs = None

    # Maximum number of headless jobs pendings, will not submit a session if theres more
    max_pending = 10

    # Maximum number of headless jobs running. will not submit if theres more
    max_running = 10

    # start 
    run_script_intermittently(script_paths, interval, max_runs, max_pending, max_running)

