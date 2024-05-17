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

def run_script_intermittently(script_path, interval, max_runs=None):
    run_count = 0
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
            if n_headless_pending < max_pending:
                print(f"Running script: {script_path}")
                subprocess.run(["python", script_path], check=True)
            else:
                print("Too many pending headless sessions. Skipping this run.")

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
    script_path = "check_status_and_launch_3Dpipeline.py"
    
    # Interval between each run in seconds
    interval = 600  # 10 minutes

    # Maximum number of runs for this script, set to None for infinite
    max_runs = None

    # Maximum number of headless jobs pendings, will not submit a session if theres more
    max_pending = 10

    run_script_intermittently(script_path, interval, max_runs)

