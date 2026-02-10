import argparse
import subprocess
from print_all_open_sessions import get_open_sessions
from prefect import flow
from possum_pipeline_control import util

def run_script_intermittently(
    script_paths, max_pending=20, max_running=50, args=None
):
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
            n_headless_pending = df_sessions[
                (df_sessions["type"] == "headless")
                & (df_sessions["status"] == "Pending")
            ].shape[0]
            print(
                f"Number of headless sessions with status 'Pending': {n_headless_pending}"
            )

            # Count the number of headless sessions with status 'Running'
            n_headless_running = df_sessions[
                (df_sessions["type"] == "headless")
                & (df_sessions["status"] == "Running")
            ].shape[0]
            print(
                f"Number of headless sessions with status 'Running': {n_headless_running}"
            )

        # If the number of pending headless sessions is less than e.g. 10, run the script
        if n_headless_pending < max_pending and n_headless_running < max_running:
            for script_path in script_paths:
                print(f"Running script: {script_path}")
                cmd_list = ["python", "-m", script_path]
                # Call the main flow
                if args.database_config_path is not None:
                    cmd_list += [
                            "--database_config_path",
                            args.database_config_path,
                        ]
                process = subprocess.Popen(
                    cmd_list,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,               # line-buffered
                    universal_newlines=True  # ensures \n splitting works across platforms
                )
                util.print_subprocess_output(process, cmd_list)
        else:
            if n_headless_pending >= max_pending:
                print("Too many pending headless sessions. Skipping this run.")
            if n_headless_running >= max_running:
                print("Too many running headless sessions. Skipping this run.")

    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running the script: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

@flow(name="control_1D_pipeline_PartialTiles", log_prints=True)
def main_flow():
    parser = argparse.ArgumentParser(description="Update Partial Tile Google Sheet")
    parser.add_argument(
        "--database_config_path",
        type=str,
        help="Path to .env file with database connection parameters.",
    )
    args = parser.parse_args()

    # Load POSSUM status token from environment variables if database_config_path is specified, otherwise load Prefect secrets
    util.initiate_possum_status_sheet_and_token(args.database_config_path)
    # Path to the script to be run intermittently
    script_paths = [
        "possum_pipeline_control.update_partialtile_google_sheet",  # Check POSSUM Pipeline Status sheet and create queue of jobs in POSSUM Pipeline Validation sheet.
        # This is done via "check_status_and_launch_1Dpipeline_PartialTiles.py 'pre'"
        # which also downloads the tiles in a CANFAR job.
        "possum_pipeline_control.check_status_and_launch_1Dpipeline_PartialTiles",  # Check POSSUM Pipeline Validation sheet and launch jobs
        # ,"check_ingest_1Dpipeline_PartialTiles.py" # TODO: Check POSSUM Pipeline Validation sheet and ingest results
        # actually, Craig will validate, and Cameron will ingest into YouCat
    ]

    # Maximum number of headless jobs pendings, will not submit a session if theres more
    max_pending = 15

    # Maximum number of headless jobs running. will not submit if theres more
    max_running = (
        15  # probably good to set low to limit parallel downloads of separate tiles
    )

    # start
    run_script_intermittently(
        script_paths, max_pending, max_running, args
    )

if __name__ == "__main__":
    main_flow()
    
