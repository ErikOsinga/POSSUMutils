"""
This job manually deletes state file 'state_file last_3dtile_download_launch_utc.txt' so create_symlinks can rerun.
"""
from pathlib import Path
from prefect import flow

@flow(name="delete_state_file", log_prints=True)
def main():
    state_file = Path.home() / ".possum" / "last_3dtile_download_launch_utc.txt"
    if state_file.exists():
        state_file.unlink()  # deletes the file
        print(f"Deleted state file: {state_file}")
    else:
        print(f"No state file found at: {state_file}")
