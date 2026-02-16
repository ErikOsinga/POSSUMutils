#!/usr/bin/env python3

import argparse
import time

# from skaha.session import Session
from canfar.sessions import Session

from print_all_open_sessions import get_open_sessions


def kill_headless_sessions(also_pending: bool, pause_seconds=1):
    """
    Find all running sessions labeled as 'headless' and destroy them.
    Optionally pauses `pause_seconds` between each destroy call.
    """
    session = Session()
    df_sessions = get_open_sessions()

    if df_sessions.empty:
        print("No open sessions found.")
        return

    if also_pending:
        # Filter for headless sessions that are still running
        mask = (df_sessions["type"].str.lower() == "headless") & (
            (df_sessions["status"].str.lower() == "running")
            | (df_sessions["status"].str.lower() == "pending")
        )
    else:
        # Filter for headless sessions that are still running
        mask = (df_sessions["type"].str.lower() == "headless") & (
            df_sessions["status"].str.lower() == "running"
        )

    headless_running = df_sessions[mask]

    if headless_running.empty:
        if also_pending:
            print("No running or pending 'headless' sessions to kill.")
        else:
            print("No running 'headless' sessions to kill.")
        return

    for idx, row in headless_running.iterrows():
        sess_id = row["id"]
        sess_name = row["name"]
        try:
            session.destroy(sess_id)
            print(f"Killed headless session: name='{sess_name}', id='{sess_id}'")
        except Exception as e:
            print(f"Failed to kill session id='{sess_id}': {e}")
        time.sleep(pause_seconds)


def main(also_pending: bool):
    """
    Entry point: kill all running 'headless' CANFAR sessions.
    """
    kill_headless_sessions(also_pending)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update Partial Tile Google Sheet")
    parser.add_argument(
        "--also-pending",
        action="store_true",
        help="Also kill 'headless' sessions that are pending.",
    )
    args = parser.parse_args()

    main(args.also_pending)
