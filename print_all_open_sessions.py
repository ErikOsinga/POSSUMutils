"""Convenience script to print all open sessions on CANFAR"""

import pandas as pd
from canfar.sessions import Session

from possum_pipeline_control import util


def get_open_sessions():
    """Return a table with information about currently open sessions"""
    util.stage_cadc_certificate('')
    session = Session()

    # Fetch open sessions
    open_sessions = session.fetch()

    if len(open_sessions) == 0:
        return pd.DataFrame()

    # Convert the list of dictionaries to a pandas DataFrame
    df_sessions = pd.DataFrame(
        [
            {
                "type": s["type"],
                "status": s["status"],
                "startTime": s["startTime"] if "startTime" in s else "Pending",
                "name": s["name"],
                "id": s["id"],
            }
            for s in open_sessions
        ]
    )

    # sort by startTime
    df_sessions = df_sessions.sort_values(by="startTime", ascending=False)
    # Reset the index
    df_sessions = df_sessions.reset_index(drop=True)

    return df_sessions


if __name__ == "__main__":
    df_sessions = get_open_sessions()
    if len(df_sessions) == 0:
        print("No open sessions.")
    else:
        print("Open sessions:")
        print(df_sessions)
