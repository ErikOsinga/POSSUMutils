"""
This script is DEPRECATED. Superseded by launch_download_session() in the module check_status_and_launch_3Dpipeline_v2.py

        It used to be called periodically from p1 with cron

        see

        crontab -e

        It submits a download job to CANFAR which will pull new files off' the pawsey storage.

"""
import os
from datetime import date

# from skaha.session import Session  # noqa: E402
from canfar.sessions import Session
from automation import canfar_wrapper

session = Session()

today = date.today()
print("Download script called. Today's date:", today)


def launch_download():
    """Launch tile download script"""

    print("Launching tile download script on CANFAR")
    run_name = "download"

    # optionally :latest for always the latest version
    # TODO: there's a bug in CANFAR where the latest tag doesnt work
    image = os.getenv("IMAGE")
    # good default values for download script
    cores = 2
    ram = 16  # Check allowed values at canfar.net/science-portal

    # Template bash/python script to run
    args = "/arc/projects/CIRADA/polarimetry/software/POSSUMutils/cirada_software/3dpipeline_downloadscript.py"

    print("Launching session")
    print(f"Command: {args}")
    print(f"Image:{image}")

    session_id = session.create(
        name=run_name.replace(
            "_", "-"
        ),  # Prevent Error 400: name can only contain alpha-numeric chars and '-'
        image=image,
        cores=cores,
        ram=ram,
        kind="headless",
        cmd="python",
        args=args,
        replicas=1,
        env={
            "PREFECT_API_URL": os.getenv('PREFECT_SERVER_IP'),
            "PREFECT_API_AUTH_STRING": os.getenv('PREFECT_API_AUTH_STRING')
        },
    )

    print("Check sessions at https://ws-uv.canfar.net/skaha/v1/session")
    print(
        f"Check logs at https://ws-uv.canfar.net/skaha/v1/session/{session_id[0]}?view=logs"
    )

    return session_id[0]


if __name__ == "__main__":
    canfar_wrapper.run_canfar_task_with_polling.with_options(name="poll_download")(launch_download)
