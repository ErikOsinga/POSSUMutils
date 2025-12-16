"""
A test module to check whether a user has set up everything around the POSSUM pipeline orchestration

This script:

- Checks whether it can access the POSSUM database (only reads)
- Checks whether it can access the POSSUM Status Sheet Google Sheet (only reads)
- launches a small test 3D pipeline headless job to CANFAR
- Has it report back to p1's prefect server

Should be executed on p1


Assumes that it's called from the POSSUMutils root dir as

python -m possum_pipeline_control.test3dpipeline_job


@author: Erik Osinga
"""

from dotenv import load_dotenv
from canfar.sessions import Session
from possum_pipeline_control.test_database_access import (
    NTILES_BAND1_GSPREAD,
    NTILES_BAND1_AUSSRC,
    check_acces_to_google_spread,
    check_acces_to_prod_db,
)

session = Session()


def launch_test_session(jobname="testjob"):
    # Template bash script to run
    args = "/arc/projects/CIRADA/polarimetry/software/POSSUMutils/cirada_software/test_3dpipeline_job.sh"

    print("Launching download session")
    print(f"Command: bash {args}")

    image = "images.canfar.net/cirada/possumpipelineprefect-3.12:v1.15.2"
    # could also use flexible resources ?
    session_id = session.create(
        name=jobname.replace(
            "_", "-"
        ),  # Prevent Error 400: name can only contain alpha-numeric chars and '-'
        image=image,
        cores=4,  # set to None for flexible mode
        ram=10,  # set to None flexible mode
        kind="headless",
        cmd="bash",
        args=args,
        replicas=1,
        env={},
    )

    print("Check sessions at https://ws-uv.canfar.net/skaha/v1/session")
    print(
        f"Check logs at https://ws-uv.canfar.net/skaha/v1/session/{session_id[0]}?view=logs"
    )

    return


if __name__ == "__main__":
    # load env for database credentials and google spreadsheet credential
    load_dotenv(dotenv_path="./automation/config.env")

    # Check access to production database from p1
    check_acces_to_prod_db()

    # Check access to Cameron's sheet from p1
    check_acces_to_google_spread()

    # Launch 3d pipeline test job on CANFAR
    launch_test_session()
