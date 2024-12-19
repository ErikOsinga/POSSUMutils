import argparse
from skaha.session import Session

session = Session()

def launch_session(run_name, tilenumber, image, cores, ram):
    """Launch 3D pipeline run"""

    # Template bash script to run
    args = f"/arc/projects/CIRADA/polarimetry/software/run_3Dpipeline_band1_prefect.sh {run_name} {tilenumber}"

    print("Launching session")
    print(f"Command: bash {args}")

    session_id = session.create(
        name=run_name.replace('_', '-'),  # Prevent Error 400: name can only contain alpha-numeric chars and '-'
        image=image,
        cores=cores,
        ram=ram,
        kind="headless",
        cmd="bash",
        args=args,
        replicas=1,
        env={},
    )

    print("Check sessions at https://ws-uv.canfar.net/skaha/v0/session")
    print(f"Check logs at https://ws-uv.canfar.net/skaha/v0/session/{session_id[0]}?view=logs")

    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch a 3D pipeline run")
    parser.add_argument("tilenumber", type=int, help="The tile number to process")

    args = parser.parse_args()
    tilenumber = args.tilenumber
    run_name = f"tile{tilenumber}"  # Run name has to match the working directory on CANFAR

    # optionally :latest for always the latest version
    # image = "images.canfar.net/cirada/possumpipelineprefect-3.12:latest"
    image = "images.canfar.net/cirada/possumpipelineprefect-3.12:v1.11.0"
    # good default values
    cores = 16
    ram = 112  # Check allowed values at canfar.net/science-portal

    launch_session(run_name, tilenumber, image, cores, ram)
