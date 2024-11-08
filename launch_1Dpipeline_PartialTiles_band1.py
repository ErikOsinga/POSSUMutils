import argparse
from skaha.session import Session
from skaha.models import ContainerRegistry

# Shouldnt put these on github...
# see https://shinybrar.github.io/skaha/
# registry = ContainerRegistry(username="ErikOsinga", secret="CLI")

# session = Session(registry=registry)
session = Session()

def launch_session(run_name, field_ID, tilenumber, SBnumber, image, cores, ram):
    """Launch 1D pipeline Partial Tile run"""

    # Template bash script to run
    args = f"/arc/projects/CIRADA/polarimetry/software/run_1Dpipeline_PartialTiles_band1.sh {run_name} {field_ID} {tilenumber} {SBnumber}"

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
    parser = argparse.ArgumentParser(description="Launch a 1D pipeline Partial Tiles run")
    parser.add_argument("field_ID", type=str, help="The field ID to process")
    parser.add_argument("tilenumber", type=int, help="The tile number to process")
    parser.add_argument("SBnumber", type=int, help="The SB number to process")

    args = parser.parse_args()
    field_ID = args.field_ID
    tilenumber = args.tilenumber
    SBnumber = args.SBnumber
    
    # max 15 characters for run name
    run_name = f"{field_ID}_{tilenumber}"

    # optionally :latest for always the latest version
    # image = "images.canfar.net/cirada/possumpipelineprefect-3.12:latest"
    image = "images.canfar.net/cirada/possumpipelineprefect-3.12:v1.11.0"
    # good default values
    cores = 4
    ram = 4  # Check allowed values at canfar.net/science-portal

    launch_session(run_name, field_ID, tilenumber, SBnumber, image, cores, ram)
