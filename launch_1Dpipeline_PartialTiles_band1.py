import argparse
from skaha.session import Session

session = Session()

def launch_session(run_name, field_ID, tilenumber, image, cores, ram):
    """Launch 1D pipeline Partial Tile run"""

    # Template bash script to run
    args = f"/arc/projects/CIRADA/polarimetry/software/run_1Dpipeline_PartialTiles_band1.sh {run_name} {field_ID} {tilenumber}"

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
    )

    print("Check sessions at https://ws-uv.canfar.net/skaha/v0/session")
    print(f"Check logs at https://ws-uv.canfar.net/skaha/v0/session/{session_id[0]}?view=logs")

    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch a 1D pipeline Partial Tiles run")
    parser.add_argument("field_ID", type=str, help="The field ID to process")
    parser.add_argument("tilenumber", type=int, help="The tile number to process")

    args = parser.parse_args()
    field_ID = args.field_ID
    tilenumber = args.tilenumber
    # max 15 characters for run name
    run_name = f"{field_ID}_{tilenumber}"

    # optionally :latest for always the latest version
    image = "images.canfar.net/cirada/possumpipelineprefect-3.12:latest"
    # good default values
    cores = 16
    ram = 40  # Check allowed values at canfar.net/science-portal

    launch_session(run_name, tilenumber, image, cores, ram)
