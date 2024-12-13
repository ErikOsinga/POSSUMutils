import argparse
import ast
from datetime import datetime
from skaha.session import Session
# from skaha.models import ContainerRegistry

# Shouldnt put these on github...
# see https://shinybrar.github.io/skaha/
# registry = ContainerRegistry(username="ErikOsinga", secret="CLI")

# session = Session(registry=registry)
session = Session()

def arg_as_list(s):
    v = ast.literal_eval(s)
    if type(v) is not list:
        raise argparse.ArgumentTypeError("Argument \"%s\" is not a list" % (s))
    return v

def launch_session(run_name, field_ID, tilenumbers, SBnumber, image, cores, ram):
    """Launch 1D pipeline Partial Tile run"""
    t1, t2, t3, t4 = tilenumbers

    # Template bash script to run
    args = f"/arc/projects/CIRADA/polarimetry/software/POSSUMutils/cirada_software/run_1Dpipeline_PartialTiles_band1.sh {run_name} {field_ID} {SBnumber} {t1} {t2} {t3} {t4}"

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
    parser.add_argument("field_ID", type=str, help="The field ID to process, e.g. 1412-28")
    parser.add_argument("tilenumbers", type=arg_as_list, help="A list of 4 tile numbers to process. Empty strings for less tilenumbers. e.g. ['8843','8971','',''] ")
    parser.add_argument("SBnumber", type=int, help="The SB number to process")

    args = parser.parse_args()
    field_ID = args.field_ID
    tilenumbers = args.tilenumbers
    SBnumber = args.SBnumber
    
    timestr = ((datetime.now().strftime("%d/%m/%Y %H:%M:%S"))[11:]).replace(':','-') # ":" is not allowed character
    # max 15 characters for run name. SBID+timenow: e.g. 50413-11-39-21
    run_name = f"{SBnumber}-{timestr}"

    # optionally :latest for always the latest version. CANFAR has a bug with that though.
    # image = "images.canfar.net/cirada/possumpipelineprefect-3.12:latest"
    image = "images.canfar.net/cirada/possumpipelineprefect-3.12:v1.11.0" # v1.12.1 has astropy issue https://github.com/astropy/astropy/issues/17497
    # good default values
    cores = 4
    # Allocate different RAM based on how many tiles, lets say ~ 10 GB per tile needed
    number_of_tiles = len([t for t in tilenumbers if t != ''])
    ram = 10*number_of_tiles
    # Check allowed values at canfar.net/science-portal, 10, 20, 30, 40 GB should be allowed

    launch_session(run_name, field_ID, tilenumbers, SBnumber, image, cores, ram)
