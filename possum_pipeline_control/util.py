from __future__ import annotations
from pathlib import Path
import os
import errno
import json
import shutil
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pathlib import Path
from prefect import task
from prefect.blocks.system import Secret

"""
Utility methods shared across the scripts
"""


update_cadc_proxy_msg = """Please run cadc-get-cert -u <UserName> --days-valid 30 in a CANFAR interactive session 
                and update Prefect secret 'cadc-proxy-pem' with the content to update your CADC certificate!!!"""

def get_band_number(band: str) -> str:
    """
    Convert band string to band number.
    """
    return "1" if band == "943MHz" else "2"


def get_full_field_name(field_ID: str, band: str) -> str:
    """
    Build field name prefix

    args:
        field_ID: e.g. 1227-69
        band: '943MHz' or '1367MHz'

    returns:
        full field name: e.g. EMU_1227-69 for band 1, WALLABY_1227-69 for band 2
    """
    fieldname = (
        "EMU_" if band == "943MHz" else "WALLABY_"
    )  # TODO: verify WALLABY_ fieldname
    return f"{fieldname}{field_ID}"


def get_sbid_num(sbid: str | None) -> str | None:
    """
    Remove ASKAP- prefix from sbid if present
    """
    if sbid is None:
        return None

    if sbid.startswith("ASKAP-"):
        return sbid.replace("ASKAP-", "")

    return sbid

@task(name="Stage CADC certificate")
def stage_cadc_certificate(
    workdir: str,
    source_cert: str = "~/.ssl/cadcproxy.pem",
    dest_relpath: str = ".ssl/cadcproxy.pem",
    max_age_days: int = 30
) -> str:
    src = Path(os.path.expanduser(source_cert))
    if not src.exists():
        # Try the secret from prefect
        secret_block = Secret.load("cadc-proxy-pem")
        pem_str = secret_block.get()
        if pem_str:
            write_cadcproxy_pem(pem_str)
        else:
            raise FileNotFoundError(
                f"Required CADC certificate not found: {src}. {update_cadc_proxy_msg}"                
            )
    else:
        # Check age, certificates are max valid for 30 days
        mtime = datetime.fromtimestamp(src.stat().st_mtime, tz=timezone.utc)
        if datetime.now(timezone.utc) - mtime > timedelta(days=max_age_days):
            # Delete expired certificate so we auto load a new one next time
            print('Deleting stale cadcproxy.pem file..')
            os.remove(src)
            # Prompt user to rewrite a new secret
            raise ValueError(
                f"CADC certificate has expired and been deleted! {update_cadc_proxy_msg}"
            )

    # Copy to workdir/.ssl/cadcproxy.pem (create directory if needed)
    if workdir:
        dest = Path(workdir) / dest_relpath
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        return str(dest)
    
    return dest_relpath

def write_cadcproxy_pem(content):
    """
    Write a cadcproxy.pem file in .ssl directory 
    using Prefect secret 'cadc-proxy-pem'.
    """
    # Construct the full path using the user's home directory
    home_dir = os.path.expanduser("~")
    ssl_dir = os.path.join(home_dir, ".ssl")
    file_path = os.path.join(ssl_dir, "cadcproxy.pem")

    write_to_file(ssl_dir, file_path, 'cadc-proxy-pem')

def initiate_possum_status_sheet_and_token(database_config_path=None):
    """
    Get the POSSUM status token from the environment or 
    initiate it from Prefect secret if not present.
    """
    if database_config_path:
        load_dotenv(dotenv_path=database_config_path)
        # first, look for POSSUM_STATUS_TOKEN
        Google_API_token = os.getenv("POSSUM_STATUS_TOKEN")
        if not os.path.isfile(Google_API_token):
            raise FileNotFoundError(
                f"Google API token file not found at {Google_API_token}"
            )
        # now, look for POSSUM_STATUS_SHEET
        possum_status_sheet = os.getenv("POSSUM_STATUS_SHEET")
        if not possum_status_sheet:
            raise ValueError(
                "POSSUM_STATUS_SHEET environment variable not set"
            )
    else:
        # load secrets into environment variables
        Google_API_token = write_possum_token_file()
        os.environ["POSSUM_STATUS_TOKEN"] = Google_API_token
        possum_status_sheet = Secret.load("possum-status-sheet").get()
        os.environ["POSSUM_STATUS_SHEET"] = possum_status_sheet

    return Google_API_token  
    
def write_possum_token_file():
    """
    Write a Google API json token file to access the POSSUM status sheet
    using Prefect secret 'possum-status-token'.
    """
    # Construct the full path using the user's home directory
    home_dir = os.path.expanduser("~")
    ssl_dir = os.path.join(home_dir, ".ssl")
    file_path = os.path.join(ssl_dir, "possum.json")
 
    write_to_file(ssl_dir, file_path, 'possum-status-token')
    return file_path

def write_to_file(dir, file_path, secret_name):
    # Create the directory if it does not exist
    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
            print(f"Created directory: {dir}")
        except OSError as e:
            # Handle potential permissions issues or race conditions
            if e.errno != errno.EEXIST:
                raise

    # Write the content to the file
    try:
        with open(file_path, "w") as f:
            secret_block = Secret.load(secret_name)
            secret_str = secret_block.get()
            if secret_str:
                if isinstance(secret_str, dict):
                    # json file
                    json.dump(secret_str, f)
                else:    
                    f.write(secret_str.strip()) 
        print(f"Successfully wrote to: {file_path}")
    except IOError as e:
        print(f"Error writing to file {file_path}: {e}")
        raise

class TemporaryWorkingDirectory:
    """Context manager to temporarily change the current working directory."""

    def __init__(self, path: str | os.PathLike):
        self.new_path = Path(path).expanduser().resolve()
        self._original_path: Path | None = None

    def __enter__(self) -> Path:
        # Store the original working directory
        self._original_path = Path.cwd()

        # Change to the new directory
        os.chdir(self.new_path)

        # Optionally return the new path for use inside the with-block
        return self.new_path

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        # Always restore the original directory, even if an exception occurred
        if self._original_path is not None:
            os.chdir(self._original_path)

        # Returning False means exceptions are not suppressed
        return False
