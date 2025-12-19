#!/usr/bin/env python3

import os
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from prefect import flow, task


@task(name="Change to downloads directory")
def change_directory(workdir: str) -> str:
    os.chdir(workdir)
    return os.getcwd()


@task(name="Stage CADC certificate and set ACLs")
def stage_cadc_certificate(
    workdir: str,
    source_cert: str = "~/.ssl/cadcproxy.pem",
    dest_relpath: str = ".ssl/cadcproxy.pem",
    max_age_days: int = 30,
    group_name: str = "CIRADA-Polarimetry",
) -> str:
    src = Path(os.path.expanduser(source_cert))
    if not src.exists():
        raise FileNotFoundError(
            f"Required CADC certificate not found: {src}"
            "Please run cadc-get-cert -u <UserName> --days-valid 30 in a CANFAR interactive session "
            "to update your CADC certificate!!!"
        )

    # Check age, certificates are max valid for 30 days
    mtime = datetime.fromtimestamp(src.stat().st_mtime, tz=timezone.utc)
    if datetime.now(timezone.utc) - mtime > timedelta(days=max_age_days):
        raise ValueError(
            "Please run cadc-get-cert -u <UserName> --days-valid 30 in a CANFAR interactive session "
            "to update your CADC certificate!!!"
        )

    # Copy to workdir/.ssl/cadcproxy.pem (create directory if needed)
    dest = Path(workdir) / dest_relpath
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)

    # Set access for the CIRADA-Polarimetry group: rw
    subprocess.run(
        ["setfacl", "-m", f"g:{group_name}:rw", str(dest)],
        check=True,
    )

    return str(dest)


@task(name="Check for config.yml")
def config_exists(workdir: str, config_name: str = "config.yml") -> bool:
    return (Path(workdir) / config_name).exists()


@task(name="Run possum_run_remote")
def run_possum_run_remote(workdir: str) -> None:
    subprocess.run(
        ["possum_run_remote"],
        cwd=workdir,
        check=True,
    )


@flow(name="download_and_ingest_tiles")
def download_and_ingest_tiles_flow(
    workdir: str = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads",
    config_name: str = "config.yml",
    group_name: str = "CIRADA-Polarimetry",
) -> None:
    change_directory(workdir)

    # certificate freshness + staging + ACLs (Access Control Lists)
    staged_cert = stage_cadc_certificate(workdir=workdir, group_name=group_name)
    print(f"Staged CADC certificate to: {staged_cert}")

    if config_exists(workdir, config_name):
        print("Running possum_run_remote")
        run_possum_run_remote(workdir)
        print("possum_run_remote finished")
    else:
        print("possum_run_remote config.yml file does not exist!")


if __name__ == "__main__":
    download_and_ingest_tiles_flow()
