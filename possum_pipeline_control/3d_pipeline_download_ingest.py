#!/usr/bin/env python3

import os
import subprocess

from pathlib import Path
from prefect import flow, task
from possum_pipeline_control import util


@task(name="Change to downloads directory")
def change_directory(workdir: str) -> str:
    os.chdir(workdir)
    return os.getcwd()


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
    staged_cert = util.stage_cadc_certificate(workdir=workdir)
    # Set access for the CIRADA-Polarimetry group: rw
    subprocess.run(
        ["setfacl", "-m", f"g:{group_name}:rw", ".ssl/cadcproxy.pem"],
        check=True,
    )
    print(f"Staged CADC certificate to: {staged_cert}")

    if config_exists(workdir, config_name):
        print("Running possum_run_remote")
        run_possum_run_remote(workdir)
        print("possum_run_remote finished")
    else:
        print("possum_run_remote config.yml file does not exist!")


if __name__ == "__main__":
    download_and_ingest_tiles_flow()
