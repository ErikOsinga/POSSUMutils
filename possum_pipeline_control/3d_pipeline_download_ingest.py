#!/usr/bin/env python3

import os
import subprocess
from pathlib import Path

from prefect import flow, task


@task(name="Change_to_downloads_directory")
def change_directory(workdir: str) -> str:
    os.chdir(workdir)
    return os.getcwd()


@task(name="Check_for_config.yml")
def config_exists(workdir: str, config_name: str = "config.yml") -> bool:
    return (Path(workdir) / config_name).exists()


@task(name="Run-possum_run_remote")
def run_possum_run_remote(workdir: str) -> None:
    # Run in the target directory regardless of current working directory
    subprocess.run(
        ["possum_run_remote"],
        cwd=workdir,
        check=True,
    )


@flow(name="download_and_ingest_tiles", log_prints=True)
def download_and_ingest_tiles_flow(
    workdir: str = "/arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads",
    config_name: str = "config.yml",
) -> None:
    change_directory(workdir)

    if config_exists(workdir, config_name):
        print("Running possum_run_remote")
        run_possum_run_remote(workdir)
        print("possum_run_remote finished")
    else:
        print("possum_run_remote config.yml file does not exist!")


if __name__ == "__main__":
    download_and_ingest_tiles_flow()
