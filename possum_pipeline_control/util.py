from __future__ import annotations
from pathlib import Path
import os

"""
Utility methods shared across the scripts
"""


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
