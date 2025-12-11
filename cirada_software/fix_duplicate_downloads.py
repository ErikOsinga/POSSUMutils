#!/usr/bin/env python3
"""
Fix duplicate downloads for ASKAP tiles based on symbolic link logs.

This script:
- Finds the latest symbolic link log in a log directory.
- Parses that log for lines like:
    "Tile 5761 skipped, band: 943MHz found 5 files instead of 4."
- Selects tiles that have more than 4 files (up to and including 8).
- Calls cirada_software.delete_duplicate_downloads.dedupe_tiles for each such tile.
"""

import argparse
import logging
import re
import sys
from pathlib import Path

from cirada_software.delete_duplicate_downloads import dedupe_tiles


LOGGER = logging.getLogger(__name__)


def latest_file(directory: Path, pattern: str) -> Path | None:
    """
    Return the latest-modified file in 'directory' that matches 'pattern'.

    Parameters
    ----------
    directory : Path
        Directory to search in.
    pattern : str
        Glob pattern, e.g. 'symbolic_links_log_*' or '*log'.

    Returns
    -------
    Path | None
        Path to the most recently modified matching file, or None if no match.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise NotADirectoryError(f"{directory} is not a valid directory")

    candidates = [p for p in directory.glob(pattern) if p.is_file()]
    if not candidates:
        return None

    return max(candidates, key=lambda p: p.stat().st_mtime)


def parse_skipped_tiles_with_excess_files(
    log_file: Path,
    min_files: int = 5,
    max_files: int = 8,
) -> dict[int, int]:
    """
    Parse the log file for tiles that were skipped because they had
    between min_files and max_files (inclusive) instead of 4.

    Expected line format:
    'Tile 5761 skipped, band: 943MHz found 5 files instead of 4.'

    Parameters
    ----------
    log_file : Path
        Path to the log file.
    min_files : int
        Minimum number of files (inclusive) to consider "excess".
    max_files : int
        Maximum number of files (inclusive) to consider.

    Returns
    -------
    dict[int, int]
        Mapping from tile number to number of files.
        If the same tile appears multiple times, the last occurrence wins.
    """
    pattern = re.compile(
        r"Tile\s+(\d+)\s+skipped,\s+band:\s+\S+\s+found\s+(\d+)\s+files instead of 4\.",
        re.IGNORECASE,
    )

    tiles: dict[int, int] = {}

    with log_file.open("r", encoding="utf-8") as fh:
        for line in fh:
            match = pattern.search(line)
            if not match:
                continue

            tile_str, nfiles_str = match.groups()
            tile = int(tile_str)
            nfiles = int(nfiles_str)

            if min_files <= nfiles <= max_files:
                tiles[tile] = nfiles

    return tiles


def run_deduplication(
    download_dir: Path,
    log_dir: Path,
    log_glob: str = "symbolic_links_log_*",
) -> None:
    """
    Main workflow:
    - Find latest log file.
    - Parse tiles with excess files.
    - Call dedupe_tiles for each tile.

    Parameters
    ----------
    download_dir : Path
        Root directory where tile downloads are stored.
    log_dir : Path
        Directory containing symbolic link logs.
    log_glob : str
        Glob pattern to find log files in log_dir.
    """
    LOGGER.info("Using download directory: %s", download_dir)
    LOGGER.info("Using log directory: %s", log_dir)

    if not download_dir.is_dir():
        LOGGER.error("Download directory does not exist or is not a directory: %s", download_dir)
        sys.exit(1)

    if not log_dir.is_dir():
        LOGGER.error("Log directory does not exist or is not a directory: %s", log_dir)
        sys.exit(1)

    latest_log = latest_file(log_dir, log_glob)
    if latest_log is None:
        LOGGER.error("No log files found in %s matching pattern '%s'", log_dir, log_glob)
        sys.exit(1)

    LOGGER.info("Latest log file: %s", latest_log)

    tiles_with_excess = parse_skipped_tiles_with_excess_files(latest_log)
    if not tiles_with_excess:
        LOGGER.info(
            "No tiles with more than 4 (and up to 8) files found in %s. Nothing to do.",
            latest_log,
        )
        return

    LOGGER.info(
        "Found %d tile(s) with excess files: %s",
        len(tiles_with_excess),
        ", ".join(str(t) for t in sorted(tiles_with_excess)),
    )

    for tile, nfiles in sorted(tiles_with_excess.items()):
        LOGGER.info(
            "Running dedupe_tiles for tile %d (found %d files).",
            tile,
            nfiles,
        )
        pattern = r"20*-0*/*/*/*_{tile}_*"
        dedupe_tiles(
            tile=tile,
            root=download_dir,
            pattern=pattern,
            delete=True,
        )


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build the argument parser for the command line interface.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Fix duplicate downloads for ASKAP tiles based on the latest symbolic link log."
        )
    )

    parser.add_argument(
        "--downloaddir",
        type=Path,
        required=True,
        help="Root directory containing downloaded ASKAP tiles.",
    )
    parser.add_argument(
        "--logdir",
        type=Path,
        required=True,
        help="Directory containing symbolic link log files.",
    )

    return parser


def main() -> None:
    """
    Entry point for command line execution.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = build_arg_parser()
    args = parser.parse_args()

    run_deduplication(
        download_dir=args.downloaddir,
        log_dir=args.logdir,
    )


if __name__ == "__main__":
    main()
