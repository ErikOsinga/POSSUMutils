"""

Script that takes as input a .log file and a number, and prints a list of tile numbers (one per line),
that have been skipped because they contain that number of tiles instead of 4.

e.g. from the command line:

python tilenumbers_with_ntiles.py --logfile symbolic_links_log_20251205_140648.txt --ntiles 5
"""

#!/usr/bin/env python3
import argparse
import re
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Print tile numbers from a symbolic links log that were skipped "
            "because they have a specific number of files instead of 4."
        )
    )
    parser.add_argument(
        "--logfile",
        required=True,
        help="Path to the symbolic links log file.",
    )
    parser.add_argument(
        "--ntiles",
        type=int,
        required=True,
        help="Number of files found for skipped tiles (e.g. 3, 5, 6).",
    )
    return parser.parse_args()


def extract_tiles_with_count(logfile: str, ntiles: int) -> list[int]:
    """
    Parse the logfile and return a list of tile numbers that were skipped
    because they had `ntiles` files instead of 4.
    """
    # Example line:
    # Tile 11137 skipped, band: 943MHz found 3 files instead of 4.
    pattern = re.compile(
        r"Tile\s+(\d+)\s+skipped.*found\s+(\d+)\s+files\s+instead\s+of\s+4\.",
        re.IGNORECASE,
    )

    tiles: list[int] = []

    try:
        with open(logfile, "r", encoding="utf-8") as fh:
            for line in fh:
                match = pattern.search(line)
                if not match:
                    continue

                tile_number = int(match.group(1))
                found_files = int(match.group(2))

                if found_files == ntiles:
                    tiles.append(tile_number)
    except FileNotFoundError:
        print(f"Error: logfile '{logfile}' not found.", file=sys.stderr)
        sys.exit(1)
    except OSError as exc:
        print(f"Error reading logfile '{logfile}': {exc}", file=sys.stderr)
        sys.exit(1)

    return tiles


def main() -> None:
    args = parse_args()
    tiles = extract_tiles_with_count(args.logfile, args.ntiles)

    # Print one tile number per line
    for tile in tiles:
        print(tile)


if __name__ == "__main__":
    main()
