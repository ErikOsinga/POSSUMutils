#!/usr/bin/env python3
"""
dedupe_tiles.py — keep the newest copy of each matching file, delete older duplicates.

Assumes a directory structure like:
  YYYY-MM-DDThh_mm_ss[_ms]_YYYY-MM-DDThh_mm_ss[_ms]/.../<tile>/...<tile>...fits

Examples:
  # Show what would be deleted for tile 10997 (dry-run)
  python dedupe_tiles.py 10997

  # Actually delete the older duplicates
  python dedupe_tiles.py 10997 --delete

  # move to a safe place instead of deleting
  python dedupe_tiles.py 10997 --moveto /path/to/safe/place

  # Restrict search to a root
  python dedupe_tiles.py 10997 --root /arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads

  # Narrow/adjust the glob if needed
  python dedupe_tiles.py 10997 --pattern '202*-0*/*/*/*{tile}*'
"""

from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Iterable
import shutil


# Matches timestamps like 2025-04-29T03_28_45 or 2025-04-28T22_01_08_879291
TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}(?:_\d+)?"
)


def parse_timestamp(token: str) -> Optional[datetime]:
    """
    Convert a token like '2025-04-28T22_01_08_879291' to a datetime.
    Microseconds are optional and represented as an extra '_######' chunk.

    Strategy:
      - replace underscores with separators where appropriate
      - try microsecond-aware first, then without.
    """
    base = token.replace("T", "T")
    parts = token.split("_")
    # parts: [YYYY-MM-DDThh, mm, ss, (optional usec)]
    try:
        if len(parts) == 4:
            fmt = "%Y-%m-%dT%H_%M_%S_%f"
        elif len(parts) == 3:
            fmt = "%Y-%m-%dT%H_%M_%S"
        else:
            return None
        return datetime.strptime(token, fmt)
    except ValueError:
        return None


def extract_best_datetime(path: Path) -> Optional[datetime]:
    """
    Find ALL timestamp-like tokens in the full path and return the latest one.

    Rationale:
      The top-level dir encodes a start_end time block, and some paths include
      microseconds. Taking the maximum timestamp across the path is a robust
      proxy for "newest run". This avoids relying on filesystem mtimes.
    """
    text = str(path)
    candidates = []
    for m in TIMESTAMP_RE.finditer(text):
        dt = parse_timestamp(m.group(0))
        if dt is not None:
            candidates.append(dt)
    if not candidates:
        return None
    return max(candidates)


def find_matches(root: Path, tile: str, pattern: str) -> List[Path]:
    """
    Expand a glob pattern that includes '{tile}' and return matching files.
    """
    glob_pat = pattern.format(tile=tile)
    return [p for p in root.glob(glob_pat) if p.is_file()]


def group_by_basename(paths: Iterable[Path]) -> Dict[str, List[Path]]:
    """
    Group paths by their basename (the filename) so we can dedupe per product.
    """
    groups: Dict[str, List[Path]] = {}
    for p in paths:
        groups.setdefault(p.name, []).append(p)
    return groups


def choose_latest(paths: List[Path]) -> Path:
    """
    Pick the path with the newest (max) extracted timestamp; if none parse,
    fall back to latest modification time.
    """
    with_dt = []
    without_dt = []
    for p in paths:
        dt = extract_best_datetime(p)
        if dt is None:
            without_dt.append(p)
        else:
            with_dt.append((dt, p))

    if with_dt:
        with_dt.sort(key=lambda x: x[0])
        return with_dt[-1][1]

    # Fallback: mtime
    return max(paths, key=lambda p: p.stat().st_mtime)


def plan_deletions(groups: Dict[str, List[Path]]) -> Dict[str, List[Path]]:
    """
    For each basename group, keep the newest and mark others for deletion.
    """
    to_delete: Dict[str, List[Path]] = {}
    for fname, paths in groups.items():
        if len(paths) < 2:
            continue
        latest = choose_latest(paths)
        older = [p for p in paths if p != latest]
        if older:
            to_delete[fname] = older
    return to_delete


def delete_paths(paths: Iterable[Path]) -> None:
    for p in paths:
        try:
            p.unlink()
        except Exception as exc:
            logging.error("Failed to delete %s: %s", p, exc)

def safe_target_path(dest: Path, root: Path, src: Path) -> Path:
    """
    Build a destination path inside 'dest' that preserves the relative path
    under --root. If that exact target exists, append .dupN to avoid clobbering.
    """
    try:
        rel = src.resolve().relative_to(Path(root).resolve())
        target = dest / rel
    except Exception:
        # Fallback if src is outside root or cannot be relativized
        target = dest / src.name

    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        return target

    stem, suffix = target.stem, target.suffix
    i = 1
    while True:
        cand = target.with_name(f"{stem}.dup{i}{suffix}")
        if not cand.exists():
            return cand
        i += 1

def move_paths(paths: Iterable[Path], dest: Path, root: Path) -> list[tuple[Path, Path]]:
    """
    Move files into 'dest', preserving relative paths under --root.
    Returns list of (src, final_dest) pairs.
    """
    moved: list[tuple[Path, Path]] = []
    for p in paths:
        t = safe_target_path(dest, root, p)
        shutil.move(str(p), str(t))
        moved.append((p, t))
    return moved

def _setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete older duplicates of tile files, keeping only the newest copy per filename."
    )
    parser.add_argument("tile", help="Tile number or token to search for (e.g., 10997).")
    parser.add_argument(
        "--root",
        default=".",
        help="Root directory to search (default: current directory).",
    )
    parser.add_argument(
        "--pattern",
        default="202*-0*/*/*/*{tile}*",
        help="Glob pattern relative to --root; must include '{tile}'. (default: %(default)s)",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete files. Without this flag, performs a dry-run."
    )
    group.add_argument(
        "--moveto",
        metavar="DIR",
        help="Move older duplicates into DIR (preserves path relative to --root)."
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v for INFO, -vv for DEBUG).",
    )
    args = parser.parse_args()
    _setup_logging(args.verbose)

    root = Path(args.root).resolve()
    if "{tile}" not in args.pattern:
        parser.error("--pattern must include '{tile}' placeholder.")

    matches = find_matches(root, args.tile, args.pattern)
    if not matches:
        print(f"No matches found under {root} for tile '{args.tile}' with pattern '{args.pattern}'.")
        return

    logging.info("Found %d matching files.", len(matches))
    groups = group_by_basename(matches)
    deletions = plan_deletions(groups)

    if not deletions:
        print("No duplicate basenames found; nothing to delete or move.")
        return

    total = sum(len(v) for v in deletions.values())
    print(f"Found {len(deletions)} duplicated filenames; {total} files are candidates.\n")

    # Preview
    if args.moveto:
        dest = Path(args.moveto).resolve()
        print(f"Planned moves to {dest}:\n")
    else:
        print("Dry-run (no changes). Use --delete to remove, or --moveto DIR to move.\n")

    for fname, paths in sorted(deletions.items()):
        newest = choose_latest(groups[fname])  # keep newest among all copies
        print(f"[KEEP]   {newest}")
        for p in sorted(paths):
            if args.moveto:
                preview = safe_target_path(Path(args.moveto).resolve(), root, p)
                print(f"[MOVE]   {p} -> {preview}")
            else:
                print(f"[DELETE] {p}")
        print("")

    # Execute
    all_to_delete = sorted([p for plist in deletions.values() for p in plist], key=lambda x: str(x))

    if args.delete:
        delete_paths(all_to_delete)
        print(f"Deleted {len(all_to_delete)} files.")
    elif args.moveto:
        dest = Path(args.moveto).resolve()
        dest.mkdir(parents=True, exist_ok=True)
        moved = move_paths(all_to_delete, dest, root)
        print(f"Moved {len(moved)} files into {dest} (paths preserved under --root).")


if __name__ == "__main__":
    main()

