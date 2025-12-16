#!/usr/bin/env python3
"""
dedupe_tiles — programmatic and CLI interface for removing or relocating
older duplicate files for a given tile.

Exposed API:
    - dedupe_tiles(...)
    - DedupePlanner(...)
    - utility functions (parse_timestamp, extract_best_datetime, etc.)
"""

from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Iterable, Tuple
import shutil


TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}(?:_\d+)?")


# ---------------------------------------------------------------------
# Timestamp utilities
# ---------------------------------------------------------------------


def parse_timestamp(token: str) -> Optional[datetime]:
    parts = token.split("_")
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
    text = str(path)
    candidates: List[datetime] = []
    for m in TIMESTAMP_RE.finditer(text):
        dt = parse_timestamp(m.group(0))
        if dt:
            candidates.append(dt)
    return max(candidates) if candidates else None


# ---------------------------------------------------------------------
# Matching, grouping, dedupe logic
# ---------------------------------------------------------------------


def find_matches(root: Path, tile: str, pattern: str) -> List[Path]:
    glob_pat = pattern.format(tile=tile)
    return [p for p in root.glob(glob_pat) if p.is_file()]


def group_by_basename(paths: Iterable[Path]) -> Dict[str, List[Path]]:
    groups: Dict[str, List[Path]] = {}
    for p in paths:
        groups.setdefault(p.name, []).append(p)
    return groups


def choose_latest(paths: List[Path]) -> Path:
    with_dt: List[Tuple[datetime, Path]] = []
    without_dt: List[Path] = []

    for p in paths:
        dt = extract_best_datetime(p)
        if dt is None:
            without_dt.append(p)
        else:
            with_dt.append((dt, p))

    if with_dt:
        with_dt.sort(key=lambda x: x[0])
        return with_dt[-1][1]

    return max(paths, key=lambda p: p.stat().st_mtime)


def plan_deletions(groups: Dict[str, List[Path]]) -> Dict[str, List[Path]]:
    to_delete: Dict[str, List[Path]] = {}
    for fname, paths in groups.items():
        if len(paths) < 2:
            continue
        latest = choose_latest(paths)
        older = [p for p in paths if p != latest]
        if older:
            to_delete[fname] = older
    return to_delete


# ---------------------------------------------------------------------
# File operations
# ---------------------------------------------------------------------


def delete_paths(paths: Iterable[Path]) -> None:
    for p in paths:
        try:
            p.unlink()
        except Exception as exc:
            logging.error("Failed to delete %s: %s", p, exc)


def safe_target_path(dest: Path, root: Path, src: Path) -> Path:
    try:
        rel = src.resolve().relative_to(root.resolve())
        target = dest / rel
    except Exception:
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


def move_paths(
    paths: Iterable[Path], dest: Path, root: Path
) -> List[Tuple[Path, Path]]:
    moved: List[Tuple[Path, Path]] = []
    for p in paths:
        t = safe_target_path(dest, root, p)
        shutil.move(str(p), str(t))
        moved.append((p, t))
    return moved


# ---------------------------------------------------------------------
# High-level planner class (importable API)
# ---------------------------------------------------------------------


class DedupePlanner:
    """
    Encapsulates the dedupe workflow so it can be reused programmatically.
    """

    def __init__(self, tile: str, root: Path, pattern: str):
        self.tile = tile
        self.root = root
        self.pattern = pattern
        self.matches: List[Path] = []
        self.groups: Dict[str, List[Path]] = {}
        self.deletions: Dict[str, List[Path]] = {}

    def run_discovery(self) -> None:
        self.matches = find_matches(self.root, self.tile, self.pattern)
        self.groups = group_by_basename(self.matches)
        self.deletions = plan_deletions(self.groups)

    def candidates(self) -> List[Path]:
        return sorted([p for plist in self.deletions.values() for p in plist], key=str)

    def execute_delete(self) -> int:
        to_delete = self.candidates()
        delete_paths(to_delete)
        return len(to_delete)

    def execute_move(self, dest: Path) -> List[Tuple[Path, Path]]:
        dest.mkdir(parents=True, exist_ok=True)
        return move_paths(self.candidates(), dest, self.root)


# ---------------------------------------------------------------------
# Simple programmatic API
# ---------------------------------------------------------------------


def dedupe_tiles(
    tile: str,
    root: str | Path = ".",
    pattern: str = r"20*-0*/*/*/*_{tile}_*",
    delete: bool = False,
    moveto: Optional[str | Path] = None,
) -> dict:
    """
    Programmatic API for deduping tiles.

    Returns a dictionary describing the action.
    """
    root = Path(root).resolve()

    if "{tile}" not in pattern:
        raise ValueError("pattern must contain '{tile}'")

    planner = DedupePlanner(tile=tile, root=root, pattern=pattern)
    planner.run_discovery()

    if not planner.matches:
        return {
            "status": "no_matches",
            "matches": [],
        }

    if not planner.deletions:
        return {
            "status": "no_duplicates",
            "matches": planner.matches,
        }

    if delete:
        n = planner.execute_delete()
        return {
            "status": "deleted",
            "count": n,
        }

    if moveto is not None:
        moved = planner.execute_move(Path(moveto).resolve())
        return {
            "status": "moved",
            "moved": moved,
        }

    return {
        "status": "dry_run",
        "deletions": planner.deletions,
    }


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------


def _setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def _print_cli_summary(result: dict) -> None:
    """
    Human-readable summary for CLI use.
    """
    status = result.get("status")

    if status == "no_matches":
        print("No matching files found.")
        return

    if status == "no_duplicates":
        n = len(result.get("matches", []))
        print(f"Found {n} files, but no duplicates. Nothing to do.")
        return

    if status == "dry_run":
        deletions = result.get("deletions", {})
        count = sum(len(v) for v in deletions.values())
        groups = len(deletions)
        print(
            f"Dry-run: found {groups} duplicated basenames; {count} files would be removed or moved."
        )
        return

    if status == "deleted":
        print(f"Deleted {result.get('count', 0)} files.")
        return

    if status == "moved":
        moved = result.get("moved", [])
        print(f"Moved {len(moved)} files.")
        # Pretty-print first few pairs
        for src, dst in moved[:5]:
            print(f"  {src} -> {dst}")
        if len(moved) > 5:
            print(f"  ... +{len(moved) - 5} more")
        return

    print(f"Unknown status: {status}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete or move older duplicates of tile files, keeping only the newest copy per filename."
    )
    parser.add_argument("tile")
    parser.add_argument("--root", default=".")
    parser.add_argument("--pattern", default=r"20*-0*/*/*/*_{tile}_*")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--delete", action="store_true")
    group.add_argument("--moveto", metavar="DIR")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args()

    _setup_logging(args.verbose)

    result = dedupe_tiles(
        tile=args.tile,
        root=args.root,
        pattern=args.pattern,
        delete=args.delete,
        moveto=args.moveto,
    )

    # Print human readable summary
    _print_cli_summary(result)

    # Also print the raw dict for scripting/logging
    print("")
    print("Raw result:")
    print(result)


if __name__ == "__main__":
    main()
