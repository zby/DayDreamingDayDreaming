#!/usr/bin/env python3
"""Utility to identify (and optionally delete) evaluation runs missing artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil


def find_incomplete_evaluations(evaluation_root: Path) -> tuple[list[Path], list[Path]]:
    """Return directories missing metadata and raw artifacts.

    Args:
        evaluation_root: Path to `data/gens/evaluation` directory.

    Returns:
        A tuple `(missing_metadata, missing_raw)` with lists of directory Paths.
    """

    missing_metadata: list[Path] = []
    missing_raw: list[Path] = []

    if not evaluation_root.exists():
        return missing_metadata, missing_raw

    for gen_dir in sorted(p for p in evaluation_root.iterdir() if p.is_dir()):
        meta_path = gen_dir / "metadata.json"
        raw_path = gen_dir / "raw.txt"

        if not meta_path.exists():
            missing_metadata.append(gen_dir)
        if not raw_path.exists():
            missing_raw.append(gen_dir)

    return missing_metadata, missing_raw


def delete_directories(directories: list[Path]) -> None:
    for directory in directories:
        if directory.exists() and directory.is_dir():
            shutil.rmtree(directory)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Identify evaluation runs missing metadata or raw artifacts."
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Path to project data root (default: data)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete directories missing required artifacts.",
    )
    args = parser.parse_args()

    evaluation_root = args.data_root / "gens" / "evaluation"

    missing_metadata, missing_raw = find_incomplete_evaluations(evaluation_root)

    print(f"Evaluation root: {evaluation_root}")
    print(f"Runs missing metadata.json: {len(missing_metadata)}")
    if missing_metadata:
        for path in missing_metadata:
            print(f"  - {path}")

    print(f"Runs missing raw.txt: {len(missing_raw)}")
    if missing_raw:
        for path in missing_raw:
            print(f"  - {path}")

    if args.delete:
        if missing_metadata:
            print("Removing directories missing metadata.json ...")
            delete_directories(missing_metadata)
        if missing_raw:
            print("Removing directories missing raw.txt ...")
            delete_directories(missing_raw)
        print("Deletion complete.")


if __name__ == "__main__":
    main()
