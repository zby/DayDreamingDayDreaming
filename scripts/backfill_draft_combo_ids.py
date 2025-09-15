#!/usr/bin/env python3
"""Backfill combo_id into draft metadata.json files using cohort membership.

Usage:
    python scripts/backfill_draft_combo_ids.py --data-root /path/to/data

Given a DayDreaming data root, this script scans cohorts/*/membership.csv for
draft rows and ensures the corresponding data/gens/draft/<gen_id>/metadata.json
contains a combo_id field matching the membership entry. Existing combo_id
values are left unchanged when they already match.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import pandas as pd

def _load_membership_paths(cohorts_dir: Path) -> Iterable[Path]:
    if not cohorts_dir.exists():
        return []
    return sorted(p for p in cohorts_dir.iterdir() if p.is_dir())


def backfill_combo_ids(data_root: Path, *, dry_run: bool = False) -> dict[str, int]:
    """Populate combo_id for draft metadata using cohort membership rows.

    Returns simple counters describing the operation.
    """

    cohorts_dir = data_root / "cohorts"
    gens_root = data_root / "gens" / "draft"
    stats = {
        "cohorts": 0,
        "membership_csvs": 0,
        "draft_rows": 0,
        "skipped_missing_combo": 0,
        "metadata_missing": 0,
        "metadata_errors": 0,
        "unchanged": 0,
        "updates": 0,
    }

    for cohort_path in _load_membership_paths(cohorts_dir):
        stats["cohorts"] += 1
        mpath = cohort_path / "membership.csv"
        if not mpath.exists():
            continue

        try:
            df = pd.read_csv(mpath)
            stats["membership_csvs"] += 1
        except Exception as exc:
            print(f"! Failed to read {mpath}: {exc}")
            stats["metadata_errors"] += 1
            continue

        if df.empty or "stage" not in df.columns or "gen_id" not in df.columns:
            continue

        draft_rows = df[df["stage"].astype(str).str.lower() == "draft"]
        if draft_rows.empty or "combo_id" not in draft_rows.columns:
            continue

        for _idx, row in draft_rows.iterrows():
            gen_id = str(row.get("gen_id") or "").strip()
            combo_id = str(row.get("combo_id") or "").strip()
            if not gen_id or not combo_id:
                stats["skipped_missing_combo"] += 1
                continue

            stats["draft_rows"] += 1
            meta_path = gens_root / gen_id / "metadata.json"
            if not meta_path.exists():
                print(f"- Skipping {gen_id}: metadata.json not found")
                stats["metadata_missing"] += 1
                continue

            try:
                metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception as exc:
                print(f"! Failed to parse {meta_path}: {exc}")
                stats["metadata_errors"] += 1
                continue

            current = metadata.get("combo_id")
            if current is not None:
                current_str = str(current).strip()
                if current_str and current_str != combo_id:
                    raise RuntimeError(
                        f"combo_id mismatch for {gen_id}: metadata has '{current_str}' but membership has '{combo_id}'"
                    )
                if current_str == combo_id:
                    stats["unchanged"] += 1
                    continue

            print(f"* Updating {gen_id}: combo_id -> '{combo_id}'")
            metadata["combo_id"] = combo_id
            stats["updates"] += 1
            if not dry_run:
                meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill draft combo_id metadata")
    repo_data = Path.cwd() / "data"
    default_data_root = repo_data if repo_data.exists() else Path.cwd()
    parser.add_argument(
        "--data-root",
        type=Path,
        default=default_data_root,
        help=f"Path to project data root (default: {default_data_root})",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Actually write changes (default is dry-run)",
    )
    args = parser.parse_args()

    data_root = args.data_root.resolve()
    if not data_root.exists():
        raise SystemExit(f"Data root does not exist: {data_root}")

    dry_run = not args.write
    stats = backfill_combo_ids(data_root, dry_run=dry_run)
    suffix = " (dry run)" if dry_run else ""
    print(
        "Data root: {root}\n"
        "Done. Updated {updates} draft metadata files{suffix}. Scanned {csvs} membership CSVs"
        " with {rows} draft rows ({unchanged} already matched, {missing} metadata missing).".format(
            root=data_root,
            updates=stats["updates"],
            suffix=suffix,
            csvs=stats["membership_csvs"],
            rows=stats["draft_rows"],
            unchanged=stats["unchanged"],
            missing=stats["metadata_missing"],
        )
    )


if __name__ == "__main__":
    main()
