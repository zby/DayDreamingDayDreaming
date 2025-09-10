#!/usr/bin/env python3
"""
Find generations that reference a non-existent parent.

Checks gens/<stage>/<gen_id>/metadata.json for parent_gen_id and verifies that
the expected parent directory exists:
  - essay -> parent is draft
  - evaluation -> parent is essay

By default scans both essay and evaluation stages.

Usage examples:
  uv run python scripts/data_checks/find_missing_parents.py
  uv run python scripts/data_checks/find_missing_parents.py --stage essay
  uv run python scripts/data_checks/find_missing_parents.py --output /tmp/missing_parents.csv

Exit codes:
  0  no missing parents found
  2  missing parents found
  3  fatal error
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple
import csv
import sys


PARENT_STAGE = {
    "essay": "draft",
    "evaluation": "essay",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument(
        "--stage",
        choices=["essay", "evaluation", "all"],
        default="all",
        help="Stage to scan (default: all)",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional CSV to write rows: stage,gen_id,parent_gen_id,expected_parent_stage,expected_parent_path,problem",
    )
    return p.parse_args()


def _read_metadata(path: Path) -> Dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def scan_stage(data_root: Path, stage: str) -> Tuple[List[Dict[str, str]], int, int]:
    """Return (missing_rows, scanned_docs, skipped_docs)."""
    assert stage in ("essay", "evaluation"), stage
    parent_stage = PARENT_STAGE[stage]
    stage_dir = data_root / "gens" / stage
    missing: List[Dict[str, str]] = []
    scanned = 0
    skipped = 0

    if not stage_dir.exists():
        print(f"No {stage} directory found: {stage_dir}")
        return missing, scanned, skipped

    for child in stage_dir.iterdir():
        if not child.is_dir():
            continue
        gen_id = child.name
        meta_fp = child / "metadata.json"
        if not meta_fp.exists():
            skipped += 1
            continue
        meta = _read_metadata(meta_fp)
        parent_gen_id = str(meta.get("parent_gen_id") or "").strip()
        if not parent_gen_id:
            skipped += 1
            continue
        scanned += 1
        parent_dir = data_root / "gens" / parent_stage / parent_gen_id
        if not parent_dir.exists():
            missing.append(
                {
                    "stage": stage,
                    "gen_id": gen_id,
                    "parent_gen_id": parent_gen_id,
                    "expected_parent_stage": parent_stage,
                    "expected_parent_path": str(parent_dir),
                    "problem": "missing_parent_dir",
                }
            )
    return missing, scanned, skipped


def write_csv(rows: List[Dict[str, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "stage",
                "gen_id",
                "parent_gen_id",
                "expected_parent_stage",
                "expected_parent_path",
                "problem",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Wrote CSV: {out_path} ({len(rows)} rows)")


def main() -> int:
    args = parse_args()
    stages = ("essay", "evaluation") if args.stage == "all" else (args.stage,)
    try:
        all_missing: List[Dict[str, str]] = []
        total_scanned = 0
        total_skipped = 0
        for stage in stages:
            missing, scanned, skipped = scan_stage(args.data_root, stage)
            total_scanned += scanned
            total_skipped += skipped
            if missing:
                print(f"[{stage}] Missing parents: {len(missing)} (scanned={scanned}, skipped={skipped})")
                # Preview first few
                for r in missing[:10]:
                    print(
                        f"  - gen_id={r['gen_id']} parent={r['parent_gen_id']} expected={r['expected_parent_path']}"
                    )
            else:
                print(f"[{stage}] No missing parents (scanned={scanned}, skipped={skipped})")
            all_missing.extend(missing)

        if args.output:
            write_csv(all_missing, args.output)

        print(
            f"Summary: missing={len(all_missing)} scanned={total_scanned} skipped={total_skipped} stages={','.join(stages)}"
        )
        return 2 if all_missing else 0
    except Exception as e:
        print(f"ERROR: scan failed: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())

