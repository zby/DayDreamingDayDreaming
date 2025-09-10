#!/usr/bin/env python3
"""
Purge all generations belonging to a specific cohort from the gens store.

Deletes directories under data/gens/{draft,essay,evaluation}/<gen_id> whose
metadata.json contains the given cohort_id. Deletion order is evaluation → essay → draft
to avoid transient parent-orphan states during removal.

Usage examples:
  # Dry-run (default):
  uv run python scripts/purge_cohort.py --cohort-id restored-TEST

  # Actually delete:
  uv run python scripts/purge_cohort.py --cohort-id restored-TEST --execute

Options:
  --data-root   Base data directory (default: data)
  --stages      Limit stages (default: evaluation essay draft)
  --execute     Perform deletions; otherwise only prints what would be deleted

Exit codes:
  0 success
  2 no matching generations found
  3 error
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
from typing import Dict, List, Tuple
import sys


DEFAULT_STAGES = ("evaluation", "essay", "draft")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--cohort-id", required=True, help="Cohort id to purge")
    p.add_argument("--stages", nargs="*", choices=list(DEFAULT_STAGES), default=list(DEFAULT_STAGES), help="Stages to process (default: evaluation essay draft)")
    p.add_argument("--execute", action="store_true", help="Actually delete directories (otherwise dry-run)")
    return p.parse_args()


def read_cohort_id(md_path: Path) -> str:
    try:
        md = json.loads(md_path.read_text(encoding="utf-8"))
        val = md.get("cohort_id")
        return str(val).strip() if val is not None else ""
    except Exception:
        return ""


def collect_targets(data_root: Path, cohort_id: str, stages: List[str]) -> Dict[str, List[Path]]:
    gens_root = data_root / "gens"
    out: Dict[str, List[Path]] = {s: [] for s in stages}
    for stage in stages:
        stage_dir = gens_root / stage
        if not stage_dir.exists():
            continue
        for child in stage_dir.iterdir():
            if not child.is_dir():
                continue
            mdp = child / "metadata.json"
            if not mdp.exists():
                continue
            cid = read_cohort_id(mdp)
            if cid == cohort_id:
                out[stage].append(child)
    return out


def purge(targets: Dict[str, List[Path]], *, execute: bool) -> Tuple[int, int]:
    """Return (examined, deleted)."""
    examined = 0
    deleted = 0
    for stage in DEFAULT_STAGES:  # always honor safe order
        dirs = targets.get(stage, [])
        if not dirs:
            continue
        print(f"Stage {stage}: {len(dirs)} to delete")
        for d in sorted(dirs):
            examined += 1
            if execute:
                shutil.rmtree(d, ignore_errors=True)
                deleted += 1
            else:
                print(f"[dry-run] Would delete {d}")
    return examined, deleted


def main() -> int:
    args = parse_args()
    data_root: Path = args.data_root
    cohort_id: str = args.cohort_id
    stages: List[str] = list(args.stages)

    if not cohort_id:
        print("ERROR: --cohort-id is required", file=sys.stderr)
        return 3

    targets = collect_targets(data_root, cohort_id, stages)
    total = sum(len(v) for v in targets.values())
    if total == 0:
        print(f"No generations found for cohort_id='{cohort_id}' under {data_root}/gens")
        return 2

    examined, deleted = purge(targets, execute=args.execute)
    print(f"Summary: cohort='{cohort_id}' examined={examined} deleted={deleted} dry_run={not args.execute}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

