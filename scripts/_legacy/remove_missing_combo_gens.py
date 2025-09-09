#!/usr/bin/env python3
"""
Remove gens for generations that reference combo_ids missing from data/combo_mappings.csv.

Scopes only the gens store (data/gens) and leaves any legacy stores untouched.

Behavior:
- Scans data/gens/{draft,essay,evaluation}/*/metadata.json
- Resolves each doc's combo_id via:
  - draft: metadata.combo_id
  - essay: metadata.parent_gen_id -> read draft metadata.combo_id
  - evaluation: metadata.parent_gen_id (essay) -> essay.parent_gen_id (draft) -> draft metadata.combo_id
- Compares against data/combo_mappings.csv combo_id set
- Prints a summary of affected docs grouped by stage
- Fails (exit code 2) if any missing combos are detected and --apply is not set
- With --apply, deletes the matching gens directories and exits 0

Usage:
  uv run python scripts/remove_missing_combo_gens.py --data-root data --apply
  uv run python scripts/remove_missing_combo_gens.py --data-root data            # dry-run; fails if any
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Dict, List, Tuple
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--apply", action="store_true", help="Apply deletions (otherwise dry-run and fail if any found)")
    return p.parse_args()


def _load_known_combos(data_root: Path) -> set[str]:
    csv = data_root / "combo_mappings.csv"
    if not csv.exists():
        print(f"ERROR: {csv} not found", file=sys.stderr)
        raise SystemExit(2)
    df = pd.read_csv(csv)
    if "combo_id" not in df.columns:
        print(f"ERROR: {csv} missing required 'combo_id' column", file=sys.stderr)
        raise SystemExit(2)
    return set(df["combo_id"].astype(str).unique().tolist())


def _read_json(path: Path) -> Dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _draft_combo_for_essay(gens_root: Path, essay_id: str) -> str | None:
    em = _read_json(gens_root / "essay" / essay_id / "metadata.json")
    parent = str(em.get("parent_gen_id") or "").strip()
    if not parent:
        return None
    dm = _read_json(gens_root / "draft" / parent / "metadata.json")
    cid = str(dm.get("combo_id") or "").strip()
    return cid or None


def _draft_combo_for_eval(gens_root: Path, eval_id: str) -> str | None:
    ev = _read_json(gens_root / "evaluation" / eval_id / "metadata.json")
    essay_id = str(ev.get("parent_gen_id") or "").strip()
    if not essay_id:
        return None
    return _draft_combo_for_essay(gens_root, essay_id)


def find_missing_combo_docs(data_root: Path) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]], List[Tuple[str, str]]]:
    gens_root = data_root / "gens"
    known = _load_known_combos(data_root)
    missing_drafts: List[Tuple[str, str]] = []
    missing_essays: List[Tuple[str, str]] = []
    missing_evals: List[Tuple[str, str]] = []

    # Drafts
    draft_root = gens_root / "draft"
    if draft_root.exists():
        for d in draft_root.iterdir():
            if not d.is_dir():
                continue
            mid = d.name
            dm = _read_json(d / "metadata.json")
            cid = str(dm.get("combo_id") or "").strip()
            if cid and cid not in known:
                missing_drafts.append((mid, cid))

    # Essays
    essay_root = gens_root / "essay"
    if essay_root.exists():
        for e in essay_root.iterdir():
            if not e.is_dir():
                continue
            eid = e.name
            cid = _draft_combo_for_essay(gens_root, eid)
            if cid and cid not in known:
                missing_essays.append((eid, cid))

    # Evaluations
    eval_root = gens_root / "evaluation"
    if eval_root.exists():
        for ev in eval_root.iterdir():
            if not ev.is_dir():
                continue
            evid = ev.name
            cid = _draft_combo_for_eval(gens_root, evid)
            if cid and cid not in known:
                missing_evals.append((evid, cid))

    return missing_drafts, missing_essays, missing_evals


def main() -> int:
    args = parse_args()
    data_root = args.data_root
    gens_root = data_root / "gens"
    if not gens_root.exists():
        print(f"No gens store found at {gens_root} — nothing to do")
        return 0

    missing_drafts, missing_essays, missing_evals = find_missing_combo_docs(data_root)
    total = len(missing_drafts) + len(missing_essays) + len(missing_evals)

    if total == 0:
        print("No gens referencing missing combos were found. ✅")
        return 0

    print("Gens referencing missing combo_ids detected:")
    if missing_drafts:
        print("- draft:")
        for gid, cid in missing_drafts[:50]:
            print(f"  * {gid} (combo_id={cid})")
        if len(missing_drafts) > 50:
            print(f"  ... and {len(missing_drafts)-50} more")
    if missing_essays:
        print("- essay:")
        for gid, cid in missing_essays[:50]:
            print(f"  * {gid} (combo_id={cid})")
        if len(missing_essays) > 50:
            print(f"  ... and {len(missing_essays)-50} more")
    if missing_evals:
        print("- evaluation:")
        for gid, cid in missing_evals[:50]:
            print(f"  * {gid} (combo_id={cid})")
        if len(missing_evals) > 50:
            print(f"  ... and {len(missing_evals)-50} more")

    if not args.apply:
        print("\nDry-run. Failing due to missing combos. Re-run with --apply to delete these gens.")
        return 2

    # Apply deletions
    def _rm(stage: str, gid: str) -> None:
        p = gens_root / stage / gid
        if p.exists():
            shutil.rmtree(p)

    for gid, _ in missing_drafts:
        _rm("draft", gid)
    for gid, _ in missing_essays:
        _rm("essay", gid)
    for gid, _ in missing_evals:
        _rm("evaluation", gid)

    print(f"Deleted {total} gens directories from {gens_root} (draft={len(missing_drafts)}, essay={len(missing_essays)}, evaluation={len(missing_evals)}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

