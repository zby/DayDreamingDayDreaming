#!/usr/bin/env python3
"""
Link tasks CSVs to the latest existing docs in the docs store (by task_id).

This does NOT regenerate any artifacts. It scans data/docs/<stage>/*/metadata.json
to find the most recent document per task_id, then updates the corresponding
tasks CSV to include doc_id (and parent_doc_id for essays) so downstream assets
can operate on the already-generated documents.

Stages handled:
- Drafts:   data/2_tasks/draft_generation_tasks.csv (column draft_task_id → doc_id)
- Essays:   data/2_tasks/essay_generation_tasks.csv (column essay_task_id → doc_id, parent_doc_id)
- Evaluations: data/2_tasks/evaluation_tasks.csv (column evaluation_task_id → doc_id, parent_doc_id)

Rules:
- Chooses the "latest" doc for a task_id by directory mtime (best-effort).
- Only fills missing doc_id/parent_doc_id by default; use --force to overwrite
  existing values.

Usage:
  uv run python scripts/link_tasks_to_latest_docs.py \
    --data-root data [--drafts] [--essays] [--evaluations] [--force] [--dry-run]
"""

from __future__ import annotations

import argparse
from pathlib import Path
import json
import os
import pandas as pd
from typing import Dict, Tuple


def _scan_stage_latest_by_task(docs_stage_root: Path) -> Dict[str, Tuple[str, float]]:
    """Return mapping task_id -> (doc_id, mtime_ns) for the latest doc per task.

    Latest is chosen by directory mtime. Requires metadata.json with a task_id.
    """
    out: Dict[str, Tuple[str, float]] = {}
    if not docs_stage_root.exists():
        return out
    for d in docs_stage_root.iterdir():
        if not d.is_dir():
            continue
        mp = d / "metadata.json"
        if not mp.exists():
            continue
        try:
            meta = json.loads(mp.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        task_id = str(meta.get("task_id") or "").strip()
        if not task_id:
            continue
        # Use directory mtime as the recency signal
        try:
            mtime_ns = os.stat(d).st_mtime_ns
        except Exception:
            mtime_ns = 0
        cur = out.get(task_id)
        if cur is None or mtime_ns > cur[1]:
            out[task_id] = (d.name, mtime_ns)
    return out


def _update_csv_with_mapping(
    csv_path: Path,
    key_col: str,
    doc_col: str,
    mapping: Dict[str, Tuple[str, float]],
    *,
    force: bool = False,
) -> Tuple[int, int]:
    """Fill doc_col in CSV rows using mapping[key_col] = (doc_id, mtime).

    Returns (rows_seen, rows_updated).
    """
    if not csv_path.exists():
        return (0, 0)
    df = pd.read_csv(csv_path)
    if key_col not in df.columns:
        return (0, 0)
    if doc_col not in df.columns:
        df[doc_col] = ""
    rows_seen = len(df)
    updated = 0
    def _maybe(val, existing):
        nonlocal updated
        if not val:
            return existing
        if not existing or str(existing).strip().lower() == "nan" or force:
            updated += 1
            return val
        return existing
    df[doc_col] = [
        _maybe(mapping.get(str(row[key_col]), ("", 0))[0], row.get(doc_col, ""))
        for _, row in df.iterrows()
    ]
    if updated > 0:
        df.to_csv(csv_path, index=False)
    return (rows_seen, updated)


def _update_essays_parent(csv_path: Path, parent_map: Dict[str, str], *, force: bool = False) -> Tuple[int, int]:
    """Fill parent_doc_id in essay_generation_tasks.csv using a map doc_id->parent_doc_id.

    Returns (rows_seen, rows_updated).
    """
    if not csv_path.exists():
        return (0, 0)
    df = pd.read_csv(csv_path)
    if "essay_task_id" not in df.columns:
        return (0, 0)
    if "doc_id" not in df.columns:
        return (0, 0)
    if "parent_doc_id" not in df.columns:
        df["parent_doc_id"] = ""
    rows_seen = len(df)
    updated = 0
    def _maybe(val, existing):
        nonlocal updated
        if not val:
            return existing
        if not existing or str(existing).strip().lower() == "nan" or force:
            updated += 1
            return val
        return existing
    df["parent_doc_id"] = [
        _maybe(parent_map.get(str(row.get("doc_id", "")), ""), row.get("parent_doc_id", ""))
        for _, row in df.iterrows()
    ]
    if updated > 0:
        df.to_csv(csv_path, index=False)
    return (rows_seen, updated)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--drafts", action="store_true", help="Update draft_generation_tasks.csv")
    ap.add_argument("--essays", action="store_true", help="Update essay_generation_tasks.csv")
    ap.add_argument("--evaluations", action="store_true", help="Update evaluation_tasks.csv")
    ap.add_argument("--force", action="store_true", help="Overwrite existing doc_id/parent_doc_id")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    data_root: Path = args.data_root
    any_flag = args.drafts or args.essays or args.evaluations
    do_drafts = args.drafts or (not any_flag)
    do_essays = args.essays or (not any_flag)
    do_evals = args.evaluations or (not any_flag)

    # Build latest maps
    draft_latest = _scan_stage_latest_by_task(data_root / "docs" / "draft") if do_drafts or do_essays else {}
    essay_latest = _scan_stage_latest_by_task(data_root / "docs" / "essay") if do_essays or do_evals else {}
    eval_latest = _scan_stage_latest_by_task(data_root / "docs" / "evaluation") if do_evals else {}

    # Update drafts
    if do_drafts:
        csv_path = data_root / "2_tasks" / "draft_generation_tasks.csv"
        rows, upd = (0, 0)
        if csv_path.exists():
            if args.dry_run:
                # simulate mapping application
                df = pd.read_csv(csv_path)
                keys = set(df.get("draft_task_id", []).astype(str))
                coverage = len([k for k in keys if k in draft_latest])
                print(f"[dry-run] drafts: {coverage}/{len(keys)} task_ids have matching docs; would update doc_id where missing ({csv_path}).")
            else:
                rows, upd = _update_csv_with_mapping(csv_path, "draft_task_id", "doc_id", draft_latest, force=args.force)
                print(f"drafts: updated {upd}/{rows} rows in {csv_path}")
        else:
            print(f"drafts: CSV not found: {csv_path}")

    # Update essays
    if do_essays:
        csv_path = data_root / "2_tasks" / "essay_generation_tasks.csv"
        rows, upd = (0, 0)
        if csv_path.exists():
            # First, optionally link essay_task_id -> doc_id (only if you want to point at existing essays)
            if args.dry_run:
                df = pd.read_csv(csv_path)
                keys = set(df.get("essay_task_id", []).astype(str))
                coverage = len([k for k in keys if k in essay_latest])
                print(f"[dry-run] essays: {coverage}/{len(keys)} task_ids have matching docs; would update doc_id where missing ({csv_path}).")
            else:
                rows, upd = _update_csv_with_mapping(csv_path, "essay_task_id", "doc_id", essay_latest, force=args.force)
                print(f"essays: updated doc_id for {upd}/{rows} rows in {csv_path}")
            # Then, fill parent_doc_id for essay tasks from the latest DRAFT docs by draft_task_id
            # Build mapping draft_task_id -> draft_doc_id
            draft_task_to_doc: Dict[str, str] = {k: v[0] for k, v in draft_latest.items()}
            df2 = pd.read_csv(csv_path)
            if "draft_task_id" in df2.columns:
                updated = 0
                if "parent_doc_id" not in df2.columns:
                    df2["parent_doc_id"] = ""
                def _fill_parent(row):
                    nonlocal updated
                    cur = str(row.get("parent_doc_id", ""))
                    if cur and not args.force and cur.lower() != "nan":
                        return cur
                    draft_tid = str(row.get("draft_task_id", ""))
                    val = draft_task_to_doc.get(draft_tid, "")
                    if val:
                        updated += 1
                        return val
                    return cur
                if args.dry_run:
                    # Show a summary of how many rows would get parent_doc_id filled
                    sim = df2.copy()
                    sim["_new_parent"] = sim.apply(_fill_parent, axis=1)
                    changed = (sim["_new_parent"].astype(str) != sim["parent_doc_id"].astype(str)).sum()
                    print(f"[dry-run] essays: would set parent_doc_id for ~{changed} rows via draft_task_id mapping")
                else:
                    df2["parent_doc_id"] = df2.apply(_fill_parent, axis=1)
                    df2.to_csv(csv_path, index=False)
                    print(f"essays: set parent_doc_id for {updated}/{len(df2)} rows in {csv_path} via draft_task_id mapping")
        else:
            print(f"essays: CSV not found: {csv_path}")

    # Update evaluations: link evaluation_task_id -> doc_id; fill parent_doc_id from eval metadata
    if do_evals:
        csv_path = data_root / "2_tasks" / "evaluation_tasks.csv"
        if csv_path.exists():
            # Link evaluation_task_id -> doc_id
            if args.dry_run:
                df = pd.read_csv(csv_path)
                keys = set(df.get("evaluation_task_id", []).astype(str))
                coverage = len([k for k in keys if k in eval_latest])
                print(f"[dry-run] evaluations: {coverage}/{len(keys)} task_ids have matching docs; would update doc_id where missing ({csv_path}).")
            else:
                rows, upd = _update_csv_with_mapping(csv_path, "evaluation_task_id", "doc_id", eval_latest, force=args.force)
                print(f"evaluations: updated doc_id for {upd}/{rows} rows in {csv_path}")

            # Fill parent_doc_id from evaluation metadata (doc_id -> parent_doc_id)
            parent_map: Dict[str, str] = {}
            eval_root = data_root / "docs" / "evaluation"
            if eval_root.exists():
                for d in eval_root.iterdir():
                    if not d.is_dir():
                        continue
                    mp = d / "metadata.json"
                    try:
                        if mp.exists():
                            m = json.loads(mp.read_text(encoding="utf-8")) or {}
                            pid = str(m.get("parent_doc_id") or "").strip()
                            if pid:
                                parent_map[d.name] = pid
                    except Exception:
                        continue
            if args.dry_run:
                print(f"[dry-run] evaluations: would fill parent_doc_id for existing evaluation doc_ids (coverage ~{len(parent_map)})")
            else:
                # Reuse helper by reading, applying mapping, and writing
                df = pd.read_csv(csv_path)
                if "parent_doc_id" not in df.columns:
                    df["parent_doc_id"] = ""
                updated = 0
                def _fill_eval_parent(row):
                    nonlocal updated
                    cur = str(row.get("parent_doc_id", ""))
                    if cur and not args.force and cur.lower() != "nan":
                        return cur
                    doc_id = str(row.get("doc_id", ""))
                    val = parent_map.get(doc_id, "")
                    if val:
                        updated += 1
                        return val
                    return cur
                df["parent_doc_id"] = df.apply(_fill_eval_parent, axis=1)
                df.to_csv(csv_path, index=False)
                print(f"evaluations: set parent_doc_id for {updated}/{len(df)} rows in {csv_path}")
        else:
            print(f"evaluations: CSV not found: {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
