#!/usr/bin/env python3
"""
Migrate generated CSV tables to draft_* schema without touching templates.

What it does (non-destructive additions):
- Adds 'draft_template' if missing (copied from 'link_template' when present).
- Adds 'draft_task_id' if missing (copied from 'link_task_id' when present).

Targets (if present):
- data/5_parsing/parsed_scores.csv
- data/7_cross_experiment/parsed_scores.csv
- data/7_cross_experiment/evaluation_results.csv
- data/6_summary/generation_scores_pivot.csv
- data/6_summary/final_results.csv

Usage:
  python scripts/migrate_generated_tables_to_draft_schema.py [--dry-run]

Notes:
- This script does not modify any template files under data/1_raw.
- It preserves legacy link_* columns; removal can happen in a later cleanup.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


def migrate_df(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Apply in-memory migration rules and return (new_df, changes).

    Non-destructive: only adds missing draft_* columns derived from link_*.
    """
    changes: list[str] = []
    out = df

    if "draft_template" not in out.columns and "link_template" in out.columns:
        out = out.copy()
        out["draft_template"] = out["link_template"]
        changes.append("added draft_template from link_template")

    if "draft_task_id" not in out.columns and "link_task_id" in out.columns:
        out = out.copy()
        out["draft_task_id"] = out["link_task_id"]
        changes.append("added draft_task_id from link_task_id")

    return out, changes


def migrate_file(path: Path, dry_run: bool = False) -> tuple[bool, list[str]]:
    if not path.exists():
        return False, []
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"❌ Failed to read {path}: {e}")
        return False, []

    new_df, changes = migrate_df(df)
    if not changes:
        print(f"= {path}: no changes needed")
        return False, []

    if dry_run:
        print(f"~ {path}: would apply -> {', '.join(changes)}")
        return True, changes

    try:
        new_df.to_csv(path, index=False)
        print(f"✓ {path}: applied -> {', '.join(changes)}")
        return True, changes
    except Exception as e:
        print(f"❌ Failed to write {path}: {e}")
        return False, []


def main():
    parser = argparse.ArgumentParser(description="Migrate generated tables to draft_* schema")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing files")
    args = parser.parse_args()

    roots = [
        Path("data/5_parsing/parsed_scores.csv"),
        Path("data/7_cross_experiment/parsed_scores.csv"),
        Path("data/7_cross_experiment/evaluation_results.csv"),
        Path("data/6_summary/generation_scores_pivot.csv"),
        Path("data/6_summary/final_results.csv"),
    ]

    any_changes = False
    for p in roots:
        changed, _ = migrate_file(p, dry_run=args.dry_run)
        any_changes = any_changes or changed

    if args.dry_run:
        print("\nDry-run complete.")
    else:
        print("\nMigration complete.")

    sys.exit(0)


if __name__ == "__main__":
    main()

