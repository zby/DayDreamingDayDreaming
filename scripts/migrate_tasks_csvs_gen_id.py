#!/usr/bin/env python3
"""
Migrate task CSVs to unified gen-id schema:
- data/2_tasks/essay_generation_tasks.csv: rename column 'essay_gen_id' -> 'gen_id' (if present)
- data/2_tasks/evaluation_tasks.csv: drop column 'draft_gen_id' (if present)

Writes files in-place with atomic replace and prints a short report.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def migrate(data_root: Path) -> None:
    two_tasks = data_root / "2_tasks"
    essays = two_tasks / "essay_generation_tasks.csv"
    evals = two_tasks / "evaluation_tasks.csv"
    if essays.exists():
        df = pd.read_csv(essays)
        changed = False
        if "essay_gen_id" in df.columns and "gen_id" not in df.columns:
            df = df.rename(columns={"essay_gen_id": "gen_id"})
            changed = True
        if changed:
            atomic_write_csv(df, essays)
            print(f"Updated {essays} (renamed essay_gen_id -> gen_id)")
        else:
            print(f"No change: {essays}")
    else:
        print(f"Missing: {essays}")

    if evals.exists():
        df = pd.read_csv(evals)
        if "draft_gen_id" in df.columns:
            df = df.drop(columns=["draft_gen_id"], errors="ignore")
            atomic_write_csv(df, evals)
            print(f"Updated {evals} (dropped draft_gen_id)")
        else:
            print(f"No change: {evals}")
    else:
        print(f"Missing: {evals}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate task CSVs to gen-id unified schema")
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    args = ap.parse_args()
    migrate(args.data_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

