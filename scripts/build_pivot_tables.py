#!/usr/bin/env python3
"""
Build cross-experiment pivot table of essay scores by evaluation template+model.

Inputs:
- Parsed scores CSV (typically produced by scripts/parse_all_scores.py)

Output (written under data/7_cross_experiment/):
- evaluation_scores_by_template_model.csv
  Rows: essay_task_id (+ key metadata)
  Columns: evaluation_template__evaluation_model
  Values: score (first if duplicates)

Usage:
  python scripts/build_pivot_tables.py \
    --parsed-scores data/7_cross_experiment/parsed_scores.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def build_pivot(parsed_scores: Path, out_dir: Path) -> None:
    if not parsed_scores.exists():
        raise FileNotFoundError(f"Parsed scores CSV not found: {parsed_scores}")

    df = pd.read_csv(parsed_scores)

    # Ensure required columns exist
    required = [
        "essay_task_id",
        "combo_id",
        "link_template",
        "generation_template",
        "generation_model",
        "evaluation_template",
        "evaluation_model",
        "score",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in parsed scores: {missing}")

    # Compose column key as template__model
    df["evaluation_template_model"] = df["evaluation_template"].astype(str) + "__" + df["evaluation_model"].astype(str)
    # Deterministic order before pivot
    df_sorted = df.sort_values(["essay_task_id", "evaluation_template_model"])  # deterministic

    meta_cols = [
        "essay_task_id",
        "combo_id",
        "link_template",
        "generation_template",
        "generation_model",
    ]

    # Create pivot by evaluation_template__evaluation_model
    pivot = (
        df_sorted
        .pivot_table(
            index=meta_cols,
            columns="evaluation_template_model",
            values="score",
            aggfunc="first",
        )
        .reset_index()
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "evaluation_scores_by_template_model.csv"
    pivot.to_csv(out_file, index=False)
    print(f"Wrote pivot: {out_file} ({len(pivot)} rows, {len(pivot.columns)} cols)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build evaluation pivots from parsed scores")
    parser.add_argument(
        "--parsed-scores",
        type=Path,
        required=True,
        help="Path to parsed scores CSV (from scripts/parse_all_scores.py)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/7_cross_experiment"),
        help="Directory to write pivot CSVs (default: data/7_cross_experiment)",
    )
    args = parser.parse_args()

    build_pivot(parsed_scores=args.parsed_scores, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
