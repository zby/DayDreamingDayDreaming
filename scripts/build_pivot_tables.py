#!/usr/bin/env python3
"""
Build cross-experiment pivot table of essay scores by evaluation template+model.

Inputs:
- Parsed scores CSV (typically produced by scripts/aggregate_scores.py)

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


def _compose_essay_task_id(df: pd.DataFrame) -> pd.Series:
    """Derive the essay_task_id using combo/template/model columns from the new schema."""

    required = ["combo_id", "draft_template", "generation_model", "generation_template"]
    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols:
        raise ValueError(
            "Unable to derive essay_task_id; missing required columns: "
            f"{missing_cols}"
        )

    def compose(row: pd.Series) -> str:
        parts: list[str] = []
        for col in required:
            value = row[col]
            if pd.isna(value):
                raise ValueError(
                    "Unable to derive essay_task_id due to NaN in column "
                    f"'{col}' for row index {row.name}"
                )
            text = str(value).strip()
            if not text:
                raise ValueError(
                    "Unable to derive essay_task_id due to empty value in column "
                    f"'{col}' for row index {row.name}"
                )
            parts.append(text)
        return "_".join(parts)

    return df.apply(compose, axis=1)


def build_pivot(parsed_scores: Path, out_dir: Path) -> None:
    if not parsed_scores.exists():
        raise FileNotFoundError(f"Parsed scores CSV not found: {parsed_scores}")

    df = pd.read_csv(parsed_scores)

    task_cols = ["combo_id", "draft_template", "generation_template", "generation_model"]
    missing_task_cols = [col for col in task_cols if col not in df.columns]
    if missing_task_cols:
        raise ValueError(
            "Parsed scores missing required task metadata columns: "
            f"{missing_task_cols}"
        )

    missing_mask = df[task_cols].isna().any(axis=1)
    empty_mask = df[task_cols].astype(str).apply(lambda col: col.str.strip() == "").any(axis=1)
    drop_mask = missing_mask | empty_mask
    if drop_mask.any():
        dropped = int(drop_mask.sum())
        drop_details = df.loc[drop_mask, task_cols + ["gen_id", "parent_gen_id", "evaluation_template", "evaluation_llm_model"]]
        print(
            "Dropping rows without complete task metadata:\n"
            f"Total dropped: {dropped}\n"
            f"Missing columns breakdown:\n{drop_details.head(20).to_markdown(index=False)}"
        )
        df = df.loc[~drop_mask].copy()
    if df.empty:
        raise ValueError("No rows with complete task metadata available for pivot")

    # Ensure required columns exist (canonical uses draft_template; support legacy link_template)
    # Accept either 'template_id' (new) or 'evaluation_template' (legacy)
    required_base = [
        "essay_task_id",
        "combo_id",
        "generation_template",
        "generation_model",
        "score",
    ]
    df["essay_task_id"] = _compose_essay_task_id(df)

    missing = [c for c in required_base if c not in df.columns]
    if missing:
        raise ValueError(
            "Missing columns in parsed scores: "
            f"{missing}; required base columns: {required_base}"
        )
    # Normalize template column name
    if "evaluation_template" not in df.columns:
        raise ValueError("Missing required column 'evaluation_template' in parsed scores")

    # Backward-compat: if draft_template missing, populate from link_template when present
    if "draft_template" not in df.columns:
        if "link_template" in df.columns:
            df["draft_template"] = df["link_template"]
        else:
            # Keep a column for downstream selection even if empty
            df["draft_template"] = None

    # Compose column key as template__evaluator (strict: require evaluation_llm_model)
    if "evaluation_llm_model" not in df.columns:
        raise ValueError("Missing required column 'evaluation_llm_model' in parsed scores")
    df["evaluation_template_model"] = (
        df["evaluation_template"].astype(str) + "__" + df["evaluation_llm_model"].astype(str)
    )
    all_combo_columns = sorted(df["evaluation_template_model"].unique())

    df_sorted = df.sort_values(["essay_task_id", "evaluation_template_model"])  # deterministic

    meta_columns = {
        "parent_gen_id": "first",
        "combo_id": "first",
        "draft_template": "first",
        "generation_template": "first",
        "generation_model": "first",
        "cohort_id": "first",
    }
    available_meta = {col: agg for col, agg in meta_columns.items() if col in df_sorted.columns}
    meta_df = (
        df_sorted[["essay_task_id", *available_meta.keys()]]
        .groupby("essay_task_id", as_index=False)
        .agg(available_meta)
    )

    pivot = (
        df_sorted
        .pivot_table(
            index="essay_task_id",
            columns="evaluation_template_model",
            values="score",
            aggfunc="first",
        )
        .reset_index()
    )

    for combo in all_combo_columns:
        if combo not in pivot.columns:
            pivot[combo] = pd.NA

    result = meta_df.merge(pivot, on="essay_task_id", how="outer")

    meta_output_cols = [
        col for col in ["parent_gen_id", "combo_id", "draft_template", "generation_template", "generation_model", "cohort_id"]
        if col in result.columns
    ]
    ordered_columns = meta_output_cols + [col for col in all_combo_columns if col in result.columns]
    pivot = result.reindex(columns=ordered_columns)

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
        help="Path to parsed scores CSV (from scripts/aggregate_scores.py)",
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
