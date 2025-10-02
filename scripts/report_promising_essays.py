#!/usr/bin/env python3
"""Report high-potential essays using aggregated evaluation scores.

This script ranks essays by a baseline evaluation (default: novelty) and
compares coverage against an upgraded evaluation (default: novelty-v2).
It prints a summary table, highlights essays missing the target evaluation,
and optionally writes the merged results to a CSV for downstream use.

Example usage:
  uv run python scripts/report_promising_essays.py --top-n 20 --score-span 0.5 \
      --cohort-id cohort-abc --output-csv data/cohorts/cohort-abc/reports/summary/promising_essays_novelty.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Iterable

import pandas as pd

from daydreaming_dagster.data_layer.paths import Paths, COHORT_REPORT_ASSET_TARGETS

from daydreaming_dagster.utils.evaluation_scores import (
    parent_ids_missing_template,
    rank_parent_essays_by_template,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--aggregated-scores",
        type=Path,
        default=Path("data/7_cross_experiment/aggregated_scores.csv"),
        help="Path to aggregated_scores.csv (defaults to data/7_cross_experiment/aggregated_scores.csv)",
    )
    parser.add_argument(
        "--cohort-id",
        type=str,
        default=None,
        help="If provided, read aggregated scores from data/cohorts/<id>/reports/parsing/aggregated_scores.csv",
    )
    parser.add_argument(
        "--baseline-template",
        type=str,
        default="novelty",
        help="Evaluation template used to rank essays (default: novelty)",
    )
    parser.add_argument(
        "--target-template",
        type=str,
        default="novelty-v2",
        help="Evaluation template that should cover the baseline winners (default: novelty-v2)",
    )
    parser.add_argument(
        "--evaluation-model",
        dest="evaluation_models",
        action="append",
        default=None,
        help="Restrict to these evaluation models (repeat flag for multiple). Defaults to all models present",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Keep only the first N essays after ranking (applied after score-span filtering)",
    )
    parser.add_argument(
        "--score-span",
        type=float,
        default=None,
        help="Keep essays whose baseline score is within this distance from the top score",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional path to write the merged summary as CSV",
    )
    return parser.parse_args()


def _renamed(df: pd.DataFrame, prefix: str, *, columns: Iterable[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    cols = set(df.columns if columns is None else columns)
    mapping = {col: f"{prefix}{col}" for col in cols if col != "parent_gen_id"}
    return df.rename(columns=mapping)


def _load_scores(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"aggregated_scores file not found: {path}")
    return pd.read_csv(path)


def main() -> int:
    args = parse_args()

    aggregated_path = args.aggregated_scores
    if args.cohort_id:
        cohort_id = args.cohort_id.strip()
        if not cohort_id:
            print("ERROR: --cohort-id requires a non-empty value", file=sys.stderr)
            return 2
        paths = Paths.from_str("data")
        _, filename = COHORT_REPORT_ASSET_TARGETS["cohort_aggregated_scores"]
        aggregated_path = paths.cohort_parsing_csv(cohort_id, filename)

    try:
        df = _load_scores(aggregated_path)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    baseline = rank_parent_essays_by_template(
        df,
        template_id=args.baseline_template,
        evaluation_models=args.evaluation_models,
        score_span=args.score_span,
        top_n=args.top_n,
    )

    if baseline.empty:
        print(
            "No rows found for the requested baseline template."
            f" Template: {args.baseline_template}",
            file=sys.stderr,
        )
        return 1

    candidates = baseline["parent_gen_id"].tolist()
    missing = parent_ids_missing_template(
        df,
        candidates,
        template_id=args.target_template,
    )

    target_full = rank_parent_essays_by_template(
        df,
        template_id=args.target_template,
        evaluation_models=args.evaluation_models,
    )
    target_subset = target_full[target_full["parent_gen_id"].isin(candidates)].copy()

    summary = _renamed(baseline, prefix="baseline_")
    summary = summary.merge(
        _renamed(target_subset, prefix="target_"),
        on="parent_gen_id",
        how="left",
    )
    summary["target_missing"] = summary["parent_gen_id"].isin(missing)

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print("Baseline ranking (top rows):")
        print(summary.head(len(summary)))

    if missing:
        print("\nEssays missing target evaluation:")
        for pid in missing:
            print(f"  - {pid}")
    else:
        print("\nAll baseline essays already have the target evaluation.")

    if args.output_csv:
        summary.to_csv(args.output_csv, index=False)
        print(f"\nWrote summary to {args.output_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
