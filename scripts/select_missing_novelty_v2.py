#!/usr/bin/env python3
"""
Select essays missing novelty-v2 evaluations from the top-N promising essays
and write them to a cohort's selected_essays.txt for evaluation-only mode.

Usage examples:
  uv run python scripts/select_missing_novelty_v2.py --cohort novelty_v2_backfill --top-n 30
  uv run python scripts/select_missing_novelty_v2.py --cohort my_cohort --top-n 50
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--cohort",
        type=str,
        required=True,
        help="Cohort name (will create data/cohorts/<cohort>/selected_essays.txt)",
    )
    p.add_argument(
        "--top-n",
        type=int,
        default=30,
        help="Number of top essays to consider from baseline ranking",
    )
    p.add_argument(
        "--aggregated-scores",
        type=Path,
        default=Path("data/7_cross_experiment/aggregated_scores.csv"),
        help="Path to aggregated_scores.csv (cross-cohort)",
    )
    p.add_argument(
        "--baseline-template",
        type=str,
        default="novelty",
        help="Baseline novelty template to rank by",
    )
    p.add_argument(
        "--target-template",
        type=str,
        default="novelty-v2",
        help="Target template to check for missing evaluations",
    )
    p.add_argument(
        "--average-models",
        type=str,
        nargs="*",
        default=["sonnet-4", "gemini_25_pro"],
        help="Models to average for baseline ranking",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.aggregated_scores.exists():
        print(f"ERROR: aggregated_scores file not found: {args.aggregated_scores}", file=sys.stderr)
        return 2

    df = pd.read_csv(args.aggregated_scores)

    # Validate required columns
    if df.empty:
        print("ERROR: aggregated_scores is empty", file=sys.stderr)
        return 2

    if "evaluation_template" not in df.columns:
        print(f"ERROR: 'evaluation_template' not found in aggregated_scores", file=sys.stderr)
        return 2

    if "parent_gen_id" not in df.columns:
        print("ERROR: aggregated_scores must include 'parent_gen_id'", file=sys.stderr)
        return 2

    # Filter to successful scores only
    cond_score = df["score"].notna() if "score" in df.columns else True
    cond_parent = df["parent_gen_id"].notna()
    valid = df[cond_score & cond_parent].copy()

    if valid.empty:
        print("ERROR: No valid scores found in aggregated_scores", file=sys.stderr)
        return 1

    # Calculate baseline ranking (avg of top scores per model, for baseline template)
    baseline = valid[valid["evaluation_template"] == args.baseline_template].copy()
    if baseline.empty:
        print(f"ERROR: No scores found for baseline template '{args.baseline_template}'", file=sys.stderr)
        return 1

    # Filter to requested evaluation models
    if "evaluation_llm_model" not in baseline.columns:
        print("ERROR: aggregated_scores must include 'evaluation_llm_model'", file=sys.stderr)
        return 2

    baseline = baseline[baseline["evaluation_llm_model"].isin(args.average_models)].copy()
    if baseline.empty:
        print(f"ERROR: No scores found for evaluation models: {args.average_models}", file=sys.stderr)
        return 1

    # Compute average baseline score per essay
    per_model_baseline = (
        baseline.groupby(["parent_gen_id", "evaluation_llm_model"])["score"]
        .max()
        .reset_index()
    )
    avg_baseline = (
        per_model_baseline.groupby("parent_gen_id")["score"]
        .mean()
        .reset_index()
        .rename(columns={"score": "baseline_avg_score"})
        .sort_values(by=["baseline_avg_score", "parent_gen_id"], ascending=[False, True])
    )

    # Take top-N by baseline
    top_n_essays = avg_baseline.head(args.top_n)["parent_gen_id"].tolist()

    # Check which ones are missing target template evaluations
    target = valid[valid["evaluation_template"] == args.target_template].copy()
    essays_with_target = set(target["parent_gen_id"].unique())

    missing = [eid for eid in top_n_essays if eid not in essays_with_target]

    if not missing:
        print(f"All top {args.top_n} essays already have {args.target_template} evaluations!")
        return 0

    # Write to data/2_tasks/selected_essays.txt with evaluation-only mode
    out_path = Path("data/2_tasks/selected_essays.txt")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# mode: evaluation-only",
        "# include-existing-evaluations",
    ] + missing + [""]

    content = "\n".join(lines)
    out_path.write_text(content, encoding="utf-8")

    print(f"Wrote {len(missing)} essay gen_ids missing {args.target_template} evaluations to {out_path}")
    print(f"Essays: {', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}")
    print(f"\nTo run cohort '{args.cohort}':")
    print(f"  1. Set environment: export DD_COHORT={args.cohort}")
    print(f"  2. Start Dagster UI: uv run dagster dev -f src/daydreaming_dagster/definitions.py")
    print(f"  3. Materialize cohort assets (group:cohort)")
    print(f"  4. Materialize evaluation assets (group:evaluation) - use backfill for all partitions")
    print(f"\nNote: The cohort will be written to data/cohorts/{args.cohort}/")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())