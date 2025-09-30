#!/usr/bin/env python3
"""
Select top-N essays by evaluation score and write them to data/2_tasks/selected_essays.txt.

Supports multiple selection strategies:
- Top-N: Select the top N essays by average score
- Score span: Select all essays within X points of the top score
- Max only: Select all essays tied for the highest score

Usage examples:
  # Top 30 by novelty
  uv run python scripts/select_top_essays.py --template novelty --top-n 30

  # Top 25 by prior-art
  uv run python scripts/select_top_essays.py --template gemini-prior-art-eval --top-n 25

  # All essays within 0.5 points of top novelty-v2 score
  uv run python scripts/select_top_essays.py --template novelty-v2 --score-span 0.5

  # Only essays tied for maximum score
  uv run python scripts/select_top_essays.py --template novelty --max-only
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
        "--template",
        type=str,
        required=True,
        help="Evaluation template to rank by (e.g., novelty, novelty-v2, gemini-prior-art-eval)",
    )
    p.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top essays to select (default: 10)",
    )
    p.add_argument(
        "--score-span",
        type=float,
        default=None,
        help="Include all essays within this score distance from the top (overrides --top-n)",
    )
    p.add_argument(
        "--max-only",
        action="store_true",
        help="Select only essays tied for the highest score (overrides --top-n and --score-span)",
    )
    p.add_argument(
        "--aggregated-scores",
        type=Path,
        default=Path("data/7_cross_experiment/aggregated_scores.csv"),
        help="Path to aggregated_scores.csv (default: data/7_cross_experiment/aggregated_scores.csv)",
    )
    p.add_argument(
        "--average-models",
        type=str,
        nargs="*",
        default=["sonnet-4", "gemini_25_pro"],
        help="Evaluation models to average scores across (default: sonnet-4 gemini_25_pro)",
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
        print("ERROR: 'evaluation_template' not found in aggregated_scores", file=sys.stderr)
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

    # Filter to requested template
    filtered = valid[valid["evaluation_template"] == args.template].copy()
    if filtered.empty:
        print(f"ERROR: No scores found for template '{args.template}'", file=sys.stderr)
        return 1

    # Filter to requested evaluation models
    if "evaluation_llm_model" not in filtered.columns:
        print("ERROR: aggregated_scores must include 'evaluation_llm_model'", file=sys.stderr)
        return 2

    filtered = filtered[filtered["evaluation_llm_model"].isin(args.average_models)].copy()
    if filtered.empty:
        print(f"ERROR: No scores found for evaluation models: {args.average_models}", file=sys.stderr)
        return 1

    # Compute average score per essay (max score per model, then average across models)
    per_model = (
        filtered.groupby(["parent_gen_id", "evaluation_llm_model"])["score"]
        .max()
        .reset_index()
    )
    averaged = (
        per_model.groupby("parent_gen_id")["score"]
        .mean()
        .reset_index()
        .sort_values(by=["score", "parent_gen_id"], ascending=[False, True])
    )

    # Determine selection strategy
    if args.max_only:
        # Select all essays tied for maximum score
        max_score = averaged["score"].max()
        selected = averaged[averaged["score"] == max_score]
        selection_desc = f"all tied at score={max_score:.2f}"
    elif args.score_span is not None:
        # Select all essays within score_span of the top
        max_score = averaged["score"].max()
        threshold = max_score - args.score_span
        selected = averaged[averaged["score"] >= threshold]
        selection_desc = f"score >= {threshold:.2f} (within {args.score_span} of top)"
    else:
        # Select top-N
        selected = averaged.head(args.top_n)
        selection_desc = f"top {args.top_n}"

    top_essays = selected["parent_gen_id"].astype(str).tolist()

    if not top_essays:
        print(f"No essays found for template '{args.template}'")
        return 0

    # Write to data/2_tasks/selected_essays.txt
    out_path = Path("data/2_tasks/selected_essays.txt")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        f"# {selection_desc} by {args.template} (averaged across {', '.join(args.average_models)})",
    ] + top_essays + [""]

    content = "\n".join(lines)
    out_path.write_text(content, encoding="utf-8")

    print(f"Wrote {len(top_essays)} essay gen_ids ({selection_desc}) to {out_path}")
    print(f"Essays: {', '.join(top_essays[:5])}{'...' if len(top_essays) > 5 else ''}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
