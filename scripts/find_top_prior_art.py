#!/usr/bin/env python3
"""
Find top-N generations by prior-art scores and write a simple list that can be
edited manually and passed to the partition-registration script.

Outputs (by default):
- data/2_tasks/selected_generations.txt  (one document_id per line)
- Optional: CSV with document_id + score (+ template) via --csv-output

A "document_id" is either an essay_task_id (two-phase essay) or a draft_task_id
(two-phase draft). The downstream script accepts this list and writes curated
task CSVs and registers dynamic partitions in Dagster.

Usage examples:
  uv run python scripts/find_top_prior_art.py \
    --parsed-scores data/7_cross_experiment/parsed_scores.csv --top-n 25

  # Write both text and CSV
  uv run python scripts/find_top_prior_art.py --csv-output data/2_tasks/selected_generations.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


DEFAULT_PRIOR_ART_TEMPLATES = [
    "gemini-prior-art-eval",
    "gemini-prior-art-eval-v2",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parsed-scores", type=Path, default=Path("data/7_cross_experiment/parsed_scores.csv"), help="Path to cross-experiment parsed_scores.csv")
    p.add_argument("--top-n", type=int, default=10, help="Number of top documents to select")
    p.add_argument("--prior-art-templates", type=str, nargs="*", default=DEFAULT_PRIOR_ART_TEMPLATES, help="Prior-art templates to consider (use whichever are present)")
    p.add_argument("--output", type=Path, default=Path("data/2_tasks/selected_generations.txt"), help="Output path for line-delimited document_id list")
    p.add_argument("--csv-output", type=Path, default=None, help="Optional CSV output with document_id and score")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.parsed_scores.exists():
        print(f"ERROR: parsed_scores file not found: {args.parsed_scores}", file=sys.stderr)
        return 2

    df = pd.read_csv(args.parsed_scores)
    if df.empty or "evaluation_template" not in df.columns:
        print("ERROR: parsed_scores is empty or missing 'evaluation_template' column", file=sys.stderr)
        return 2

    # Ensure document_id column exists
    if "document_id" not in df.columns and "evaluation_task_id" not in df.columns:
        print("ERROR: parsed_scores must include either 'document_id' or 'evaluation_task_id'", file=sys.stderr)
        return 2
    if "document_id" not in df.columns:
        def _doc_from_tid(tid: str) -> str | None:
            if not isinstance(tid, str):
                return None
            parts = tid.split("__")
            return parts[0] if len(parts) == 3 else None
        df = df.copy()
        df["document_id"] = df["evaluation_task_id"].map(_doc_from_tid)

    # Filter to available prior-art templates and successful scores
    available = [tpl for tpl in args.prior_art_templates if tpl in set(df["evaluation_template"].unique())]
    if not available:
        print(
            f"No prior-art templates found in parsed_scores. Looked for any of: {args.prior_art_templates}",
            file=sys.stderr,
        )
        return 1
    filt = df[
        df["evaluation_template"].isin(available)
        & df.get("error").isna()
        & df.get("score").notna()
        & df["document_id"].notna()
    ].copy()
    if filt.empty:
        print("No successful prior-art scores found to select from.", file=sys.stderr)
        return 1

    # Get best score per document_id
    best = (
        filt.sort_values(["document_id", "score"], ascending=[True, False])
        .groupby("document_id", as_index=False).first()[["document_id", "score", "evaluation_template"]]
        .sort_values(["score", "document_id"], ascending=[False, True])
    )
    top = best.head(args.top_n)

    # Ensure output directory
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Write simple line-delimited list
    with args.output.open("w", encoding="utf-8") as f:
        for doc in top["document_id"].tolist():
            f.write(doc + "\n")
    print(f"Wrote {len(top)} document_ids to {args.output}")

    if args.csv_output:
        args.csv_output.parent.mkdir(parents=True, exist_ok=True)
        top.to_csv(args.csv_output, index=False)
        print(f"Wrote CSV with scores to {args.csv_output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

