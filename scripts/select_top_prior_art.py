#!/usr/bin/env python3
"""
Select top-N documents by prior-art scores and emit evaluation partition keys.

Defaults:
- TOP_N = 10
- Prior-art templates: gemini-prior-art-eval, gemini-prior-art-eval-v2 (use whichever are present; if both, take max per document)
- Target evaluation template: novelty
- Evaluation model id: sonnet-4

Usage examples:
  uv run python scripts/select_top_prior_art.py \
    --parsed-scores data/5_parsing/parsed_scores.csv \
    --out reports/novelty_partitions.txt

  # Override target template or N
  uv run python scripts/select_top_prior_art.py --top-n 25 --target-template novelty
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
    p.add_argument("--parsed-scores", type=Path, default=Path("data/5_parsing/parsed_scores.csv"), help="Path to parsed_scores.csv")
    p.add_argument("--top-n", type=int, default=10, help="Number of top documents to select")
    p.add_argument("--target-template", type=str, default="novelty", help="Target evaluation template to run (e.g., novelty)")
    p.add_argument("--eval-model-id", type=str, default="sonnet-4", help="Evaluation model id to use (must exist in llm_models.csv)")
    p.add_argument("--prior-art-templates", type=str, nargs="*", default=DEFAULT_PRIOR_ART_TEMPLATES, help="Prior-art templates to consider (use whichever are present)")
    p.add_argument("--out", type=Path, default=None, help="Optional output file to write partition keys")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.parsed_scores.exists():
        print(f"ERROR: parsed_scores file not found: {args.parsed_scores}", file=sys.stderr)
        return 2

    df = pd.read_csv(args.parsed_scores)
    if df.empty or "document_id" not in df.columns or "evaluation_template" not in df.columns:
        print("ERROR: parsed_scores is empty or missing required columns (document_id, evaluation_template)", file=sys.stderr)
        return 2

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
    ].copy()
    if filt.empty:
        print("No successful prior-art scores found to select from.", file=sys.stderr)
        return 1

    best = (
        filt.groupby("document_id")["score"].max().reset_index().sort_values(
            by=["score", "document_id"], ascending=[False, True]
        )
    )
    top_docs = best.head(args.top_n)["document_id"].tolist()

    keys = [f"{doc_id}__{args.target_template}__{args.eval_model_id}" for doc_id in top_docs]

    # Print to stdout
    for k in keys:
        print(k)

    # Optionally write to file
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text("\n".join(keys) + "\n", encoding="utf-8")

    # Summary to stderr
    print(
        f"Selected {len(keys)} docs from {len(available)} prior-art templates; target='{args.target_template}', model='{args.eval_model_id}'",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

