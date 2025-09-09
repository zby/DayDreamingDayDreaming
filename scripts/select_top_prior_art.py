#!/usr/bin/env python3
"""
Select top-N essays by prior-art scores (cross-experiment) and write their
essay gen_ids to data/2_tasks/selected_essays.txt (one per line).

Defaults:
- TOP_N = 10
- Prior-art templates: gemini-prior-art-eval, gemini-prior-art-eval-v2 (use whichever are present; if both, take max per generation)
- Parsed scores source: data/7_cross_experiment/parsed_scores.csv (required)

Usage examples:
  uv run python scripts/select_top_prior_art.py \
    --parsed-scores data/7_cross_experiment/parsed_scores.csv --top-n 25
"""

from __future__ import annotations

import argparse
from pathlib import Path
import json
import sys
import pandas as pd
# No external lookups required beyond gens metadata for selected records.


DEFAULT_PRIOR_ART_TEMPLATES = [
    "gemini-prior-art-eval",
    "gemini-prior-art-eval-v2",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parsed-scores", type=Path, default=Path("data/7_cross_experiment/parsed_scores.csv"), help="Path to cross-experiment parsed_scores.csv")
    p.add_argument("--top-n", type=int, default=10, help="Number of top documents to select")
    p.add_argument("--prior-art-templates", type=str, nargs="*", default=DEFAULT_PRIOR_ART_TEMPLATES, help="Prior-art templates to consider (use whichever are present)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.parsed_scores.exists():
        print(f"ERROR: parsed_scores file not found: {args.parsed_scores}", file=sys.stderr)
        return 2
    df = pd.read_csv(args.parsed_scores)

    # Validate required columns (gen-id-first)
    if df.empty or ("template_id" not in df.columns and "evaluation_template" not in df.columns):
        print("ERROR: parsed_scores is empty or missing 'template_id' column", file=sys.stderr)
        return 2
    if "parent_gen_id" not in df.columns:
        print("ERROR: parsed_scores must include 'parent_gen_id' for grouping (gen-id-first)", file=sys.stderr)
        return 2

    # Filter to available prior-art templates and successful scores
    tpl_col = "template_id" if "template_id" in df.columns else "evaluation_template"
    available = [tpl for tpl in args.prior_art_templates if tpl in set(df[tpl_col].unique())]
    if not available:
        print(
            f"No prior-art templates found in parsed_scores. Looked for any of: {args.prior_art_templates}",
            file=sys.stderr,
        )
        return 1
    cond_tpl = df[tpl_col].isin(available)
    cond_err = (df["error"].isna() if "error" in df.columns else True)
    cond_score = (df["score"].notna() if "score" in df.columns else True)
    cond_parent = df["parent_gen_id"].notna()
    filt = df[cond_tpl & cond_err & cond_score & cond_parent].copy()

    # Exclude evaluations run by Gemini models (empirically noisy for prior-art)
    if "evaluation_model" in filt.columns:
        id_col = filt["evaluation_model"]
    elif "model" in filt.columns:
        id_col = filt["model"]
    else:
        print("ERROR: parsed_scores must include 'evaluation_model' or 'model'", file=sys.stderr)
        return 2
    id_is_gemini = id_col.fillna("").str.contains("gemini", case=False)
    mask = ~id_is_gemini
    filt = filt[mask]
    if filt.empty:
        print("No successful prior-art scores found to select from.", file=sys.stderr)
        return 1
    # Group by parent_gen_id (essay generation)
    key_col = "parent_gen_id"
    best = (
        filt.groupby(key_col)["score"].max().reset_index().sort_values(
            by=["score", key_col], ascending=[False, True]
        )
    )
    top_docs = best.head(args.top_n)[key_col].astype(str).tolist()
    if not top_docs:
        print("ERROR: No candidates found after filtering and grouping by parent_gen_id.", file=sys.stderr)
        print(f"Filters: templates={available}, top_n={args.top_n}", file=sys.stderr)
        return 1

    # Write selected essay gen_ids (one per line)
    out_path = Path("data/2_tasks/selected_essays.txt")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(top_docs) + "\n"
    out_path.write_text(content, encoding="utf-8")
    print(f"Wrote {len(top_docs)} essay gen_ids to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
