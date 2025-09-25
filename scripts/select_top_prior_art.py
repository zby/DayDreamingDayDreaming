#!/usr/bin/env python3
"""
Select the strongest essays by prior-art scores (cross-experiment) and write their
essay gen_ids to `data/2_tasks/selected_essays.txt` (one per line).

Defaults:
- TOP_N = 10
- `--score-span 0.5` keeps every essay whose averaged score is within 0.5 points of the top.
- `--max-only` is a shorthand for `--score-span 0` (only exact ties).
- Prior-art template default: gemini-prior-art-eval (add others via `--prior-art-templates` if desired)
- Parsed scores source: data/7_cross_experiment/parsed_scores.csv (required)

Usage examples:
  uv run python scripts/select_top_prior_art.py \
    --parsed-scores data/7_cross_experiment/parsed_scores.csv --top-n 25
  uv run python scripts/select_top_prior_art.py --max-only
  uv run python scripts/select_top_prior_art.py --score-span 0.5
"""

from __future__ import annotations

import argparse
from pathlib import Path
import json
import sys
import pandas as pd
# No external lookups required beyond gens metadata for selected records.


# Default to the original prior-art template; v2 can be added explicitly via CLI.
DEFAULT_PRIOR_ART_TEMPLATES = ["gemini-prior-art-eval"]
DEFAULT_AVERAGE_MODELS = ["sonnet-4", "gemini_25_pro"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parsed-scores", type=Path, default=Path("data/7_cross_experiment/parsed_scores.csv"), help="Path to cross-experiment parsed_scores.csv")
    p.add_argument("--top-n", type=int, default=10, help="Number of top documents to select")
    p.add_argument(
        "--max-only",
        action="store_true",
        help="Select all essays tied for the highest prior-art score (ignores --top-n)",
    )
    p.add_argument(
        "--prior-art-templates",
        type=str,
        nargs="*",
        default=DEFAULT_PRIOR_ART_TEMPLATES,
        help="Prior-art templates to consider (use whichever are present)",
    )
    p.add_argument(
        "--average-models",
        type=str,
        nargs="*",
        default=DEFAULT_AVERAGE_MODELS,
        help="Evaluation models whose scores should be averaged per essay",
    )
    p.add_argument(
        "--score-span",
        type=float,
        default=0.5,
        help="Include essays within this score distance from the top average (set <=0 to disable)",
    )
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
    cond_err = df["error"].isna() if "error" in df.columns else True
    cond_score = df["score"].notna() if "score" in df.columns else True
    cond_parent = df["parent_gen_id"].notna()
    filt = df[cond_tpl & cond_err & cond_score & cond_parent].copy()

    if "evaluation_llm_model" not in filt.columns:
        print("ERROR: parsed_scores must include 'evaluation_llm_model' column", file=sys.stderr)
        return 2

    allowed_models = set(args.average_models)
    if not allowed_models:
        print("ERROR: --average-models must include at least one evaluation model", file=sys.stderr)
        return 2

    filt = filt[filt["evaluation_llm_model"].isin(allowed_models)].copy()
    if filt.empty:
        print(
            "No successful prior-art scores found for the requested evaluation models.",
            file=sys.stderr,
        )
        return 1

    key_col = "parent_gen_id"
    per_model = (
        filt.groupby([key_col, "evaluation_llm_model"])["score"].max().reset_index()
    )
    averaged = (
        per_model.groupby(key_col)["score"].mean().reset_index().sort_values(
            by=["score", key_col], ascending=[False, True]
        )
    )
    score_span = args.score_span if args.score_span is not None else -1.0
    if args.max_only:
        score_span = 0.0

    max_score = averaged["score"].max()
    if pd.isna(max_score):
        print("ERROR: Unable to compute maximum prior-art score.", file=sys.stderr)
        return 1

    selected = averaged.copy()
    if score_span >= 0:
        threshold = max_score - score_span
        selected = selected[selected["score"] >= threshold]
    if score_span < 0:
        threshold = None

    limit_results = (score_span < 0) or (score_span == 0 and not args.max_only)
    if limit_results and args.top_n:
        selected = selected.head(args.top_n)

    top_docs = selected[key_col].astype(str).tolist()
    if not top_docs:
        print("ERROR: No candidates found after filtering and grouping by parent_gen_id.", file=sys.stderr)
        print(f"Filters: templates={available}, top_n={args.top_n}", file=sys.stderr)
        return 1

    # Write selected essay gen_ids (one per line)
    out_path = Path("data/2_tasks/selected_essays.txt")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(top_docs) + "\n"
    out_path.write_text(content, encoding="utf-8")
    if score_span >= 0:
        thresh_display = threshold if threshold is not None else max_score
        print(
            f"Wrote {len(top_docs)} essay gen_ids (score >= {thresh_display:.2f}) to {out_path}"
        )
    elif args.max_only:
        pivot_score = selected["score"].iloc[0] if not selected.empty else "?"
        print(
            f"Wrote {len(top_docs)} essay gen_ids (all tied at score={pivot_score}) to {out_path}"
        )
    else:
        print(f"Wrote {len(top_docs)} essay gen_ids to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
