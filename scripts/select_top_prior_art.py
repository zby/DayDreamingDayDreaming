#!/usr/bin/env python3
"""
Select top-N documents by prior-art scores (cross-experiment) and write
curated essay generation tasks to data/2_tasks/essay_generation_tasks.csv.

Defaults:
- TOP_N = 10
- Prior-art templates: gemini-prior-art-eval, gemini-prior-art-eval-v2 (use whichever are present; if both, take max per document)
- Parsed scores source: data/7_cross_experiment/parsed_scores.csv (required)
 - Output tasks CSV: data/2_tasks/essay_generation_tasks.csv (always)

Usage examples:
  uv run python scripts/select_top_prior_art.py \
    --parsed-scores data/7_cross_experiment/parsed_scores.csv

  # Override target template or N
  uv run python scripts/select_top_prior_art.py --top-n 25
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
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.parsed_scores.exists():
        print(f"ERROR: parsed_scores file not found: {args.parsed_scores}", file=sys.stderr)
        return 2
    df = pd.read_csv(args.parsed_scores)

    # Validate columns and derive document_id if necessary
    if df.empty or "evaluation_template" not in df.columns:
        print("ERROR: parsed_scores is empty or missing 'evaluation_template' column", file=sys.stderr)
        return 2
    if "document_id" not in df.columns and "evaluation_task_id" not in df.columns:
        print("ERROR: parsed_scores must include either 'document_id' or 'evaluation_task_id'", file=sys.stderr)
        return 2
    if "document_id" not in df.columns and "evaluation_task_id" in df.columns:
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
    best = (
        filt.groupby("document_id")["score"].max().reset_index().sort_values(
            by=["score", "document_id"], ascending=[False, True]
        )
    )

    top_docs = best.head(args.top_n)["document_id"].tolist()

    # Build curated essay_generation_tasks.csv
    models_csv = Path("data/1_raw/llm_models.csv")
    essay_dir = Path("data/3_generation/essay_responses")
    out_csv = Path("data/2_tasks/essay_generation_tasks.csv")

    # Load model mapping id -> provider/model name
    model_map = {}
    if models_csv.exists():
        try:
            mdf = pd.read_csv(models_csv)
            if {"id","model"}.issubset(mdf.columns):
                model_map = dict(zip(mdf["id"].astype(str), mdf["model"].astype(str)))
        except Exception as e:
            print(f"Warning: failed to read model mapping ({e}); will use model ids as names", file=sys.stderr)

    rows = []
    missing = []
    for doc in top_docs:
        parts = doc.split("_")
        if len(parts) < 4:
            print(f"Skipping malformed doc id (need >=4 parts): {doc}", file=sys.stderr)
            continue
        essay_template = parts[-1]
        generation_model = parts[-2]
        link_template = parts[-3]
        combo_id = "_".join(parts[:-3])
        link_task_id = f"{combo_id}_{link_template}_{generation_model}"
        fp = essay_dir / f"{doc}.txt"
        if not fp.exists():
            missing.append(str(fp))
        rows.append({
            "essay_task_id": doc,
            "link_task_id": link_task_id,
            "combo_id": combo_id,
            "link_template": link_template,
            "essay_template": essay_template,
            "generation_model": generation_model,
            "generation_model_name": model_map.get(generation_model, generation_model),
        })

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=[
        "essay_task_id","link_task_id","combo_id","link_template",
        "essay_template","generation_model","generation_model_name"
    ]).drop_duplicates(subset=["essay_task_id"]).to_csv(out_csv, index=False)

    print(f"Wrote {len(rows)} curated tasks to {out_csv}")
    if missing:
        print("Warning: missing essay files (evaluation will fail for these if run):", file=sys.stderr)
        for m in missing[:10]:
            print(" -", m, file=sys.stderr)
        if len(missing) > 10:
            print(f" ... {len(missing)-10} more", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
