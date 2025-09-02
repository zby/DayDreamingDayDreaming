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

    # Build curated generation task CSVs (essays and drafts)
    models_csv = Path("data/1_raw/llm_models.csv")
    essay_dir = Path("data/3_generation/essay_responses")
    links_dir = Path("data/3_generation/links_responses")
    essay_out_csv = Path("data/2_tasks/essay_generation_tasks.csv")
    link_out_csv = Path("data/2_tasks/link_generation_tasks.csv")
    link_tpl_csv = Path("data/1_raw/link_templates.csv")
    essay_tpl_csv = Path("data/1_raw/essay_templates.csv")

    # Load model mapping id -> provider/model name
    model_map = {}
    if models_csv.exists():
        try:
            mdf = pd.read_csv(models_csv)
            if {"id","model"}.issubset(mdf.columns):
                model_map = dict(zip(mdf["id"].astype(str), mdf["model"].astype(str)))
        except Exception as e:
            print(f"Warning: failed to read model mapping ({e}); will use model ids as names", file=sys.stderr)

    # Load known templates to parse IDs robustly
    link_tpls: set[str] = set()
    essay_tpls: set[str] = set()
    try:
        if link_tpl_csv.exists():
            ldf = pd.read_csv(link_tpl_csv)
            if "template_id" in ldf.columns:
                link_tpls = set(ldf["template_id"].astype(str))
        if essay_tpl_csv.exists():
            edf = pd.read_csv(essay_tpl_csv)
            if "template_id" in edf.columns:
                essay_tpls = set(edf["template_id"].astype(str))
    except Exception as e:
        print(f"Warning: failed to read template CSVs ({e}); falling back to naive parsing", file=sys.stderr)

    essay_rows = []
    link_rows = []
    missing_essays = []
    missing_links = []

    for doc in top_docs:
        parts = doc.split("_")

        # Try to detect essay vs draft by locating an essay template token
        essay_template = None
        link_template = None
        generation_model = None
        combo_id = None

        e_idx = None
        if essay_tpls:
            for i in range(len(parts)-1, -1, -1):
                if parts[i] in essay_tpls:
                    essay_template = parts[i]
                    e_idx = i
                    break
        if e_idx is not None:
            # Essay doc id: find last link template before essay
            l_idx = None
            if link_tpls:
                for j in range(e_idx - 1, -1, -1):
                    if parts[j] in link_tpls:
                        link_template = parts[j]
                        l_idx = j
                        break
            if l_idx is None and len(parts) >= 4:
                # Fallback naive positions
                link_template = parts[-3]
                l_idx = len(parts) - 3
            gen_tokens = parts[(l_idx + 1) if l_idx is not None else 0 : e_idx]
            generation_model = "_".join(gen_tokens) if gen_tokens else None
            combo_id = "_".join(parts[: (l_idx if l_idx is not None else 0)]) or None

            if not (combo_id and link_template and generation_model and essay_template):
                print(f"Skipping malformed essay id: {doc}", file=sys.stderr)
                continue

            link_task_id = f"{combo_id}_{link_template}_{generation_model}"
            fp = essay_dir / f"{doc}.txt"
            if not fp.exists():
                missing_essays.append(str(fp))
            essay_rows.append({
                "essay_task_id": doc,
                "link_task_id": link_task_id,
                "combo_id": combo_id,
                "link_template": link_template,
                "essay_template": essay_template,
                "generation_model": generation_model,
                "generation_model_name": model_map.get(generation_model, generation_model),
            })
        else:
            # Draft doc id: locate link template and model
            l_idx = None
            if link_tpls:
                for i in range(len(parts)-1, -1, -1):
                    if parts[i] in link_tpls:
                        l_idx = i
                        link_template = parts[i]
                        break
            if l_idx is None:
                # Fallback naive for drafts: expect combo + link_template + model
                if len(parts) >= 3:
                    link_template = parts[-2]
                    generation_model = parts[-1]
                    combo_id = "_".join(parts[:-2])
                else:
                    print(f"Skipping malformed draft id: {doc}", file=sys.stderr)
                    continue
            else:
                generation_model = "_".join(parts[l_idx+1:]) if l_idx+1 < len(parts) else None
                combo_id = "_".join(parts[:l_idx]) if l_idx > 0 else None
            if not (combo_id and link_template and generation_model):
                print(f"Skipping malformed draft id (parsed): {doc}", file=sys.stderr)
                continue
            link_task_id = doc  # For drafts, document_id equals link_task_id
            fp = links_dir / f"{link_task_id}.txt"
            if not fp.exists():
                missing_links.append(str(fp))
            link_rows.append({
                "link_task_id": link_task_id,
                "combo_id": combo_id,
                "link_template": link_template,
                "generation_model": generation_model,
                "generation_model_name": model_map.get(generation_model, generation_model),
            })

    # Write outputs
    essay_out_csv.parent.mkdir(parents=True, exist_ok=True)
    if essay_rows:
        pd.DataFrame(essay_rows, columns=[
            "essay_task_id","link_task_id","combo_id","link_template",
            "essay_template","generation_model","generation_model_name"
        ]).drop_duplicates(subset=["essay_task_id"]).to_csv(essay_out_csv, index=False)
        print(f"Wrote {len(essay_rows)} curated essay tasks to {essay_out_csv}")
    if link_rows:
        pd.DataFrame(link_rows, columns=[
            "link_task_id","combo_id","link_template","generation_model","generation_model_name"
        ]).drop_duplicates(subset=["link_task_id"]).to_csv(link_out_csv, index=False)
        print(f"Wrote {len(link_rows)} curated link tasks to {link_out_csv}")

    if missing_essays:
        print("Warning: missing essay files (evaluation will fail for these if run):", file=sys.stderr)
        for m in missing_essays[:10]:
            print(" -", m, file=sys.stderr)
        if len(missing_essays) > 10:
            print(f" ... {len(missing_essays)-10} more", file=sys.stderr)
    if missing_links:
        print("Warning: missing drafts files (evaluation will fail for these if run):", file=sys.stderr)
        for m in missing_links[:10]:
            print(" -", m, file=sys.stderr)
        if len(missing_links) > 10:
            print(f" ... {len(missing_links)-10} more", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
