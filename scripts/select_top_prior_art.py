#!/usr/bin/env python3
"""
Select top-N documents by prior-art scores (cross-experiment) and write
curated essay generation tasks to data/2_tasks/essay_generation_tasks.csv.

Optionally control writing the curated output via --write-drafts.
When set to false, the script performs selection but does not write
the curated tasks CSV.

Defaults:
- TOP_N = 10
- Prior-art templates: gemini-prior-art-eval, gemini-prior-art-eval-v2 (use whichever are present; if both, take max per generation)
- Parsed scores source: data/7_cross_experiment/parsed_scores.csv (required)
- Output tasks CSV: data/2_tasks/essay_generation_tasks.csv (always)

Usage examples:
  uv run python scripts/select_top_prior_art.py \
    --parsed-scores data/7_cross_experiment/parsed_scores.csv

  # Override target template or N
  uv run python scripts/select_top_prior_art.py --top-n 25

  # Do not write the curated tasks CSV (dry run)
  uv run python scripts/select_top_prior_art.py --no-write-drafts
"""

from __future__ import annotations

import argparse
from pathlib import Path
import json
import sys
import pandas as pd
# No external lookups required; operate only on docs metadata for selected records


DEFAULT_PRIOR_ART_TEMPLATES = [
    "gemini-prior-art-eval",
    "gemini-prior-art-eval-v2",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parsed-scores", type=Path, default=Path("data/7_cross_experiment/parsed_scores.csv"), help="Path to cross-experiment parsed_scores.csv")
    p.add_argument("--top-n", type=int, default=10, help="Number of top documents to select")
    p.add_argument("--prior-art-templates", type=str, nargs="*", default=DEFAULT_PRIOR_ART_TEMPLATES, help="Prior-art templates to consider (use whichever are present)")
    drafts_group = p.add_mutually_exclusive_group()
    drafts_group.add_argument(
        "--write-drafts",
        dest="write_drafts",
        action="store_true",
        help="Write curated essay_generation_tasks.csv (default: on)",
    )
    drafts_group.add_argument(
        "--no-write-drafts",
        dest="write_drafts",
        action="store_false",
        help="Do not write curated essay_generation_tasks.csv (dry run)",
    )
    p.set_defaults(write_drafts=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.parsed_scores.exists():
        print(f"ERROR: parsed_scores file not found: {args.parsed_scores}", file=sys.stderr)
        return 2
    df = pd.read_csv(args.parsed_scores)

    # Validate required columns (doc-id-first)
    if df.empty or "evaluation_template" not in df.columns:
        print("ERROR: parsed_scores is empty or missing 'evaluation_template' column", file=sys.stderr)
        return 2
    if "parent_doc_id" not in df.columns:
        print("ERROR: parsed_scores must include 'parent_doc_id' for grouping (doc-id-first)", file=sys.stderr)
        return 2

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
        & (
            (df["parent_doc_id"].notna() if "parent_doc_id" in df.columns else df["document_id"].notna())
        )
    ].copy()

    # Exclude evaluations run by Gemini models (empirically noisy for prior-art)
    try:
        name_col = (filt.get("evaluation_model_name") or filt.get("model_name"))
        id_col = (filt.get("evaluation_model") or filt.get("model"))
        name_is_gemini = name_col.fillna("").str.contains("gemini", case=False) if name_col is not None else False
        id_is_gemini = id_col.fillna("").str.contains("gemini", case=False) if id_col is not None else False
        mask = ~(name_is_gemini | id_is_gemini)
        filt = filt[mask]
    except Exception:
        # If columns missing/unexpected, proceed without the exclusion
        pass
    if filt.empty:
        print("No successful prior-art scores found to select from.", file=sys.stderr)
        return 1
    # Prefer grouping by parent_doc_id (generation doc) if present; else fallback to document_id
    key_col = "parent_doc_id"
    best = (
        filt.groupby(key_col)["score"].max().reset_index().sort_values(
            by=["score", key_col], ascending=[False, True]
        )
    )
    top_docs = best.head(args.top_n)[key_col].astype(str).tolist()
    if not top_docs:
        print("ERROR: No candidates found after filtering and grouping by parent_doc_id.", file=sys.stderr)
        print(f"Filters: templates={available}, top_n={args.top_n}", file=sys.stderr)
        return 1

    # Build curated generation task CSVs (essays, and optionally drafts)
    models_csv = Path("data/1_raw/llm_models.csv")
    data_root = Path("data")
    # Legacy response directories are not used in doc-id-first path
    essay_out_csv = Path("data/2_tasks/essay_generation_tasks.csv")
    # No draft tasks output here; we only write curated essay tasks

    # Load model mapping id -> provider/model name
    model_map = {}
    if models_csv.exists():
        try:
            mdf = pd.read_csv(models_csv)
            if {"id","model"}.issubset(mdf.columns):
                model_map = dict(zip(mdf["id"].astype(str), mdf["model"].astype(str)))
        except Exception as e:
            print(f"Warning: failed to read model mapping ({e}); will use model ids as names", file=sys.stderr)

    # No template CSV parsing in doc-id-first path

    essay_rows = []
    missing_essays = []
    # Collect missing-field diagnostics per selected doc
    missing_required: list[str] = []

    for doc in top_docs:
        # Doc is the generation (essay) doc_id; use docs store metadata
        essay_doc_id = str(doc)
        data_root = Path("data")
        essay_dir = data_root / "docs" / "essay" / essay_doc_id
        essay_meta = {}
        try:
            mp = essay_dir / "metadata.json"
            if mp.exists():
                essay_meta = json.loads(mp.read_text(encoding="utf-8")) or {}
        except Exception:
            essay_meta = {}
        essay_template = str(essay_meta.get("template_id") or essay_meta.get("essay_template") or "")
        generation_model = str(essay_meta.get("model_id") or "")
        # Resolve parent draft to get combo_id and draft_template for legacy task ids
        draft_doc_id = str(essay_meta.get("parent_doc_id") or "")
        combo_id = ""
        draft_template = ""
        if draft_doc_id:
            draft_meta_path = data_root / "docs" / "draft" / draft_doc_id / "metadata.json"
            try:
                if draft_meta_path.exists():
                    dmeta = json.loads(draft_meta_path.read_text(encoding="utf-8")) or {}
                    combo_id = str(dmeta.get("combo_id") or "")
                    draft_template = str(dmeta.get("template_id") or dmeta.get("draft_template") or "")
                    # If generation_model missing, use the draft model
                    if not generation_model:
                        generation_model = str(dmeta.get("model_id") or "")
            except Exception:
                pass
        parent_doc_id = essay_doc_id
        if not (essay_dir / "parsed.txt").exists():
            missing_essays.append(str(essay_dir / "parsed.txt"))
        # Validate required fields
        missing_fields = []
        if not essay_template:
            missing_fields.append("essay_template")
        if not generation_model:
            missing_fields.append("generation_model")
        if not draft_doc_id:
            missing_fields.append("essay.parent_doc_id (draft parent)")
        if not combo_id:
            missing_fields.append("combo_id")
        if not draft_template:
            missing_fields.append("draft_template")
        if missing_fields:
            missing_required.append(f"{essay_doc_id}: missing {', '.join(missing_fields)}")
            continue
        # Legacy task ids (still required): compose strictly from docs metadata
        draft_task_id = f"{combo_id}_{draft_template}_{generation_model}"
        essay_task_id = f"{draft_task_id}_{essay_template}"
        essay_rows.append({
            "essay_task_id": essay_task_id,
            "parent_doc_id": parent_doc_id,
            "draft_task_id": draft_task_id,
            "combo_id": combo_id,
            "draft_template": draft_template,
            "essay_template": essay_template,
            "generation_model": generation_model,
            "generation_model_name": model_map.get(generation_model, generation_model) if generation_model else "",
        })

        # No legacy fallback: grouping strictly by parent_doc_id

    # Write output (essay tasks only)
    if args.write_drafts:
        essay_out_csv.parent.mkdir(parents=True, exist_ok=True)
        if essay_rows:
            if missing_required:
                print("ERROR: missing required metadata to compose legacy task IDs:", file=sys.stderr)
                for msg in missing_required:
                    print(" -", msg, file=sys.stderr)
                return 2
            pd.DataFrame(essay_rows, columns=[
                "essay_task_id","parent_doc_id","draft_task_id","combo_id","draft_template",
                "essay_template","generation_model","generation_model_name"
            ]).drop_duplicates(subset=["essay_task_id"]).to_csv(essay_out_csv, index=False)
            print(f"Wrote {len(essay_rows)} curated essay tasks to {essay_out_csv}")
        else:
            print(
                "ERROR: No essay rows constructed. Ensure docs metadata contains essay.template_id, essay.model_id, essay.parent_doc_id and draft.combo_id, draft.template_id.",
                file=sys.stderr,
            )
            if missing_required:
                print("Details (per selected parent_doc_id):", file=sys.stderr)
                for msg in missing_required:
                    print(" -", msg, file=sys.stderr)
            return 2
    else:
        print(f"Dry run: selected {len(essay_rows)} essay tasks; no file written")

    # Note: Partition registration removed; this script only writes curated CSVs now.

    if missing_essays:
        print("Warning: missing essay files (evaluation will fail for these if run):", file=sys.stderr)
        for m in missing_essays[:10]:
            print(" -", m, file=sys.stderr)
        if len(missing_essays) > 10:
            print(f" ... {len(missing_essays)-10} more", file=sys.stderr)
    # parent_doc_id is required; above path returns early if any missing
    # Note: Draft backfill checks removed; selection focuses on essays only.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
