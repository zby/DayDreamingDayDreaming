#!/usr/bin/env python3
"""
Select top-N generations by prior-art scores (cross-experiment) and write
curated essay generation tasks to data/2_tasks/essay_generation_tasks.csv.

Optionally control writing via --write-drafts (defaults on). With --no-write-drafts,
the script performs selection but does not write the curated CSV.

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
    # Cohorts
    p.add_argument("--cohort-id", type=str, default=None, help="Explicit cohort id for curated outputs")
    p.add_argument(
        "--cohort-mode",
        type=str,
        choices=["deterministic", "timestamped"],
        default="timestamped",
        help="Cohort id selection mode for curated outputs (default: timestamped)",
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

    # Build curated generation task CSV (essays only)
    data_root = Path("data")
    essay_out_csv = Path("data/2_tasks/essay_generation_tasks.csv")
    essay_rows: list[dict] = []
    missing_required: list[str] = []

    for doc in top_docs:
        # Doc is the generation (essay) gen_id; use gens store metadata
        gen_id = str(doc)
        data_root = Path("data")
        essay_dir = data_root / "gens" / "essay" / gen_id
        mp = essay_dir / "metadata.json"
        essay_meta = json.loads(mp.read_text(encoding="utf-8")) if mp.exists() else {}
        essay_template = str(essay_meta.get("template_id") or essay_meta.get("essay_template") or "")
        generation_model = str(essay_meta.get("model_id") or "")
        # Resolve parent draft to get combo_id and draft_template for legacy task ids
        parent_gen_id = str(essay_meta.get("parent_gen_id") or "")
        combo_id = ""
        draft_template = ""
        if parent_gen_id:
            draft_meta_path = data_root / "gens" / "draft" / parent_gen_id / "metadata.json"
            dmeta = json.loads(draft_meta_path.read_text(encoding="utf-8")) if draft_meta_path.exists() else {}
            combo_id = str(dmeta.get("combo_id") or "")
            draft_template = str(dmeta.get("template_id") or dmeta.get("draft_template") or "")
            # If generation_model missing, use the draft model
            if not generation_model:
                generation_model = str(dmeta.get("model_id") or "")
        # Validate required fields
        missing_fields = []
        if not essay_template:
            missing_fields.append("essay_template")
        if not generation_model:
            missing_fields.append("generation_model")
        if not parent_gen_id:
            missing_fields.append("essay.parent_gen_id (draft parent)")
        if not combo_id:
            missing_fields.append("combo_id")
        if not draft_template:
            missing_fields.append("draft_template")
        if missing_fields:
            missing_required.append(f"{gen_id}: missing {', '.join(missing_fields)}")
            continue
        # Legacy task ids (still required): compose strictly from docs metadata
        draft_task_id = f"{combo_id}_{draft_template}_{generation_model}"
        essay_task_id = f"{draft_task_id}_{essay_template}"
        essay_rows.append({
            "essay_task_id": essay_task_id,
            # Essay row lineage: parent_gen_id points to the draft gen id
            "parent_gen_id": parent_gen_id,
            # Also include essay gen_id for convenience in examples below
            "gen_id": gen_id,
            "draft_task_id": draft_task_id,
            "combo_id": combo_id,
            "draft_template": draft_template,
            "essay_template": essay_template,
            "generation_model": generation_model,
        })

        # No legacy fallback: grouping strictly by parent_doc_id

    # Compute cohort id for curated set
    from daydreaming_dagster.utils.cohorts import compute_cohort_id
    cohort_id = compute_cohort_id(
        kind="curated",
        manifest={
            "combos": sorted(list({r.get("combo_id", "") for r in essay_rows if r.get("combo_id")})),
            "llms": [],
            "templates": {},
            "prompt_versions": None,
        },
        mode=args.cohort_mode,
        explicit=args.cohort_id,
    )

    # Write output (essay tasks only)
    if args.write_drafts:
        essay_out_csv.parent.mkdir(parents=True, exist_ok=True)
        if essay_rows:
            if missing_required:
                print("ERROR: missing required metadata to compose legacy task IDs:", file=sys.stderr)
                for msg in missing_required:
                    print(" -", msg, file=sys.stderr)
                return 2
            # Ensure gen-id-first: include gen_id for essays
            df_out = pd.DataFrame(essay_rows, columns=[
                "essay_task_id","parent_gen_id","draft_task_id","combo_id","draft_template",
                "essay_template","generation_model","gen_id"
            ]).drop_duplicates(subset=["essay_task_id"]).copy()
            df_out["cohort_id"] = cohort_id
            df_out.to_csv(essay_out_csv, index=False)
            print(f"Wrote {len(essay_rows)} curated essay tasks to {essay_out_csv}")
        else:
            print(
                "ERROR: No essay rows constructed. Ensure docs metadata contains essay.template_id, essay.model_id, essay.parent_doc_id and draft.combo_id, draft.template_id.",
                file=sys.stderr,
            )
            return 2
    else:
        print(f"Dry run: selected {len(essay_rows)} essay tasks; no file written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
