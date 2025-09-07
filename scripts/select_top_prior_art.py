#!/usr/bin/env python3
"""
Select top-N documents by prior-art scores (cross-experiment) and write
curated essay generation tasks to data/2_tasks/essay_generation_tasks.csv.

Optionally control writing the curated output via --write-drafts.
When set to false, the script performs selection but does not write
the curated tasks CSV.

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

  # Do not write the curated tasks CSV (dry run)
  uv run python scripts/select_top_prior_art.py --no-write-drafts
"""

from __future__ import annotations

import argparse
from pathlib import Path
import json
import sys
import pandas as pd
from daydreaming_dagster.utils.document_locator import find_document_path


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
    best = (
        filt.groupby("document_id")["score"].max().reset_index().sort_values(
            by=["score", "document_id"], ascending=[False, True]
        )
    )
    top_docs = best.head(args.top_n)["document_id"].tolist()

    # Build curated generation task CSVs (essays, and optionally drafts)
    models_csv = Path("data/1_raw/llm_models.csv")
    data_root = Path("data")
    essay_dir = data_root / "3_generation" / "essay_responses"
    drafts_dir = data_root / "3_generation" / "draft_responses"
    if not drafts_dir.exists():
        drafts_dir = data_root / "3_generation" / "links_responses"  # legacy fallback
    essay_out_csv = Path("data/2_tasks/essay_generation_tasks.csv")
    link_out_csv = Path("data/2_tasks/draft_generation_tasks.csv")
    draft_tpl_csv = Path("data/1_raw/draft_templates.csv")
    if not draft_tpl_csv.exists():
        alt = Path("data/1_raw/link_templates.csv")  # legacy fallback
        if alt.exists():
            draft_tpl_csv = alt
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
    draft_tpls: set[str] = set()
    essay_tpls: set[str] = set()
    try:
        if draft_tpl_csv.exists():
            ldf = pd.read_csv(draft_tpl_csv)
            if "template_id" in ldf.columns:
                draft_tpls = set(ldf["template_id"].astype(str))
        if essay_tpl_csv.exists():
            edf = pd.read_csv(essay_tpl_csv)
            if "template_id" in edf.columns:
                essay_tpls = set(edf["template_id"].astype(str))
    except Exception as e:
        print(f"Warning: failed to read template CSVs ({e}); falling back to naive parsing", file=sys.stderr)

    essay_rows = []
    missing_essays = []
    missing_parents = []

    def _find_parent_doc_id_from_essay_docs(data_root: Path, essay_task_id: str) -> str | None:
        """Scan data/docs/essay/**/metadata.json for a row with task_id == essay_task_id.

        Returns the latest by created_at if multiple, else first found; None if not found.
        """
        docs_root = data_root / "docs" / "essay"
        if not docs_root.exists():
            return None
        best = None
        best_ts = ""
        # Two-level traversal: stage/essay/<doc_id>/metadata.json (logical_key directory may be omitted)
        # Our layout is flat by doc_id: essay/<doc_id>/metadata.json
        for md in docs_root.glob("*/metadata.json"):
            try:
                meta = json.loads(md.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(meta, dict):
                continue
            if str(meta.get("task_id")) != str(essay_task_id):
                continue
            created = str(meta.get("created_at") or "")
            if created >= best_ts:
                best_ts = created
                best = str(meta.get("parent_doc_id") or "")
        return best or None

    for doc in top_docs:
        parts = doc.split("_")

        # Try to detect essay vs draft by locating an essay template token
        essay_template = None
        draft_template = None
        generation_model = None
        combo_id = None

        e_idx = None
        if essay_tpls:
            # Consider it an essay doc only if the last token is an essay template
            last_idx = len(parts) - 1
            if last_idx >= 0 and parts[last_idx] in essay_tpls:
                essay_template = parts[last_idx]
                e_idx = last_idx
        if e_idx is not None:
            # Essay doc id: find last draft template before essay (legacy: link template)
            l_idx = None
            if draft_tpls:
                for j in range(e_idx - 1, -1, -1):
                    if parts[j] in draft_tpls:
                        draft_template = parts[j]
                        l_idx = j
                        break
            if l_idx is None and len(parts) >= 4:
                # Fallback naive positions
                draft_template = parts[-3]
                l_idx = len(parts) - 3
            gen_tokens = parts[(l_idx + 1) if l_idx is not None else 0 : e_idx]
            generation_model = "_".join(gen_tokens) if gen_tokens else None
            combo_id = "_".join(parts[: (l_idx if l_idx is not None else 0)]) or None

            if not (combo_id and draft_template and generation_model and essay_template):
                print(f"Skipping malformed essay id: {doc}", file=sys.stderr)
                continue

            draft_task_id = f"{combo_id}_{draft_template}_{generation_model}"
            fp, _ = find_document_path(doc, data_root)
            if not fp:
                missing_essays.append(str(essay_dir / f"{doc}.txt"))
            parent_doc_id = _find_parent_doc_id_from_essay_docs(data_root, doc)
            if not parent_doc_id:
                missing_parents.append(doc)
            essay_rows.append({
                "essay_task_id": doc,
                "parent_doc_id": parent_doc_id or "",
                "draft_task_id": draft_task_id,
                "combo_id": combo_id,
                "draft_template": draft_template,
                "essay_template": essay_template,
                "generation_model": generation_model,
                "generation_model_name": model_map.get(generation_model, generation_model),
            })
        else:
            # Ignore draft documents for curated essay selection
            continue

    # Write output (essay tasks only)
    if args.write_drafts:
        essay_out_csv.parent.mkdir(parents=True, exist_ok=True)
        if essay_rows:
            # Enforce that parent_doc_id is present for all rows in doc-id-first mode
            if missing_parents:
                print("ERROR: Could not derive parent_doc_id for the following essay_task_id; ensure essays exist in data/docs/essay before selection:", file=sys.stderr)
                for m in missing_parents:
                    print(" -", m, file=sys.stderr)
                return 2
            pd.DataFrame(essay_rows, columns=[
                "essay_task_id","parent_doc_id","draft_task_id","combo_id","draft_template",
                "essay_template","generation_model","generation_model_name"
            ]).drop_duplicates(subset=["essay_task_id"]).to_csv(essay_out_csv, index=False)
            print(f"Wrote {len(essay_rows)} curated essay tasks to {essay_out_csv}")
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
