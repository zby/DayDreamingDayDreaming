#!/usr/bin/env python3
"""
Migrate a cohort membership CSV to the normalized schema.

Input (legacy/denormalized examples):
- stage-specific template/model columns such as: draft_template, essay_template,
  evaluation_template, generation_model, evaluation_model, draft_llm_model,
  essay_llm_model, evaluation_llm_model, etc.

Output (normalized):
- Columns: stage, gen_id, cohort_id, parent_gen_id, combo_id, template_id, llm_model_id

Notes
- For draft rows: template_id <- draft_template; llm_model_id <- generation_model
- For essay rows: template_id <- essay_template; llm_model_id <- llm_model_id if present
  else generation_model; else inherit from parent draft via parent_gen_id
- For evaluation rows: template_id <- evaluation_template; llm_model_id <- evaluation_model
  combo_id is propagated from the essay parent when available

Usage
  python scripts/migrate_membership_to_normalized.py \
    --input data/cohorts/lost-drafts/membership.csv \
    --output data/cohorts/lost-drafts/membership.normalized.csv

  To overwrite in place (writes a .bak backup first):
  python scripts/migrate_membership_to_normalized.py --input <file> --in-place
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

import pandas as pd


NORMALIZED_COLS = [
    "stage",
    "gen_id",
    "cohort_id",
    "parent_gen_id",
    "combo_id",
    "template_id",
    "llm_model_id",
]


def _get(df: pd.Series, *keys: str) -> str:
    for k in keys:
        if k in df and pd.notna(df[k]):
            s = str(df[k]).strip()
            if s:
                return s
    return ""


def normalize_membership(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=NORMALIZED_COLS)

    # Make a shallow copy; normalize stage values
    df = df.copy()
    if "stage" in df.columns:
        df["stage"] = df["stage"].astype(str).str.strip().str.lower()

    # Build quick lookup maps for parents to help backfill
    drafts = {}
    essays = {}
    if not df.empty and "stage" in df.columns:
        try:
            dsub = df[df["stage"] == "draft"][
                [c for c in ["gen_id", "combo_id", "template_id", "draft_template", "llm_model_id", "generation_model"] if c in df.columns]
            ].copy()
            for _, r in dsub.iterrows():
                drafts[str(r.get("gen_id") or "")] = {
                    "combo_id": _get(r, "combo_id"),
                    "template_id": _get(r, "template_id", "draft_template"),
                    "llm_model_id": _get(r, "llm_model_id", "generation_model"),
                }
        except Exception:
            drafts = {}
        try:
            esub = df[df["stage"] == "essay"][
                [
                    c
                    for c in [
                        "gen_id",
                        "parent_gen_id",
                        "combo_id",
                        "template_id",
                        "essay_template",
                        "llm_model_id",
                        "generation_model",
                    ]
                    if c in df.columns
                ]
            ].copy()
            for _, r in esub.iterrows():
                essays[str(r.get("gen_id") or "")] = {
                    "combo_id": _get(r, "combo_id"),
                    "parent_gen_id": _get(r, "parent_gen_id"),
                    "template_id": _get(r, "template_id", "essay_template"),
                    "llm_model_id": _get(r, "llm_model_id", "generation_model"),
                }
        except Exception:
            essays = {}

    out_rows = []
    for _, row in df.iterrows():
        stage = _get(row, "stage").lower()
        gen_id = _get(row, "gen_id")
        cohort_id = _get(row, "cohort_id")
        parent_gen_id = _get(row, "parent_gen_id")
        combo_id = _get(row, "combo_id")
        template_id = ""
        llm_model_id = ""

        if stage == "draft":
            template_id = _get(row, "template_id", "draft_template")
            llm_model_id = _get(row, "llm_model_id", "generation_model")
            parent_gen_id = ""
        elif stage == "essay":
            template_id = _get(row, "template_id", "essay_template")
            # Prefer explicit llm_model_id or generation_model; else inherit from draft
            llm_model_id = _get(row, "llm_model_id", "generation_model")
            if not llm_model_id and parent_gen_id and parent_gen_id in drafts:
                llm_model_id = drafts[parent_gen_id].get("llm_model_id") or ""
            # Ensure combo present
            if not combo_id and parent_gen_id and parent_gen_id in drafts:
                combo_id = drafts[parent_gen_id].get("combo_id") or ""
        elif stage == "evaluation":
            template_id = _get(row, "template_id", "evaluation_template")
            llm_model_id = _get(row, "llm_model_id", "evaluation_model")
            # Propagate combo_id from essay parent if available
            if not combo_id and parent_gen_id and parent_gen_id in essays:
                combo_id = essays[parent_gen_id].get("combo_id") or ""
        else:
            # Unknown stage: pass through the most generic fields
            template_id = _get(row, "template_id", "draft_template", "essay_template", "evaluation_template")
            llm_model_id = _get(row, "llm_model_id", "generation_model", "evaluation_model")

        out_rows.append(
            {
                "stage": stage,
                "gen_id": gen_id,
                "cohort_id": cohort_id,
                "parent_gen_id": parent_gen_id,
                "combo_id": combo_id,
                "template_id": template_id,
                "llm_model_id": llm_model_id,
            }
        )

    out = pd.DataFrame(out_rows, columns=NORMALIZED_COLS)
    # Drop exact duplicates by (stage, gen_id)
    if not out.empty:
        out = out.drop_duplicates(subset=["stage", "gen_id"]).reset_index(drop=True)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize cohort membership CSV to new schema")
    parser.add_argument(
        "--input",
        type=str,
        default=str(Path("data") / "cohorts" / "lost-drafts" / "membership.csv"),
        help="Path to legacy membership.csv",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to write normalized CSV (default: <input>.normalized.csv)",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the input file (writes a .bak backup first)",
    )
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(f"membership CSV not found: {in_path}")
    df = pd.read_csv(in_path)
    norm = normalize_membership(df)

    if args.in_place:
        backup = in_path.with_suffix(in_path.suffix + ".bak")
        in_path.replace(backup)
        norm.to_csv(in_path, index=False)
        print(f"Wrote normalized membership in place: {in_path} (backup: {backup})")
    else:
        out_path = Path(args.output) if args.output else in_path.with_suffix(in_path.suffix + ".normalized.csv")
        norm.to_csv(out_path, index=False)
        print(f"Wrote normalized membership to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

