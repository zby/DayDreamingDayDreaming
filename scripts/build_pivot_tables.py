#!/usr/bin/env python3
"""Build cross-experiment pivot tables of essay scores.

Outputs:
- `evaluation_scores_by_template_model.csv` – evaluation template/model pairs
  per essay task. Pass `--limit-to-active` to include only active evaluation
  templates and evaluation models in the pivot.

Usage examples:
```
python scripts/build_pivot_tables.py
python scripts/build_pivot_tables.py --limit-to-active
```
"""

from __future__ import annotations

import argparse
from pathlib import Path
import json
from typing import Set
import pandas as pd


def _compose_essay_task_id(df: pd.DataFrame) -> pd.Series:
    """Derive the essay_task_id using combo/template/model columns from the new schema."""

    required = ["combo_id", "draft_template", "generation_model", "generation_template"]
    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols:
        raise ValueError(
            "Unable to derive essay_task_id; missing required columns: "
            f"{missing_cols}"
        )

    def compose(row: pd.Series) -> str:
        parts: list[str] = []
        for col in required:
            value = row[col]
            if pd.isna(value):
                raise ValueError(
                    "Unable to derive essay_task_id due to NaN in column "
                    f"'{col}' for row index {row.name}"
                )
            text = str(value).strip()
            if not text:
                raise ValueError(
                    "Unable to derive essay_task_id due to empty value in column "
                    f"'{col}' for row index {row.name}"
                )
            parts.append(text)
        return "_".join(parts)

    return df.apply(compose, axis=1)


def _load_active_templates(eval_templates_path: Path) -> set[str]:
    import csv

    active: set[str] = set()
    with eval_templates_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("active", "").strip().lower() == "true":
                template_id = row.get("template_id", "").strip()
                if template_id:
                    active.add(template_id)
    return active


def _load_active_models(llm_models_path: Path) -> Set[str]:
    import csv

    active: set[str] = set()
    with llm_models_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            flag = str(row.get("for_evaluation") or "").strip().lower()
            if flag == "true":
                model_id = row.get("id", "").strip()
                if model_id:
                    active.add(model_id)
    return active


def _load_essay_cohorts(essay_ids: Set[str], data_root: Path) -> dict[str, str]:
    cohorts: dict[str, str] = {}
    base = data_root / "gens" / "essay"
    for essay_id in essay_ids:
        if not essay_id:
            continue
        meta_path = base / essay_id / "metadata.json"
        if not meta_path.exists():
            cohorts[essay_id] = ""
            continue
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            cohorts[essay_id] = ""
            continue
        cohorts[essay_id] = str(data.get("cohort_id") or "").strip()
    return cohorts


def build_pivot(
    parsed_scores: Path,
    out_dir: Path,
    *,
    limit_to_active: bool,
    evaluation_templates_path: Path,
    llm_models_path: Path,
    data_root: Path,
) -> None:
    if not parsed_scores.exists():
        raise FileNotFoundError(f"Parsed scores CSV not found: {parsed_scores}")

    df = pd.read_csv(parsed_scores)

    if limit_to_active:
        active_templates = _load_active_templates(evaluation_templates_path)
        active_models = _load_active_models(llm_models_path)
        if not active_templates:
            raise RuntimeError(
                f"No active evaluation templates found in {evaluation_templates_path}."
            )
        if not active_models:
            raise RuntimeError(
                f"No evaluation models marked active in {llm_models_path}."
            )
        if "evaluation_template" not in df.columns or "evaluation_llm_model" not in df.columns:
            raise ValueError(
                "Parsed scores missing 'evaluation_template' or 'evaluation_llm_model' columns"
            )
        before = len(df)
        df = df[
            df["evaluation_template"].isin(active_templates)
            & df["evaluation_llm_model"].isin(active_models)
        ].copy()
        if df.empty:
            raise RuntimeError(
                "Filtering to active evaluation templates/models removed all rows."
            )
        removed = before - len(df)
        if removed:
            print(
                "Filtered parsed scores to active templates/models: "
                f"kept {len(df)} rows (dropped {removed})."
            )

    task_cols = ["combo_id", "draft_template", "generation_template", "generation_model"]
    missing_task_cols = [col for col in task_cols if col not in df.columns]
    if missing_task_cols:
        raise ValueError(
            "Parsed scores missing required task metadata columns: "
            f"{missing_task_cols}"
        )

    missing_mask = df[task_cols].isna().any(axis=1)
    empty_mask = df[task_cols].astype(str).apply(lambda col: col.str.strip() == "").any(axis=1)
    drop_mask = missing_mask | empty_mask
    if drop_mask.any():
        dropped = int(drop_mask.sum())
        drop_details = df.loc[drop_mask, task_cols + ["gen_id", "parent_gen_id", "evaluation_template", "evaluation_llm_model"]]
        print(
            "Dropping rows without complete task metadata:\n"
            f"Total dropped: {dropped}\n"
            f"Missing columns breakdown:\n{drop_details.head(20).to_markdown(index=False)}"
        )
        df = df.loc[~drop_mask].copy()
    if df.empty:
        raise ValueError("No rows with complete task metadata available for pivot")

    # Ensure required columns exist (canonical uses draft_template; support legacy link_template)
    # Accept either 'template_id' (new) or 'evaluation_template' (legacy)
    required_base = [
        "essay_task_id",
        "combo_id",
        "generation_template",
        "generation_model",
        "score",
    ]
    df["essay_task_id"] = _compose_essay_task_id(df)

    missing = [c for c in required_base if c not in df.columns]
    if missing:
        raise ValueError(
            "Missing columns in parsed scores: "
            f"{missing}; required base columns: {required_base}"
        )
    # Normalize template column name
    if "evaluation_template" not in df.columns:
        raise ValueError("Missing required column 'evaluation_template' in parsed scores")

    # Backward-compat: if draft_template missing, populate from link_template when present
    if "draft_template" not in df.columns:
        if "link_template" in df.columns:
            df["draft_template"] = df["link_template"]
        else:
            # Keep a column for downstream selection even if empty
            df["draft_template"] = None

    # Compose column key as template__evaluator (strict: require evaluation_llm_model)
    if "evaluation_llm_model" not in df.columns:
        raise ValueError("Missing required column 'evaluation_llm_model' in parsed scores")
    df["evaluation_template_model"] = (
        df["evaluation_template"].astype(str) + "__" + df["evaluation_llm_model"].astype(str)
    )
    all_combo_columns = sorted(df["evaluation_template_model"].unique())

    df_sorted = df.sort_values(["essay_task_id", "evaluation_template_model"])  # deterministic

    essay_ids: Set[str] = set()
    if "parent_gen_id" in df_sorted.columns:
        essay_ids = set(df_sorted["parent_gen_id"].dropna().astype(str).tolist())
    essay_cohort_map = _load_essay_cohorts(essay_ids, data_root)

    meta_columns = {
        "parent_gen_id": "first",
        "combo_id": "first",
        "draft_template": "first",
        "generation_template": "first",
        "generation_model": "first",
        "cohort_id": "first",
    }
    available_meta = {col: agg for col, agg in meta_columns.items() if col in df_sorted.columns}
    meta_df = (
        df_sorted[["essay_task_id", *available_meta.keys()]]
        .groupby("essay_task_id", as_index=False)
        .agg(available_meta)
    )

    if "parent_gen_id" in meta_df.columns:
        meta_df.rename(columns={"parent_gen_id": "essay_gen_id"}, inplace=True)
        meta_df["essay_gen_id"] = meta_df["essay_gen_id"].astype(str).replace({"nan": ""})
        if essay_cohort_map:
            meta_df["cohort_id"] = meta_df["essay_gen_id"].map(lambda x: essay_cohort_map.get(x, ""))
        else:
            if "cohort_id" not in meta_df.columns:
                meta_df["cohort_id"] = ""
    
    pivot = (
        df_sorted
        .pivot_table(
            index="essay_task_id",
            columns="evaluation_template_model",
            values="score",
            aggfunc="first",
        )
        .reset_index()
    )

    for combo in all_combo_columns:
        if combo not in pivot.columns:
            pivot[combo] = pd.NA

    result = meta_df.merge(pivot, on="essay_task_id", how="outer")

    meta_output_cols = [
        col
        for col in [
            "essay_gen_id",
            "combo_id",
            "draft_template",
            "generation_template",
            "generation_model",
            "cohort_id",
        ]
        if col in result.columns
    ]
    metric_columns = [col for col in all_combo_columns if col in result.columns]

    numeric_metrics = result[metric_columns].apply(pd.to_numeric, errors="coerce") if metric_columns else None
    if numeric_metrics is not None:
        result["active_template_score_sum"] = numeric_metrics.sum(axis=1, skipna=True).fillna(0.0)
    else:
        result["active_template_score_sum"] = 0.0

    ordered_columns = meta_output_cols + metric_columns + ["active_template_score_sum"]
    pivot = result.reindex(columns=ordered_columns)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "evaluation_scores_by_template_model.csv"
    pivot.to_csv(out_file, index=False)
    print(f"Wrote pivot: {out_file} ({len(pivot)} rows, {len(pivot.columns)} cols)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build evaluation pivots from aggregated scores")
    parser.add_argument(
        "--parsed-scores",
        type=Path,
        default=Path("data/5_parsing/aggregated_scores.csv"),
        help="Path to aggregated scores CSV (default: data/5_parsing/aggregated_scores.csv)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/7_cross_experiment"),
        help="Directory to write pivot CSVs (default: data/7_cross_experiment)",
    )
    parser.add_argument(
        "--evaluation-templates",
        type=Path,
        default=Path("data/1_raw/evaluation_templates.csv"),
        help="Path to evaluation templates CSV (default: data/1_raw/evaluation_templates.csv)",
    )
    parser.add_argument(
        "--llm-models",
        type=Path,
        default=Path("data/1_raw/llm_models.csv"),
        help="Path to LLM models CSV (default: data/1_raw/llm_models.csv)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Project data root containing gens metadata (default: data)",
    )
    parser.add_argument(
        "--limit-to-active",
        action="store_true",
        help=(
            "If set, restrict the pivot to evaluations whose template and LLM model are marked active."
        ),
    )
    args = parser.parse_args()

    build_pivot(
        parsed_scores=args.parsed_scores,
        out_dir=args.out_dir,
        limit_to_active=args.limit_to_active,
        evaluation_templates_path=args.evaluation_templates,
        llm_models_path=args.llm_models,
        data_root=args.data_root,
    )


if __name__ == "__main__":
    main()
