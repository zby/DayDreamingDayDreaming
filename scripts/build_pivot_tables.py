#!/usr/bin/env python3
"""Build cross-experiment pivot tables of essay scores.

Outputs:
- `evaluation_scores_by_template_model.csv` – evaluation template/model pairs
  per essay task. `--cohort-id` automatically scopes the pivot using that
  cohort's spec; you can also pass `--cohort-allowlist <cohort_id>` explicitly.

Usage examples:
```
python scripts/build_pivot_tables.py
python scripts/build_pivot_tables.py --cohort-allowlist cohort-2025-q1
```
"""

from __future__ import annotations

import argparse
from pathlib import Path
import json
from typing import Any, Mapping, Set
import pandas as pd


def _ensure_src_on_path() -> None:
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))


_ensure_src_on_path()

from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.assets.group_cohorts import _build_spec_catalogs
from daydreaming_dagster.cohorts import (
    CohortDefinition,
    load_cohort_allowlists,
    load_cohort_definition,
)
from daydreaming_dagster.spec_dsl import ExperimentSpec


def _compose_essay_task_id(df: pd.DataFrame) -> pd.Series:
    """Derive the essay_task_id using combo/template/model columns from the new schema."""

    required = ["combo_id", "draft_template", "generation_model", "generation_template"]
    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "missing_task_columns", "missing": missing_cols},
        )

    def compose(row: pd.Series) -> str:
        parts: list[str] = []
        for col in required:
            value = row[col]
            if pd.isna(value):
                raise DDError(
                    Err.DATA_MISSING,
                    ctx={
                        "reason": "essay_task_component_nan",
                        "column": col,
                        "row_index": int(row.name),
                    },
                )
            text = str(value).strip()
            if not text:
                raise DDError(
                    Err.DATA_MISSING,
                    ctx={
                        "reason": "essay_task_component_empty",
                        "column": col,
                        "row_index": int(row.name),
                    },
                )
            parts.append(text)
        return "_".join(parts)

    return df.apply(compose, axis=1)


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
        cohorts[essay_id] = str(data.get("origin_cohort_id") or "").strip()
    return cohorts


def _load_spec_filters(
    cohort_id: str,
    paths: Paths,
    *,
    definition: CohortDefinition | None = None,
    spec: ExperimentSpec | None = None,
    catalogs: Mapping[str, Any] | None = None,
    seed: int | None = None,
) -> tuple[set[str], set[str]]:
    catalogs = catalogs or _build_spec_catalogs(paths.data_root)

    plan = definition
    if plan is None and spec is not None:
        plan = load_cohort_definition(spec, catalogs=catalogs, seed=seed)

    def _compile_from_path(*, path, catalogs, **_kwargs):
        return load_cohort_definition(path, catalogs=catalogs, seed=seed)

    allowlists = load_cohort_allowlists(
        data_root=paths.data_root,
        cohort_id=cohort_id,
        compile_definition=_compile_from_path,
        catalogs=catalogs,
        definition=plan,
    )

    return set(allowlists.evaluation_templates), set(allowlists.evaluation_models)


def build_pivot(
    parsed_scores: Path,
    out_dir: Path,
    *,
    paths: Paths,
    cohort_allowlist: str | None = None,
    cohort_definition: CohortDefinition | None = None,
    cohort_spec: ExperimentSpec | None = None,
    catalogs: Mapping[str, Any] | None = None,
    seed: int | None = None,
) -> None:
    if not parsed_scores.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "parsed_scores_missing", "path": str(parsed_scores)},
        )

    df = pd.read_csv(parsed_scores)

    evaluation_filter: tuple[set[str], set[str]] | None = None
    if cohort_allowlist:
        evaluation_filter = _load_spec_filters(
            cohort_allowlist,
            paths,
            definition=cohort_definition,
            spec=cohort_spec,
            catalogs=catalogs,
            seed=seed,
        )

    if evaluation_filter:
        eval_templates, eval_models = evaluation_filter
        if "evaluation_template" not in df.columns or "evaluation_llm_model" not in df.columns:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "parsed_scores_missing_evaluation_columns"},
            )
        before = len(df)
        df = df[
            df["evaluation_template"].isin(eval_templates)
            & df["evaluation_llm_model"].isin(eval_models)
        ].copy()
        if df.empty:
            raise DDError(
                Err.DATA_MISSING,
                ctx={"reason": "allowlist_filter_removed_all_rows"},
            )
        removed = before - len(df)
        if removed:
            print(
                "Filtered parsed scores to spec-defined evaluation templates/models: "
                f"kept {len(df)} rows (dropped {removed})."
            )

    task_cols = ["combo_id", "draft_template", "generation_template", "generation_model"]
    missing_task_cols = [col for col in task_cols if col not in df.columns]
    if missing_task_cols:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "missing_task_metadata", "missing": missing_task_cols},
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
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "no_complete_task_metadata"},
        )

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
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "parsed_scores_missing_base_columns", "missing": missing},
        )
    # Normalize template column name
    if "evaluation_template" not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "missing_evaluation_template_column"},
        )

    # Backward-compat: if draft_template missing, populate from link_template when present
    if "draft_template" not in df.columns:
        if "link_template" in df.columns:
            df["draft_template"] = df["link_template"]
        else:
            # Keep a column for downstream selection even if empty
            df["draft_template"] = None

    # Compose column key as template__evaluator (strict: require evaluation_llm_model)
    if "evaluation_llm_model" not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "missing_evaluation_llm_model_column"},
        )
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
        "origin_cohort_id": "first",
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
            meta_df["origin_cohort_id"] = meta_df["essay_gen_id"].map(lambda x: essay_cohort_map.get(x, ""))
        if "origin_cohort_id" not in meta_df.columns:
            meta_df["origin_cohort_id"] = ""
    
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
            "origin_cohort_id",
        ]
        if col in result.columns
    ]
    metric_columns = [col for col in all_combo_columns if col in result.columns]

    numeric_metrics = result[metric_columns].apply(pd.to_numeric, errors="coerce") if metric_columns else None
    score_sum_column = "allowlisted_template_score_sum"
    if numeric_metrics is not None:
        result[score_sum_column] = numeric_metrics.sum(axis=1, skipna=True).fillna(0.0)
    else:
        result[score_sum_column] = 0.0

    ordered_columns = meta_output_cols + metric_columns + [score_sum_column]
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
        default=Path("data/7_cross_experiment/aggregated_scores.csv"),
        help="Path to aggregated scores CSV (default: data/7_cross_experiment/aggregated_scores.csv)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/7_cross_experiment"),
        help="Directory to write pivot CSVs (default: data/7_cross_experiment)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Project data root containing gens metadata (default: data)",
    )
    parser.add_argument(
        "--cohort-id",
        type=str,
        default=None,
        help="If provided, derive parsed scores and output directory from data/cohorts/<id>/reports",
    )
    parser.add_argument(
        "--cohort-allowlist",
        type=str,
        default=None,
        help=(
            "Optional cohort ID whose spec defines the evaluation templates/models to include"
        ),
    )
    args = parser.parse_args()

    paths = Paths.from_str(args.data_root)

    parsed_scores_path = args.parsed_scores
    output_dir = args.out_dir

    cohort_allowlist = args.cohort_allowlist

    if args.cohort_id:
        cohort_id = args.cohort_id.strip()
        if not cohort_id:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "invalid_cohort_id", "value": args.cohort_id},
            )
        parsed_scores_path = paths.cohort_parsing_csv(cohort_id, "aggregated_scores.csv")
        output_dir = paths.cohort_report_root(cohort_id) / "summary"
        if not cohort_allowlist:
            cohort_allowlist = cohort_id

    build_pivot(
        parsed_scores=parsed_scores_path,
        out_dir=output_dir,
        paths=paths,
        cohort_allowlist=cohort_allowlist,
    )


if __name__ == "__main__":
    main()
