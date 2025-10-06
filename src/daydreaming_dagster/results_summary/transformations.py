from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import NamedTuple

import numpy as np
import pandas as pd

from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.evaluation_processing import filter_valid_scores

GENERATION_INDEX_COLUMNS = [
    "combo_id",
    "draft_template",
    "generation_template",
    "generation_model",
    "parent_gen_id",
]
EVALUATION_TEMPLATE_COLUMN = "evaluation_template"
EVALUATION_MODEL_COLUMN = "evaluation_llm_model"
SCORE_COLUMN = "score"
GENERATION_RESPONSE_COLUMN = "generation_response_path"
EVALUATION_RESPONSE_COLUMN = "evaluation_response_path"


class GenerationPivotResult(NamedTuple):
    frame: pd.DataFrame
    evaluation_columns: list[str]
    valid_row_count: int
    allowlisted_row_count: int


class FinalResultsResult(NamedTuple):
    frame: pd.DataFrame
    valid_score_count: int


class EvaluationModelTemplatePivotResult(NamedTuple):
    frame: pd.DataFrame
    coverage_stats: dict[str, dict[str, float]]
    valid_row_count: int


def _normalize_allowed(values: Iterable[str]) -> set[str]:
    return {str(value) for value in values if str(value)}


def _require_column(df: pd.DataFrame, column: str, *, reason: str) -> None:
    if column not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "missing": column,
                "reason": reason,
            },
        )


def _require_columns(df: pd.DataFrame, columns: list[str], *, reason: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "missing": missing,
                "reason": reason,
            },
        )


def _pivot_tables(
    df: pd.DataFrame,
    *,
    index_cols: list[str],
    column_key: str,
    value_column: str,
    aggregations: tuple[tuple[str, str], ...],
) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for key, agg in aggregations:
        pivot = df.pivot_table(
            index=index_cols,
            columns=column_key,
            values=value_column,
            aggfunc=agg,
        )
        if agg in {"mean", "min", "max"}:
            pivot = pivot.round(2)
        elif agg == "count":
            pivot = pivot.fillna(0).astype(int)
        tables[key] = pivot
    return tables


def _combine_pivot_tables(
    pivot_tables: dict[str, pd.DataFrame],
    *,
    index_cols: list[str],
    suffix_map: Mapping[str, str],
) -> tuple[pd.DataFrame, list[str]]:
    base_key = "mean" if "mean" in pivot_tables else next(iter(pivot_tables))
    combined = pivot_tables[base_key].reset_index()
    evaluation_columns = [column for column in combined.columns if column not in index_cols]

    for key, suffix in suffix_map.items():
        if key == base_key or key not in pivot_tables or not suffix:
            continue
        extra = pivot_tables[key].copy()
        extra.columns = [f"{column}{suffix}" for column in extra.columns]
        combined = combined.merge(extra.reset_index(), on=index_cols, how="left")

    return combined.reset_index(drop=True), evaluation_columns


def _normalize_path_lookup(
    path_lookup: pd.DataFrame | Mapping[tuple[str, str, str, str], str] | None,
    source_scores: pd.DataFrame,
) -> pd.DataFrame:
    if path_lookup is None:
        required_cols = set(GENERATION_INDEX_COLUMNS + [GENERATION_RESPONSE_COLUMN])
        missing = [col for col in required_cols if col not in source_scores.columns]
        if missing:
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "missing": missing,
                    "reason": "pivot_requires_generation_path",
                },
            )
        base = source_scores[GENERATION_INDEX_COLUMNS + [GENERATION_RESPONSE_COLUMN]].drop_duplicates()
    elif isinstance(path_lookup, Mapping):
        records = []
        for key, path in path_lookup.items():
            if len(key) != len(GENERATION_INDEX_COLUMNS):
                raise DDError(
                    Err.INVALID_CONFIG,
                    ctx={
                        "reason": "pivot_path_key_shape",
                        "expected": len(GENERATION_INDEX_COLUMNS),
                        "actual": len(key),
                    },
                )
            record = {
                **{column: value for column, value in zip(GENERATION_INDEX_COLUMNS, key)},
                GENERATION_RESPONSE_COLUMN: path,
            }
            records.append(record)
        base = pd.DataFrame(records)
    else:
        base = path_lookup.copy()

    _require_columns(
        base,
        GENERATION_INDEX_COLUMNS + [GENERATION_RESPONSE_COLUMN],
        reason="pivot_requires_generation_path",
    )
    return base.drop_duplicates(subset=GENERATION_INDEX_COLUMNS)


def compute_generation_scores_pivot(
    scores: pd.DataFrame,
    *,
    allowed_eval_templates: Iterable[str],
    allowed_eval_models: Iterable[str],
    generation_path_lookup: pd.DataFrame | Mapping[tuple[str, str, str, str], str] | None = None,
) -> GenerationPivotResult:
    valid_scores = filter_valid_scores(scores)
    valid_row_count = len(valid_scores)
    if valid_row_count == 0:
        return GenerationPivotResult(pd.DataFrame(), [], 0, 0)

    _require_columns(
        valid_scores,
        GENERATION_INDEX_COLUMNS + [EVALUATION_TEMPLATE_COLUMN, EVALUATION_MODEL_COLUMN],
        reason="pivot_requires_columns",
    )

    allowed_templates = _normalize_allowed(allowed_eval_templates)
    allowed_models = _normalize_allowed(allowed_eval_models)

    if not allowed_templates or not allowed_models:
        return GenerationPivotResult(pd.DataFrame(), [], valid_row_count, 0)

    allowlisted = valid_scores[
        valid_scores[EVALUATION_TEMPLATE_COLUMN].astype(str).isin(allowed_templates)
        & valid_scores[EVALUATION_MODEL_COLUMN].astype(str).isin(allowed_models)
    ].copy()
    allowlisted_row_count = len(allowlisted)

    if allowlisted_row_count == 0:
        return GenerationPivotResult(pd.DataFrame(), [], valid_row_count, 0)

    _require_column(
        allowlisted,
        EVALUATION_MODEL_COLUMN,
        reason="pivot_requires_evaluator",
    )
    _require_column(
        allowlisted,
        "draft_template",
        reason="pivot_requires_draft_template",
    )

    allowlisted["eval_model_template"] = (
        allowlisted[EVALUATION_MODEL_COLUMN].astype(str)
        + "_"
        + allowlisted[EVALUATION_TEMPLATE_COLUMN].astype(str)
    )

    index_cols = GENERATION_INDEX_COLUMNS
    aggregations = (
        ("mean", "mean"),
        ("min", "min"),
        ("max", "max"),
        ("count", "count"),
    )
    pivot_tables = _pivot_tables(
        allowlisted,
        index_cols=index_cols,
        column_key="eval_model_template",
        value_column=SCORE_COLUMN,
        aggregations=aggregations,
    )
    suffix_map = {
        "mean": "",
        "min": "_min",
        "max": "_max",
        "count": "_n",
    }
    pivot_df, evaluation_columns = _combine_pivot_tables(
        pivot_tables,
        index_cols=index_cols,
        suffix_map=suffix_map,
    )

    score_sum_col = "allowlisted_template_score_sum"
    if evaluation_columns:
        pivot_df[score_sum_col] = (
            pivot_df[evaluation_columns]
            .sum(axis=1, skipna=True)
            .round(2)
        )
    else:
        pivot_df[score_sum_col] = 0.0

    path_lookup = _normalize_path_lookup(generation_path_lookup, scores)
    pivot_df = pivot_df.merge(
        path_lookup,
        on=index_cols,
        how="left",
    )

    stability_cols = [
        f"{column}{suffix}"
        for column in evaluation_columns
        for key, suffix in suffix_map.items()
        if suffix and f"{column}{suffix}" in pivot_df.columns and key != "mean"
    ]
    ordered_cols = (
        index_cols
        + evaluation_columns
        + stability_cols
        + [score_sum_col, GENERATION_RESPONSE_COLUMN]
    )
    pivot_df = pivot_df[ordered_cols]

    return GenerationPivotResult(
        frame=pivot_df,
        evaluation_columns=evaluation_columns,
        valid_row_count=valid_row_count,
        allowlisted_row_count=allowlisted_row_count,
    )


def compute_final_results(scores: pd.DataFrame) -> FinalResultsResult:
    valid_scores = filter_valid_scores(scores)
    valid_score_count = len(valid_scores)
    if valid_score_count == 0:
        return FinalResultsResult(pd.DataFrame(), 0)

    summaries: list[pd.DataFrame] = []

    def _create_summary(df: pd.DataFrame, group_cols: list[str], name_prefix: str) -> None:
        if df.empty:
            return
        grouped = df.groupby(group_cols)[SCORE_COLUMN].agg(
            [
                ("count", "count"),
                ("average", "mean"),
                ("std_dev", "std"),
                ("min_score", "min"),
                ("max_score", "max"),
                ("perfect_scores", lambda values: (values == 10.0).sum()),
                ("high_scores_8plus", lambda values: (values >= 8.0).sum()),
                ("low_scores_3minus", lambda values: (values <= 3.0).sum()),
            ]
        ).round(2)
        grouped["perfect_score_pct"] = (
            grouped["perfect_scores"] / grouped["count"] * 100
        ).round(1)
        grouped["high_score_pct"] = (
            grouped["high_scores_8plus"] / grouped["count"] * 100
        ).round(1)
        result = grouped.reset_index()
        result["analysis_type"] = name_prefix
        summaries.append(result)

    _create_summary(valid_scores, ["generation_template"], "by_generation_template")
    _create_summary(valid_scores, ["generation_model"], "by_generation_model")
    _create_summary(valid_scores, [EVALUATION_MODEL_COLUMN], "by_evaluation_model")
    _create_summary(valid_scores, ["combo_id"], "by_combo_id")
    _create_summary(
        valid_scores,
        ["generation_template", "generation_model"],
        "by_template_and_generation_model",
    )
    _create_summary(
        valid_scores,
        ["generation_model", EVALUATION_MODEL_COLUMN],
        "by_generation_vs_evaluation_model",
    )

    overall_stats = pd.DataFrame(
        [
            {
                "analysis_type": "overall_statistics",
                "count": len(valid_scores),
                "average": valid_scores[SCORE_COLUMN].mean(),
                "std_dev": valid_scores[SCORE_COLUMN].std(),
                "min_score": valid_scores[SCORE_COLUMN].min(),
                "max_score": valid_scores[SCORE_COLUMN].max(),
                "perfect_scores": (valid_scores[SCORE_COLUMN] == 10.0).sum(),
                "high_scores_8plus": (valid_scores[SCORE_COLUMN] >= 8.0).sum(),
                "low_scores_3minus": (valid_scores[SCORE_COLUMN] <= 3.0).sum(),
                "perfect_score_pct": (
                    (valid_scores[SCORE_COLUMN] == 10.0).sum()
                    / len(valid_scores)
                    * 100
                ),
                "high_score_pct": (
                    (valid_scores[SCORE_COLUMN] >= 8.0).sum()
                    / len(valid_scores)
                    * 100
                ),
            }
        ]
    ).round(2)
    summaries.append(overall_stats)

    final_summary = pd.concat(summaries, ignore_index=True) if summaries else pd.DataFrame()

    column_order = [
        "analysis_type",
        "generation_template",
        "generation_model",
        EVALUATION_MODEL_COLUMN,
        "combo_id",
        "count",
        "average",
        "std_dev",
        "min_score",
        "max_score",
        "perfect_scores",
        "perfect_score_pct",
        "high_scores_8plus",
        "high_score_pct",
        "low_scores_3minus",
    ]
    existing_columns = [column for column in column_order if column in final_summary.columns]
    if existing_columns:
        final_summary = final_summary[existing_columns]

    return FinalResultsResult(frame=final_summary, valid_score_count=valid_score_count)


def filter_perfect_score_rows(scores: pd.DataFrame) -> pd.DataFrame:
    valid_scores = filter_valid_scores(scores)
    perfect_scores = valid_scores[valid_scores[SCORE_COLUMN] == 10.0].copy()

    if perfect_scores.empty:
        return pd.DataFrame(
            columns=[
                "combo_id",
                "generation_template",
                "generation_model",
                EVALUATION_MODEL_COLUMN,
                SCORE_COLUMN,
                GENERATION_RESPONSE_COLUMN,
                EVALUATION_RESPONSE_COLUMN,
            ]
        )

    required_cols = [
        "combo_id",
        "generation_template",
        "generation_model",
        EVALUATION_MODEL_COLUMN,
        SCORE_COLUMN,
        GENERATION_RESPONSE_COLUMN,
        EVALUATION_RESPONSE_COLUMN,
    ]
    _require_columns(
        perfect_scores,
        required_cols,
        reason="perfect_score_columns_missing",
    )

    result_df = perfect_scores[required_cols].copy()
    result_df["notes"] = (
        "Perfect score from "
        + result_df["generation_model"].astype(str)
        + " generation + "
        + result_df[EVALUATION_MODEL_COLUMN].astype(str)
        + " evaluation"
    )
    return result_df


def compute_evaluation_model_template_pivot(
    scores: pd.DataFrame,
) -> EvaluationModelTemplatePivotResult:
    valid_scores = filter_valid_scores(scores)
    valid_row_count = len(valid_scores)
    if valid_row_count == 0:
        return EvaluationModelTemplatePivotResult(pd.DataFrame(), {}, 0)

    _require_columns(
        valid_scores,
        GENERATION_INDEX_COLUMNS + [EVALUATION_TEMPLATE_COLUMN, EVALUATION_MODEL_COLUMN],
        reason="evaluation_model_template_pivot_requires_columns",
    )

    valid_scores["eval_model_template"] = (
        valid_scores[EVALUATION_MODEL_COLUMN].astype(str)
        + "_"
        + valid_scores[EVALUATION_TEMPLATE_COLUMN].astype(str)
    )

    aggregations = (
        ("mean", "mean"),
        ("count", "count"),
    )
    pivot_tables = _pivot_tables(
        valid_scores,
        index_cols=GENERATION_INDEX_COLUMNS,
        column_key="eval_model_template",
        value_column=SCORE_COLUMN,
        aggregations=aggregations,
    )
    suffix_map = {"mean": ""}
    pivot_df, eval_columns = _combine_pivot_tables(
        pivot_tables,
        index_cols=GENERATION_INDEX_COLUMNS,
        suffix_map=suffix_map,
    )

    pivot_df = pivot_df.where(pd.notna(pivot_df), np.nan)

    total_generations = len(pivot_df)
    coverage_stats: dict[str, dict[str, float]] = {}
    count_frame = pivot_tables.get("count")

    for column in eval_columns:
        non_null_count = 0
        if count_frame is not None and column in count_frame.columns:
            non_null_count = int(count_frame[column].gt(0).sum())
        else:
            non_null_count = int(pivot_df[column].count())
        coverage_pct = (
            (non_null_count / total_generations * 100) if total_generations > 0 else 0.0
        )
        mean_score = pivot_df[column].mean()
        coverage_stats[column] = {
            "evaluations": non_null_count,
            "coverage_pct": round(float(coverage_pct), 1),
            "mean_score": (
                round(float(mean_score), 2)
                if non_null_count > 0 and not pd.isna(mean_score)
                else None
            ),
        }

    return EvaluationModelTemplatePivotResult(
        frame=pivot_df,
        coverage_stats=coverage_stats,
        valid_row_count=valid_row_count,
    )
