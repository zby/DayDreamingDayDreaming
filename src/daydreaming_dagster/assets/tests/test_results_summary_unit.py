"""Unit tests for results summary pure transformations."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import pandas as pd
import pytest

from daydreaming_dagster.results_summary.transformations import (
    GenerationPivotResult,
    compute_evaluation_model_template_pivot,
    compute_final_results,
    compute_generation_scores_pivot,
    filter_perfect_score_rows,
)
from daydreaming_dagster.utils.errors import DDError


GEN_RESPONSE_E1 = "gens/essay/E1/parsed.txt"
GEN_RESPONSE_E2 = "gens/essay/E2/parsed.txt"
EVAL_RESPONSE_E1 = "evaluations/essay/E1.json"
EVAL_RESPONSE_E2 = "evaluations/essay/E2.json"


def _scores_rows() -> pd.DataFrame:
    """Return a baseline aggregated scores frame used by multiple tests."""

    return pd.DataFrame(
        [
            {
                "combo_id": "combo_001",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "parent_gen_id": "E1",
                "evaluation_template": "daydreaming-verification-v2",
                "evaluation_llm_model": "deepseek",
                "score": 8.5,
                "error": None,
                "generation_response_path": GEN_RESPONSE_E1,
                "evaluation_response_path": EVAL_RESPONSE_E1,
            },
            {
                "combo_id": "combo_001",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "parent_gen_id": "E1",
                "evaluation_template": "creativity-metrics-v2",
                "evaluation_llm_model": "qwen",
                "score": 9.0,
                "error": None,
                "generation_response_path": GEN_RESPONSE_E1,
                "evaluation_response_path": EVAL_RESPONSE_E1,
            },
            {
                "combo_id": "combo_002",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "parent_gen_id": "E2",
                "evaluation_template": "daydreaming-verification-v2",
                "evaluation_llm_model": "deepseek",
                "score": 9.1,
                "error": None,
                "generation_response_path": GEN_RESPONSE_E2,
                "evaluation_response_path": EVAL_RESPONSE_E2,
            },
            {
                "combo_id": "combo_002",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "parent_gen_id": "E2",
                "evaluation_template": "creativity-metrics-v2",
                "evaluation_llm_model": "qwen",
                "score": None,
                "error": "parse_error",
                "generation_response_path": GEN_RESPONSE_E2,
                "evaluation_response_path": EVAL_RESPONSE_E2,
            },
        ]
    )


def _call_generation_pivot(
    *,
    scores: pd.DataFrame,
    templates: Iterable[str] = ("daydreaming-verification-v2", "creativity-metrics-v2"),
    models: Iterable[str] = ("deepseek", "qwen"),
    path_lookup: Mapping[tuple[str, str, str, str], str] | pd.DataFrame | None = None,
) -> GenerationPivotResult:
    return compute_generation_scores_pivot(
        scores,
        allowed_eval_templates=templates,
        allowed_eval_models=models,
        generation_path_lookup=path_lookup,
    )


def test_compute_generation_scores_pivot_includes_expected_columns():
    result = _call_generation_pivot(scores=_scores_rows())
    pivot = result.frame

    expected_cols = {
        "combo_id",
        "draft_template",
        "generation_template",
        "generation_model",
        "parent_gen_id",
        "deepseek_daydreaming-verification-v2",
        "qwen_creativity-metrics-v2",
        "deepseek_daydreaming-verification-v2_min",
        "deepseek_daydreaming-verification-v2_max",
        "deepseek_daydreaming-verification-v2_n",
        "allowlisted_template_score_sum",
        "generation_response_path",
    }
    assert expected_cols.issubset(set(pivot.columns))
    assert len(pivot) == 2
    assert result.valid_row_count == 3  # fourth row dropped due to error
    assert result.allowlisted_row_count == 3
    assert pivot.loc[pivot["combo_id"] == "combo_001", "generation_response_path"].iloc[0] == GEN_RESPONSE_E1


def test_compute_generation_scores_pivot_counts_are_integers():
    result = _call_generation_pivot(scores=_scores_rows())
    count_columns = [column for column in result.frame.columns if column.endswith("_n")]
    assert count_columns, "Expected count columns with _n suffix"
    for column in count_columns:
        assert pd.api.types.is_integer_dtype(result.frame[column])


def test_compute_generation_scores_pivot_respects_path_lookup_mapping():
    scores = _scores_rows().drop(columns=["generation_response_path"])
    paths = (GEN_RESPONSE_E1, GEN_RESPONSE_E1, GEN_RESPONSE_E2, GEN_RESPONSE_E2)
    mapping = {
        (
            row["combo_id"],
            row["draft_template"],
            row["generation_template"],
            row["generation_model"],
            row["parent_gen_id"],
        ): path
        for row, path in zip(scores.to_dict("records"), paths)
    }
    result = _call_generation_pivot(scores=scores, path_lookup=mapping)
    assert not result.frame["generation_response_path"].isna().any()


def test_compute_generation_scores_pivot_raises_without_paths():
    scores = _scores_rows().drop(columns=["generation_response_path"])
    with pytest.raises(DDError):
        _call_generation_pivot(scores=scores)


def test_compute_final_results_includes_overall_statistics():
    results = compute_final_results(_scores_rows())
    assert results.valid_score_count == 3
    assert "analysis_type" in results.frame.columns
    assert (results.frame["analysis_type"] == "overall_statistics").any()


def test_compute_evaluation_model_template_pivot_reports_coverage():
    pivot_result = compute_evaluation_model_template_pivot(_scores_rows())
    assert pivot_result.valid_row_count == 3
    pivot = pivot_result.frame
    expected = {"deepseek_daydreaming-verification-v2", "qwen_creativity-metrics-v2"}
    assert expected.issubset(set(pivot.columns))
    coverage = pivot_result.coverage_stats["deepseek_daydreaming-verification-v2"]
    assert coverage["evaluations"] == 2
    assert coverage["coverage_pct"] == 100.0


def test_filter_perfect_score_rows_returns_notes_column():
    scores = _scores_rows().copy()
    scores.loc[0, "score"] = 10.0
    scores.loc[1, "score"] = 10.0
    perfect = filter_perfect_score_rows(scores)
    assert len(perfect) == 2
    assert "notes" in perfect.columns
    assert perfect["notes"].str.contains("Perfect score").all()


def test_filter_perfect_score_rows_empty_frame_retains_schema():
    perfect = filter_perfect_score_rows(_scores_rows())
    expected_columns = {
        "combo_id",
        "generation_template",
        "generation_model",
        "evaluation_llm_model",
        "score",
        "generation_response_path",
        "evaluation_response_path",
    }
    assert set(perfect.columns) == expected_columns
    assert "notes" not in perfect.columns
    assert perfect.empty
