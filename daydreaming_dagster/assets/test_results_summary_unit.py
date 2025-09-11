"""Focused unit tests for results summary assets.

Covers:
- generation_scores_pivot: pivot shape, eval columns, response path preservation
- final_results: overall statistics and by-template grouping presence
- evaluation_model_template_pivot: combined columns and metadata-like structure

These tests avoid file I/O by passing in-memory DataFrames and monkeypatching
read_evaluation_templates used by generation_scores_pivot.
"""

import pandas as pd
from dagster import build_asset_context

import daydreaming_dagster.assets.results_summary as rs


def _make_parsed_scores():
    # Minimal parsed_scores input with required columns
    return pd.DataFrame(
        [
            {
                "combo_id": "combo_001",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "evaluation_template": "daydreaming-verification-v2",
                "evaluation_llm_model": "deepseek",
                "score": 8.5,
                "error": None,
                "generation_response_path": "data/3_generation/essay_responses/E1.txt",
            },
            {
                "combo_id": "combo_001",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "evaluation_template": "creativity-metrics",
                "evaluation_llm_model": "qwen",
                "score": 9.0,
                "error": None,
                "generation_response_path": "data/3_generation/essay_responses/E1.txt",
            },
        ]
    )


def test_generation_scores_pivot_smoke(monkeypatch, tmp_path):
    # Monkeypatch evaluation templates reader to return active templates
    monkeypatch.setattr(
        rs,
        "read_evaluation_templates",
        lambda _root: pd.DataFrame([
            {"template_id": "daydreaming-verification-v2", "active": True},
            {"template_id": "creativity-metrics", "active": True},
        ]),
    )

    ctx = build_asset_context(resources={"data_root": str(tmp_path)})
    parsed = _make_parsed_scores()
    pivot = rs.generation_scores_pivot(ctx, parsed)

    # Should include evaluation columns combining template+model
    expected_cols = {
        "combo_id",
        "stage",
        "draft_template",
        "generation_template",
        "generation_model",
        "daydreaming-verification-v2_deepseek",
        "creativity-metrics_qwen",
        "sum_scores",
        "generation_response_path",
    }
    assert expected_cols.issubset(set(pivot.columns))
    # One row per generation
    assert len(pivot) == 1
    # Preserves response path from parsed_scores
    assert pivot.loc[0, "generation_response_path"] == "data/3_generation/essay_responses/E1.txt"


def test_final_results_overall_and_by_template(tmp_path):
    ctx = build_asset_context(resources={"data_root": str(tmp_path)})
    parsed = _make_parsed_scores()
    out = rs.final_results(ctx, parsed)

    assert not out.empty
    assert "analysis_type" in out.columns
    # Should include at least the overall row
    assert (out["analysis_type"] == "overall_statistics").any()
    # Should include a by_generation_template row for our template
    assert (out.get("generation_template") == "systematic-analytical-v2").any()


def test_evaluation_model_template_pivot(tmp_path):
    ctx = build_asset_context(resources={"data_root": str(tmp_path)})
    parsed = _make_parsed_scores()
    pivot = rs.evaluation_model_template_pivot(ctx, parsed)

    # Columns include combined model+template (from function's logic)
    # Note: evaluation_model_template_pivot uses evaluation_llm_model + '_' + evaluation_template
    expected_combined = {"deepseek_daydreaming-verification-v2", "qwen_creativity-metrics"}
    assert expected_combined.issubset(set(pivot.columns))
    # One row per generation
    assert len(pivot) == 1

