"""Focused unit tests for results summary assets."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from dagster import build_asset_context

import daydreaming_dagster.assets.results_summary as rs


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _prepare_data_root(data_root: Path) -> None:
    raw = data_root / "1_raw"
    raw.mkdir(parents=True, exist_ok=True)
    _write_csv(
        raw / "draft_templates.csv",
        [{"template_id": "links-v4"}],
    )
    _write_csv(
        raw / "essay_templates.csv",
        [{"template_id": "systematic-analytical-v2"}],
    )
    _write_csv(
        raw / "evaluation_templates.csv",
        [
            {"template_id": "daydreaming-verification-v2"},
            {"template_id": "creativity-metrics-v2"},
        ],
    )
    _write_csv(
        raw / "llm_models.csv",
        [
            {"id": "deepseek", "for_generation": True, "for_evaluation": True},
            {"id": "qwen", "for_generation": False, "for_evaluation": True},
        ],
    )
    _write_csv(
        raw / "replication_config.csv",
        [
            {"stage": "draft", "replicates": 1},
            {"stage": "essay", "replicates": 1},
            {"stage": "evaluation", "replicates": 1},
        ],
    )
    _write_csv(
        data_root / "combo_mappings.csv",
        [{"combo_id": "combo_001"}, {"combo_id": "combo_002"}],
    )

    spec_dir = data_root / "cohorts" / "cohort-test" / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)
    spec = {
        "axes": {
            "combo_id": {
                "levels": ["combo_001", "combo_002"],
                "catalog_lookup": {"catalog": "combos"},
            },
            "draft_template": {
                "levels": ["links-v4"],
                "catalog_lookup": {"catalog": "draft_templates"},
            },
            "draft_llm": {
                "levels": ["deepseek"],
                "catalog_lookup": {"catalog": "generation_llms"},
            },
            "essay_template": {
                "levels": ["systematic-analytical-v2"],
                "catalog_lookup": {"catalog": "essay_templates"},
            },
            "essay_llm": {
                "levels": ["deepseek"],
                "catalog_lookup": {"catalog": "essay_llms"},
            },
            "evaluation_template": {
                "levels": ["daydreaming-verification-v2", "creativity-metrics-v2"],
                "catalog_lookup": {"catalog": "evaluation_templates"},
            },
            "evaluation_llm": {
                "levels": ["deepseek", "qwen"],
                "catalog_lookup": {"catalog": "evaluation_llms"},
            },
        },
        "rules": [
            {
                "pair": {
                    "left": "evaluation_template",
                    "right": "evaluation_llm",
                    "name": "evaluation_bundle",
                    "allowed": [
                        ["daydreaming-verification-v2", "deepseek"],
                        ["creativity-metrics-v2", "qwen"],
                    ],
                }
            }
        ],
        "output": {
            "field_order": [
                "combo_id",
                "draft_template",
                "draft_llm",
                "essay_template",
                "essay_llm",
                "evaluation_template",
                "evaluation_llm",
            ]
        },
    }
    (spec_dir / "config.yaml").write_text(
        yaml.safe_dump(spec, sort_keys=False),
        encoding="utf-8",
    )


GEN_RESPONSE_E1 = "gens/essay/E1/parsed.txt"
GEN_RESPONSE_E2 = "gens/essay/E2/parsed.txt"


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
                "generation_response_path": GEN_RESPONSE_E1,
            },
            {
                "combo_id": "combo_001",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "evaluation_template": "creativity-metrics-v2",
                "evaluation_llm_model": "qwen",
                "score": 9.0,
                "error": None,
                "generation_response_path": GEN_RESPONSE_E1,
            },
        ]
    )


def test_generation_scores_pivot_smoke(tmp_path):
    _prepare_data_root(tmp_path)
    ctx = build_asset_context(resources={"data_root": str(tmp_path)}, partition_key="cohort-test")
    parsed = _make_parsed_scores()
    pivot = rs.generation_scores_pivot(ctx, parsed)

    # Should include evaluation columns combining model+template
    expected_cols = {
        "combo_id",
        "draft_template",
        "generation_template",
        "generation_model",
        "deepseek_daydreaming-verification-v2",
        "qwen_creativity-metrics-v2",
        "allowlisted_template_score_sum",
        "generation_response_path",
    }
    assert expected_cols.issubset(set(pivot.columns))
    # One row per generation
    assert len(pivot) == 1
    # Preserves response path from parsed_scores
    assert pivot.loc[0, "generation_response_path"] == GEN_RESPONSE_E1


def test_generation_scores_pivot_missing_evaluations_fill_counts(tmp_path):
    _prepare_data_root(tmp_path)
    ctx = build_asset_context(resources={"data_root": str(tmp_path)}, partition_key="cohort-test")
    parsed = pd.DataFrame(
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
                "generation_response_path": GEN_RESPONSE_E1,
            },
            {
                "combo_id": "combo_001",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "evaluation_template": "creativity-metrics-v2",
                "evaluation_llm_model": "qwen",
                "score": 9.0,
                "error": None,
                "generation_response_path": GEN_RESPONSE_E1,
            },
            {
                "combo_id": "combo_002",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "evaluation_template": "daydreaming-verification-v2",
                "evaluation_llm_model": "deepseek",
                "score": 9.1,
                "error": None,
                "generation_response_path": GEN_RESPONSE_E2,
            },
            {
                "combo_id": "combo_002",
                "stage": "essay2p",
                "draft_template": "links-v4",
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f",
                "evaluation_template": "creativity-metrics-v2",
                "evaluation_llm_model": "qwen",
                "score": None,
                "error": "parse_error",
                "generation_response_path": GEN_RESPONSE_E2,
            },
        ]
    )

    pivot = rs.generation_scores_pivot(ctx, parsed)

    # combo_002 only has a valid deepseek evaluation; qwen entry is filtered out
    row_combo_002 = pivot[pivot["combo_id"] == "combo_002"].iloc[0]
    assert row_combo_002["deepseek_daydreaming-verification-v2_n"] == 1
    assert row_combo_002["qwen_creativity-metrics-v2_n"] == 0

    count_columns = [c for c in pivot.columns if c.endswith("_n")]
    for col in count_columns:
        assert pd.api.types.is_integer_dtype(pivot[col].dtype)


def test_final_results_overall_and_by_template(tmp_path):
    _prepare_data_root(tmp_path)
    ctx = build_asset_context(resources={"data_root": str(tmp_path)}, partition_key="cohort-test")
    parsed = _make_parsed_scores()
    out = rs.final_results(ctx, parsed)

    assert not out.empty
    assert "analysis_type" in out.columns
    # Should include at least the overall row
    assert (out["analysis_type"] == "overall_statistics").any()
    # Should include a by_generation_template row for our template
    assert (out.get("generation_template") == "systematic-analytical-v2").any()


def test_evaluation_model_template_pivot(tmp_path):
    _prepare_data_root(tmp_path)
    ctx = build_asset_context(resources={"data_root": str(tmp_path)}, partition_key="cohort-test")
    parsed = _make_parsed_scores()
    pivot = rs.evaluation_model_template_pivot(ctx, parsed)

    # Columns include combined model+template (from function's logic)
    # Note: evaluation_model_template_pivot uses evaluation_llm_model + '_' + evaluation_template
    expected_combined = {"deepseek_daydreaming-verification-v2", "qwen_creativity-metrics-v2"}
    assert expected_combined.issubset(set(pivot.columns))
    # One row per generation
    assert len(pivot) == 1
