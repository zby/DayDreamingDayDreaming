"""Focused unit tests for aggregated_scores asset.

Avoids filesystem I/O by monkeypatching the score parser and membership lookup.
Ensures:
- passthrough of enriched columns from parse_all
- filtering to current cohort (evaluation stage)
"""

import pandas as pd
from dagster import build_asset_context

from daydreaming_dagster.assets.results_processing import aggregated_scores


def test_aggregated_scores_filters_and_passthrough(tmp_path, monkeypatch):
    # Monkeypatch the aggregator to avoid filesystem I/O; include two rows
    def fake_parse_all_scores(_data_root, _out_csv):
        return pd.DataFrame(
            [
                {
                    "gen_id": "E123",
                    "parent_gen_id": "D999",
                    "evaluation_template": "eval-tpl",
                    "evaluation_llm_model": "deepseek",
                    "score": 9.0,
                    "error": None,
                    "combo_id": "combo_1",
                    "draft_template": "links-v1",
                    "generation_template": "essay-tpl",
                    "generation_model": "deepseek_r1_f",
                    "stage": "essay2p",
                    "generation_response_path": str(tmp_path / "gens" / "essay" / "D999" / "parsed.txt"),
                },
                {
                    "gen_id": "E456",
                    "parent_gen_id": "D111",
                    "evaluation_template": "eval-tpl",
                    "evaluation_llm_model": "qwen",
                    "score": 8.0,
                    "error": None,
                    "combo_id": "combo_2",
                    "draft_template": "links-v2",
                    "generation_template": "essay-tpl",
                    "generation_model": "qwen_q4",
                    "stage": "essay2p",
                    "generation_response_path": str(tmp_path / "gens" / "essay" / "D111" / "parsed.txt"),
                },
            ]
        )

    # Only keep E123 in cohort filter
    def fake_stage_gen_ids(_data_root, stage):
        assert stage == "evaluation"
        return ["E123"]

    monkeypatch.setattr(
        "daydreaming_dagster.assets.results_processing.parse_all_scores",
        fake_parse_all_scores,
    )
    monkeypatch.setattr(
        "daydreaming_dagster.assets.results_processing.stage_gen_ids",
        fake_stage_gen_ids,
    )

    ctx = build_asset_context(resources={"data_root": str(tmp_path)})
    df = aggregated_scores(ctx)

    # Filtered to cohort
    assert set(df["gen_id"]) == {"E123"}
    # Passthrough of enriched fields
    assert set(["evaluation_llm_model", "generation_response_path", "draft_template"]).issubset(df.columns)
