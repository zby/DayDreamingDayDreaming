"""Focused unit tests for parsed_scores enrichment logic.

Avoids filesystem I/O by monkeypatching the score parser. Ensures:
- evaluation_model -> evaluation_llm_model mapping
- generation_response_path built from data_root/gens/essay/<parent>/parsed.txt
- stage defaults to essay2p
"""

import pandas as pd
from dagster import build_asset_context

from daydreaming_dagster.assets.results_processing import parsed_scores


def test_parsed_scores_enrichment_minimal(tmp_path, monkeypatch):
    # Monkeypatch the raw parser to avoid filesystem I/O
    def fake_parse_all_scores(_data_root, _out_csv):
        return pd.DataFrame(
            [
                {
                    "gen_id": "E123",
                    "parent_gen_id": "D999",
                    "evaluation_model": "deepseek_r1_f",
                    "evaluation_task_id": "EVAL-T1",
                    "score": 9.0,
                    "error": None,
                }
            ]
        )

    monkeypatch.setattr(
        "daydreaming_dagster.assets.results_processing.parse_all_scores",
        fake_parse_all_scores,
    )

    # Keep evaluation_tasks focused to the relevant gen_id
    eval_tasks = pd.DataFrame([
        {"gen_id": "E123"},
    ])

    ctx = build_asset_context(resources={"data_root": str(tmp_path)})
    df = parsed_scores(ctx, evaluation_tasks=eval_tasks)

    # Columns mapped/enriched
    assert "evaluation_llm_model" in df.columns
    assert df.loc[0, "evaluation_llm_model"] == "deepseek_r1_f"
    # Defaults when metadata files are not read
    assert df.loc[0, "stage"] == "essay2p"

    # Path construction is deterministic from data_root + gens + essay + parent + parsed.txt
    expected_path = str(tmp_path / "gens" / "essay" / "D999" / "parsed.txt")
    assert df.loc[0, "generation_response_path"] == expected_path

