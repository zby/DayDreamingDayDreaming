"""Focused unit tests for aggregated_scores using pure-function core.

Ensures:
- passthrough of enriched columns from aggregator
- filtering to current cohort (evaluation stage)
"""

import pandas as pd
from daydreaming_dagster.assets.results_processing import aggregated_scores_impl


def test_aggregated_scores_filters_and_passthrough(tmp_path):
    # Fake aggregator returns two rows; asset should filter to membership service ids
    class _Agg:
        def parse_all_scores(self, _data_root, gen_ids):
            df_all = pd.DataFrame(
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
                        "generation_response_path": str(tmp_path / "gens" / "essay" / "D111" / "parsed.txt"),
                    },
                ]
            )
            if gen_ids:
                return df_all[df_all["gen_id"].isin(set(map(str, gen_ids)))].reset_index(drop=True)
            return df_all

    class _Membership:
        def __init__(self):
            self.calls: list[tuple[str | None, str]] = []

        def stage_gen_ids(self, _data_root, stage, cohort_id=None):
            assert stage == "evaluation"
            self.calls.append((cohort_id, stage))
            return ["E123"]

    membership = _Membership()
    df = aggregated_scores_impl(
        tmp_path,
        scores_aggregator=_Agg(),
        membership_service=membership,
        cohort_id="COHORT-Unit",
    )

    # Filtered to cohort by the helper's gen_id list
    assert set(df["gen_id"]) == {"E123"}
    assert membership.calls == [("COHORT-Unit", "evaluation")]
    # Passthrough of enriched fields (stage should not be emitted)
    assert "stage" not in df.columns
    assert "evaluation_model" not in df.columns
    assert {"evaluation_llm_model", "generation_response_path", "draft_template", "combo_id"}.issubset(df.columns)
