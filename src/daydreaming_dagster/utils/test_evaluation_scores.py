from __future__ import annotations

import json

from daydreaming_dagster.utils.evaluation_scores import aggregate_evaluation_scores_for_ids


def test_aggregate_scores_prefers_llm_ids_and_omits_stage(tmp_path):
    data_root = tmp_path
    eval_id = "eval-001"
    essay_id = "essay-123"
    draft_id = "draft-999"

    eval_dir = data_root / "gens" / "evaluation" / eval_id
    essay_dir = data_root / "gens" / "essay" / essay_id
    draft_dir = data_root / "gens" / "draft" / draft_id

    eval_dir.mkdir(parents=True, exist_ok=True)
    essay_dir.mkdir(parents=True, exist_ok=True)
    draft_dir.mkdir(parents=True, exist_ok=True)

    (eval_dir / "parsed.txt").write_text("Score\n8.5\n", encoding="utf-8")
    (essay_dir / "parsed.txt").write_text("essay body", encoding="utf-8")

    (eval_dir / "metadata.json").write_text(
        json.dumps(
            {
                "template_id": "eval-template",
                "llm_model_id": "eval-llm",
                "parent_gen_id": essay_id,
                "cohort_id": "cohort-42",
            }
        ),
        encoding="utf-8",
    )

    (essay_dir / "metadata.json").write_text(
        json.dumps(
            {
                "template_id": "essay-template",
                "llm_model_id": "essay-llm",
                "parent_gen_id": draft_id,
            }
        ),
        encoding="utf-8",
    )

    (draft_dir / "metadata.json").write_text(
        json.dumps(
            {
                "template_id": "draft-template",
                "llm_model_id": "draft-llm",
                "combo_id": "combo-7",
            }
        ),
        encoding="utf-8",
    )

    df = aggregate_evaluation_scores_for_ids(data_root, [eval_id])
    assert len(df) == 1

    row = df.iloc[0]
    assert row["gen_id"] == eval_id
    assert row["generation_template"] == "essay-template"
    assert row["draft_template"] == "draft-template"
    assert row["generation_model"] == "essay-llm"
    assert row["combo_id"] == "combo-7"
    assert row["evaluation_llm_model"] == "eval-llm"
    assert row["cohort_id"] == "cohort-42"
    assert row["generation_response_path"] == str((essay_dir / "parsed.txt").resolve())
    assert "stage" not in df.columns
    assert "evaluation_model" not in df.columns
