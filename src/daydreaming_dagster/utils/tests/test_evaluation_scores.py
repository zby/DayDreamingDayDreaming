from __future__ import annotations

import json

import pandas as pd

from daydreaming_dagster.data_layer.paths import Paths

from daydreaming_dagster.utils.evaluation_scores import (
    aggregate_evaluation_scores_for_ids,
    parent_ids_missing_template,
    rank_parent_essays_by_template,
    _rows_to_dataframe,
    _EvaluationScoreAggregator,
)


def _make_aggregator(tmp_path, docs, raw_metadata=None):
    paths = Paths.from_str(str(tmp_path))

    def _loader(_root, stage, gen_id):
        return docs[(stage, gen_id)]

    aggregator = _EvaluationScoreAggregator(paths=paths, load_generation_fn=_loader)
    if raw_metadata is not None:
        aggregator._load_raw_metadata = lambda gid: raw_metadata.get(gid)  # type: ignore[method-assign]
    else:
        aggregator._load_raw_metadata = lambda gid: None  # type: ignore[method-assign]
    return aggregator


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

    (eval_dir / "raw_metadata.json").write_text(
        json.dumps({"input_mode": "copy", "copied_from": str((essay_dir / "parsed.txt").resolve())}),
        encoding="utf-8",
    )

    (eval_dir / "metadata.json").write_text(
        json.dumps(
            {
                "template_id": "eval-template",
                "llm_model_id": "eval-llm",
                "parent_gen_id": essay_id,
                "origin_cohort_id": "cohort-42",
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
    assert row["origin_cohort_id"] == "cohort-42"
    assert row["generation_response_path"] == str((essay_dir / "parsed.txt").resolve())
    assert row["input_mode"] == "copy"
    assert row["copied_from"].endswith("essay/essay-123/parsed.txt")
    assert "stage" not in df.columns
    assert "evaluation_model" not in df.columns


def test_rank_parent_essays_by_template_aggregates_max_and_orders():
    df = pd.DataFrame(
        [
            {"parent_gen_id": "e1", "evaluation_template": "novelty", "evaluation_llm_model": "m1", "score": 8.0},
            {"parent_gen_id": "e1", "evaluation_template": "novelty", "evaluation_llm_model": "m1", "score": 7.5},
            {"parent_gen_id": "e1", "evaluation_template": "novelty", "evaluation_llm_model": "m2", "score": 7.0},
            {"parent_gen_id": "e2", "evaluation_template": "novelty", "evaluation_llm_model": "m1", "score": 8.5},
            {
                "parent_gen_id": "e2",
                "evaluation_template": "novelty",
                "evaluation_llm_model": "m2",
                "score": 8.0,
                "error": None,
            },
            {"parent_gen_id": "e3", "evaluation_template": "novelty", "evaluation_llm_model": "m1", "score": 6.0},
            {
                "parent_gen_id": "e3",
                "evaluation_template": "novelty",
                "evaluation_llm_model": "m2",
                "score": None,
                "error": "parse failure",
            },
        ]
    )

    ranked = rank_parent_essays_by_template(df, template_id="novelty")
    assert list(ranked["parent_gen_id"]) == ["e2", "e1", "e3"]
    # e2 average of (8.5, 8.0) = 8.25; e1 average of (8.0, 7.0) = 7.5; e3 only one model 6.0
    assert ranked.loc[ranked["parent_gen_id"] == "e2", "avg_score"].iloc[0] == 8.25
    assert ranked.loc[ranked["parent_gen_id"] == "e1", "avg_score"].iloc[0] == 7.5
    # Missing/error scores should not count towards coverage
    assert ranked.loc[ranked["parent_gen_id"] == "e3", "models_covered"].iloc[0] == 1
    # Per-model columns exist and reflect max per model
    assert ranked.loc[ranked["parent_gen_id"] == "e2", "score__m1"].iloc[0] == 8.5
    assert ranked.loc[ranked["parent_gen_id"] == "e1", "score__m1"].iloc[0] == 8.0
    assert pd.isna(ranked.loc[ranked["parent_gen_id"] == "e3", "score__m2"].iloc[0])


def test_rank_parent_essays_by_template_honors_score_span_and_top_n():
    df = pd.DataFrame(
        [
            {"parent_gen_id": "e1", "evaluation_template": "novelty", "evaluation_llm_model": "m1", "score": 8.0},
            {"parent_gen_id": "e2", "evaluation_template": "novelty", "evaluation_llm_model": "m1", "score": 7.9},
            {"parent_gen_id": "e3", "evaluation_template": "novelty", "evaluation_llm_model": "m1", "score": 7.7},
            {"parent_gen_id": "e4", "evaluation_template": "novelty", "evaluation_llm_model": "m1", "score": 6.5},
        ]
    )

    ranked = rank_parent_essays_by_template(
        df,
        template_id="novelty",
        score_span=0.25,
    )
    assert list(ranked["parent_gen_id"]) == ["e1", "e2"]

    ranked_top1 = rank_parent_essays_by_template(
        df,
        template_id="novelty",
        top_n=1,
    )
    assert list(ranked_top1["parent_gen_id"]) == ["e1"]


def test_parent_ids_missing_template_highlights_missing_ids():
    df = pd.DataFrame(
        [
            {"parent_gen_id": "e1", "evaluation_template": "novelty-v2", "evaluation_llm_model": "m1", "score": 8.0},
            {"parent_gen_id": "e2", "evaluation_template": "novelty-v2", "evaluation_llm_model": "m1", "score": None},
            {"parent_gen_id": "e3", "evaluation_template": "novelty-v2", "evaluation_llm_model": "m1", "score": 7.0},
            {"parent_gen_id": "e3", "evaluation_template": "novelty-v2", "evaluation_llm_model": "m2", "score": 7.5},
        ]
    )

    missing = parent_ids_missing_template(df, ["e1", "e2", "e3", "e4"], template_id="novelty-v2")
    assert missing == ["e2", "e4"]


def test_aggregate_scores_reports_invalid_parsed(tmp_path):
    eval_id = "eval-invalid"
    docs = {
        ("evaluation", eval_id): {
            "metadata": {
                "template_id": "eval-template",
                "llm_model_id": "eval-llm",
                "parent_gen_id": None,
            },
            "parsed_text": "bad score",
        }
    }

    aggregator = _make_aggregator(tmp_path, docs)
    rows = aggregator.collect_rows([eval_id])
    df = _rows_to_dataframe(rows)
    assert len(df) == 1

    row = df.iloc[0]
    assert pd.isna(row["score"])
    assert row["error"].startswith("Invalid parsed.txt:")


def test_rows_to_dataframe_backfills_expected_columns():
    df = _rows_to_dataframe([])
    expected = {
        "gen_id",
        "parent_gen_id",
        "evaluation_template",
        "evaluation_llm_model",
        "score",
        "error",
        "evaluation_response_path",
        "combo_id",
        "draft_template",
        "generation_template",
        "generation_model",
        "generation_response_path",
        "origin_cohort_id",
        "input_mode",
        "copied_from",
    }
    assert expected.issubset(set(df.columns))
