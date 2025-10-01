from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from daydreaming_dagster.assets.group_cohorts import (
    CohortBuilder,
    MEMBERSHIP_COLUMNS,
    persist_membership_csv,
    seed_cohort_metadata,
    validate_cohort_membership,
)
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.utils.errors import DDError


def _write_metadata(root: Path, stage: str, gen_id: str, payload: dict) -> Path:
    target = root / "gens" / stage / gen_id / "metadata.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload), encoding="utf-8")
    return target


def test_cohort_builder_build_from_essays_reads_existing_metadata(tmp_path: Path) -> None:
    draft_meta = {
        "combo_id": "combo-1",
        "template_id": "draft-tpl",
        "llm_model_id": "draft-llm",
        "replicate": 1,
    }
    essay_meta = {
        "template_id": "essay-tpl",
        "parent_gen_id": "draft-1",
        "llm_model_id": "essay-llm",
        "replicate": 2,
    }

    _write_metadata(tmp_path, "draft", "draft-1", draft_meta)
    _write_metadata(tmp_path, "essay", "essay-1", essay_meta)

    builder = CohortBuilder(
        cohort_id="cohort-1",
        data_layer=GensDataLayer(tmp_path),
        replication_config={"draft": 1, "essay": 1, "evaluation": 1},
    )

    rows = builder.build_from_essays(["essay-1"])

    assert [row.stage for row in rows] == ["draft", "essay"]
    draft_row, essay_row = rows
    assert draft_row.combo_id == "combo-1"
    assert draft_row.template_id == "draft-tpl"
    assert draft_row.llm_model_id == "draft-llm"
    assert essay_row.parent_gen_id == "draft-1"
    assert essay_row.template_id == "essay-tpl"
    assert essay_row.llm_model_id == "essay-llm"


def test_cohort_builder_build_from_drafts_allocates_new_essays(tmp_path: Path) -> None:
    draft_meta = {
        "combo_id": "combo-1",
        "template_id": "draft-tpl",
        "llm_model_id": "draft-llm",
        "replicate": 1,
    }
    _write_metadata(tmp_path, "draft", "draft-1", draft_meta)

    builder = CohortBuilder(
        cohort_id="cohort-1",
        data_layer=GensDataLayer(tmp_path),
        replication_config={"draft": 1, "essay": 2, "evaluation": 1},
    )

    rows = builder.build_from_drafts(
        ["draft-1"],
        essay_template_ids=["essay-tpl"],
    )

    assert len(rows) == 3  # one draft + two essays
    draft_rows = [row for row in rows if row.stage == "draft"]
    essay_rows = [row for row in rows if row.stage == "essay"]

    assert draft_rows[0].gen_id == "draft-1"
    assert {row.parent_gen_id for row in essay_rows} == {"draft-1"}
    assert {row.template_id for row in essay_rows} == {"essay-tpl"}
    assert {row.llm_model_id for row in essay_rows} == {"draft-llm"}
    assert {row.replicate for row in essay_rows} == {1, 2}


def test_cohort_builder_build_cartesian_expands_axes(tmp_path: Path) -> None:
    builder = CohortBuilder(
        cohort_id="cohort-1",
        data_layer=GensDataLayer(tmp_path),
        replication_config={"draft": 2, "essay": 1, "evaluation": 1},
    )

    rows = builder.build_cartesian(
        combo_ids=["combo-1"],
        draft_template_ids=["draft-tpl"],
        essay_template_ids=["essay-tpl"],
        generation_model_ids=["draft-llm"],
    )

    draft_rows = [row for row in rows if row.stage == "draft"]
    essay_rows = [row for row in rows if row.stage == "essay"]

    assert len(draft_rows) == 2  # replication config drives count
    assert len(essay_rows) == 2  # drafts × essay templates × essay reps
    assert {row.combo_id for row in draft_rows} == {"combo-1"}
    assert all(row.gen_id.startswith("d_") for row in draft_rows)
    assert all(row.gen_id.startswith("e_") for row in essay_rows)


def test_cohort_builder_build_evaluations_reuses_existing(tmp_path: Path) -> None:
    draft_meta = {
        "combo_id": "combo-1",
        "template_id": "draft-tpl",
        "llm_model_id": "draft-llm",
        "replicate": 1,
    }
    essay_meta = {
        "template_id": "essay-tpl",
        "parent_gen_id": "draft-1",
        "llm_model_id": "essay-llm",
        "replicate": 1,
    }
    evaluation_meta = {
        "template_id": "eval-tpl",
        "parent_gen_id": "essay-1",
        "llm_model_id": "eval-llm",
        "replicate": 1,
    }

    _write_metadata(tmp_path, "draft", "draft-1", draft_meta)
    _write_metadata(tmp_path, "essay", "essay-1", essay_meta)
    _write_metadata(tmp_path, "evaluation", "eval-existing", evaluation_meta)

    builder = CohortBuilder(
        cohort_id="cohort-1",
        data_layer=GensDataLayer(tmp_path),
        replication_config={"draft": 1, "essay": 1, "evaluation": 1},
    )

    builder.build_from_essays(["essay-1"])
    eval_rows, stats = builder.build_evaluations(
        evaluation_templates=["eval-tpl"],
        evaluation_models=["eval-llm"],
    )

    assert stats == {"created": 0, "fully_covered": 1}
    assert len(eval_rows) == 1
    assert eval_rows[0].gen_id == "eval-existing"
    assert eval_rows[0].parent_gen_id == "essay-1"


def test_cohort_builder_build_evaluations_allocates_missing(tmp_path: Path) -> None:
    builder = CohortBuilder(
        cohort_id="cohort-1",
        data_layer=GensDataLayer(tmp_path),
        replication_config={"draft": 1, "essay": 1, "evaluation": 2},
    )

    # Build essays via cartesian expansion to seed combo map.
    builder.build_cartesian(
        combo_ids=["combo-1"],
        draft_template_ids=["draft-tpl"],
        essay_template_ids=["essay-tpl"],
        generation_model_ids=["draft-llm"],
    )

    eval_rows, stats = builder.build_evaluations(
        evaluation_templates=["eval-tpl"],
        evaluation_models=["eval-llm"],
    )

    assert stats["created"] == 2  # two replicates requested, none existed
    assert stats["fully_covered"] == 0
    assert len(eval_rows) == 2
    assert {row.replicate for row in eval_rows} == {1, 2}
    assert all(row.gen_id.startswith("v_") for row in eval_rows)


def test_validate_cohort_membership_raises_on_missing_parents(tmp_path: Path) -> None:
    membership = pd.DataFrame(
        [
            {
                "stage": "evaluation",
                "gen_id": "eval-1",
                "origin_cohort_id": "cohort-1",
                "parent_gen_id": "essay-missing",
            }
        ],
        columns=MEMBERSHIP_COLUMNS,
    )

    with pytest.raises(DDError) as exc_info:
        validate_cohort_membership(membership, data_root=tmp_path)

    ctx = exc_info.value.ctx
    assert ctx["reason"] == "cohort_parent_integrity_failed"
    assert ctx["missing_essay_parents"] == ["essay-missing"]


def test_persist_membership_csv_deduplicates_rows(tmp_path: Path) -> None:
    membership = pd.DataFrame(
        [
            {"stage": "draft", "gen_id": "draft-1", "origin_cohort_id": "cohort-1"},
            {"stage": "draft", "gen_id": "draft-1", "origin_cohort_id": "cohort-1"},
            {"stage": "essay", "gen_id": "essay-1", "origin_cohort_id": "cohort-1"},
        ]
    )

    data_layer = GensDataLayer(tmp_path)
    slim_df, out_path = persist_membership_csv(
        cohort_id="cohort-1",
        membership=membership,
        data_layer=data_layer,
    )

    assert len(slim_df) == 2
    stored = pd.read_csv(out_path)
    assert stored.equals(slim_df)


def test_seed_cohort_metadata_creates_missing_files(tmp_path: Path) -> None:
    membership = pd.DataFrame(
        [
            {
                "stage": "draft",
                "gen_id": "draft-1",
                "origin_cohort_id": "cohort-1",
                "combo_id": "combo-1",
                "template_id": "draft-tpl",
                "llm_model_id": "draft-llm",
            }
        ]
    )

    data_layer = GensDataLayer(tmp_path)
    seed_cohort_metadata(
        data_layer=data_layer,
        cohort_id="cohort-1",
        membership=membership,
        template_modes={"draft": {"draft-tpl": "llm"}},
    )

    meta_path = data_layer.paths.metadata_path("draft", "draft-1")
    assert meta_path.exists()
    saved = json.loads(meta_path.read_text(encoding="utf-8"))
    assert saved["origin_cohort_id"] == "cohort-1"
    assert saved["template_id"] == "draft-tpl"
