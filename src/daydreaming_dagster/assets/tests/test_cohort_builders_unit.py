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
