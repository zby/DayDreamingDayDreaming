from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from daydreaming_dagster.cohorts import (
    CohortCatalog,
    CohortDefinition,
    DraftPlanEntry,
    EssayPlanEntry,
    EvaluationPlanEntry,
    InMemoryGenerationRegistry,
    MEMBERSHIP_COLUMNS,
    generate_membership,
    persist_membership_csv,
    seed_cohort_metadata,
    validate_cohort_definition,
    validate_cohort_membership,
)
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.utils.errors import DDError


def test_generate_membership_builds_rows_in_order() -> None:
    draft_entry = DraftPlanEntry(
        combo_id="combo-1",
        template_id="draft-tpl",
        llm_model_id="draft-llm",
        replicate=1,
    )
    essay_entry = EssayPlanEntry(
        draft=draft_entry,
        template_id="essay-tpl",
        llm_model_id="essay-llm",
        replicate=1,
    )
    evaluation_entry = EvaluationPlanEntry(
        essay=essay_entry,
        template_id="eval-tpl",
        llm_model_id="eval-llm",
        replicate=1,
    )
    definition = CohortDefinition(
        drafts=[draft_entry],
        essays=[essay_entry],
        evaluations=[evaluation_entry],
    )

    registry = InMemoryGenerationRegistry()
    rows = generate_membership(
        definition,
        cohort_id="cohort-1",
        registry=registry,
    )

    assert [row.stage for row in rows] == ["draft", "essay", "evaluation"]
    assert rows[0].origin_cohort_id == "cohort-1"
    assert rows[1].parent_gen_id == rows[0].gen_id
    assert rows[2].parent_gen_id == rows[1].gen_id


def test_validate_cohort_definition_rejects_unknown_combo() -> None:
    definition = CohortDefinition(
        drafts=[
            DraftPlanEntry(
                combo_id="combo-missing",
                template_id="draft-tpl",
                llm_model_id="draft-llm",
                replicate=1,
            )
        ]
    )
    catalog = CohortCatalog.from_catalogs(
        {
            "combo_id": ["combo-known"],
            "draft_template": ["draft-tpl"],
            "essay_template": ["essay-tpl"],
            "evaluation_template": ["eval-tpl"],
            "draft_llm": ["draft-llm"],
            "essay_llm": ["essay-llm"],
            "evaluation_llm": ["eval-llm"],
        }
    )

    with pytest.raises(DDError) as exc_info:
        validate_cohort_definition(definition, catalog=catalog)

    ctx = exc_info.value.ctx
    assert ctx["reason"] == "catalog_combos_missing"
    assert ctx["missing"] == ["combo-missing"]


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
        data_root=data_layer.data_root,
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
        data_root=data_layer.data_root,
        cohort_id="cohort-1",
        membership=membership,
        template_modes={"draft": {"draft-tpl": "llm"}},
    )

    meta_path = data_layer.paths.metadata_path("draft", "draft-1")
    assert meta_path.exists()
    saved = json.loads(meta_path.read_text(encoding="utf-8"))
    assert saved["origin_cohort_id"] == "cohort-1"
    assert saved["template_id"] == "draft-tpl"
