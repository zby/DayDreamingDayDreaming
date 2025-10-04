from __future__ import annotations

from collections import OrderedDict
from pathlib import Path

import pytest
import yaml

from daydreaming_dagster.spec_dsl import compile_design, parse_spec_mapping
from daydreaming_dagster.utils.errors import DDError, Err

from daydreaming_dagster.cohorts import (
    CohortCatalog,
    MembershipRow,
    validate_membership_against_catalog,
)
from daydreaming_dagster.cohorts.spec_planner import CohortDefinition


FIXTURES = Path(__file__).resolve().parents[4] / "tests" / "fixtures" / "spec_dsl"


def _catalogs() -> dict[str, list[str]]:
    return {
        "combo_id": ["combo-1"],
        "draft_template": ["draft-A"],
        "draft_llm": ["draft-llm"],
        "essay_template": ["essay-X", "essay-Y"],
        "essay_llm": ["essay-llm"],
        "evaluation_template": ["eval-1"],
        "evaluation_llm": ["eval-llm"],
    }


def test_cohort_plan_from_spec_bundle() -> None:
    spec_path = FIXTURES / "cohort_cartesian" / "config.yaml"
    payload = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    spec = parse_spec_mapping(payload, source=spec_path, base_dir=spec_path.parent)
    rows = compile_design(spec, catalogs=_catalogs())

    plan = CohortDefinition.from_design_rows(rows)

    assert len(plan.drafts) == 2
    assert {draft.replicate for draft in plan.drafts} == {1, 2}
    assert all(draft.combo_id == "combo-1" for draft in plan.drafts)
    assert all(draft.template_id == "draft-A" for draft in plan.drafts)
    assert all(draft.llm_model_id == "draft-llm" for draft in plan.drafts)

    assert len(plan.essays) == 8
    assert {essay.template_id for essay in plan.essays} == {"essay-X", "essay-Y"}
    assert {essay.replicate for essay in plan.essays} == {1, 2}
    assert all(essay.llm_model_id == "essay-llm" for essay in plan.essays)

    assert len(plan.evaluations) == 8
    assert {evaluation.template_id for evaluation in plan.evaluations} == {"eval-1"}
    assert {evaluation.llm_model_id for evaluation in plan.evaluations} == {"eval-llm"}
    assert {evaluation.replicate for evaluation in plan.evaluations} == {1}

    # Ensure evaluation entries keep references to essay/draft info
    for evaluation in plan.evaluations:
        assert evaluation.essay.draft.combo_id == "combo-1"
        assert evaluation.essay.draft.template_id == "draft-A"


def test_cohort_plan_missing_required_field_raises() -> None:
    bad_row = OrderedDict(
        [
            ("combo_id", "c-1"),
            ("draft_template", "draft-A"),
            # missing draft_llm
            ("essay_template", "essay-X"),
            ("essay_llm", "essay-llm"),
            ("evaluation_template", "eval-1"),
            ("evaluation_llm", "eval-llm"),
        ]
    )

    with pytest.raises(DDError) as excinfo:
        CohortDefinition.from_design_rows([bad_row])

    assert excinfo.value.code is Err.INVALID_CONFIG
    assert excinfo.value.ctx and excinfo.value.ctx.get("field") == "draft_llm"


def _cohort_catalog() -> CohortCatalog:
    return CohortCatalog.from_catalogs(
        _catalogs(),
        replication_config={"draft": 1, "essay": 1, "evaluation": 1},
    )


def test_validate_membership_against_catalog_passes() -> None:
    catalog = _cohort_catalog()
    rows = [
        MembershipRow(
            stage="draft",
            gen_id="draft-1",
            origin_cohort_id="cohort-1",
            combo_id="combo-1",
            template_id="draft-A",
            llm_model_id="draft-llm",
        ),
        MembershipRow(
            stage="essay",
            gen_id="essay-1",
            origin_cohort_id="cohort-1",
            parent_gen_id="draft-1",
            combo_id="combo-1",
            template_id="essay-X",
            llm_model_id="essay-llm",
        ),
        MembershipRow(
            stage="evaluation",
            gen_id="eval-1",
            origin_cohort_id="cohort-1",
            parent_gen_id="essay-1",
            combo_id="combo-1",
            template_id="eval-1",
            llm_model_id="eval-llm",
        ),
    ]

    validate_membership_against_catalog(rows, catalog=catalog)


def test_validate_membership_against_catalog_missing_combo_raises() -> None:
    catalog = _cohort_catalog()
    rows = [
        MembershipRow(
            stage="draft",
            gen_id="draft-1",
            origin_cohort_id="cohort-1",
            combo_id="missing-combo",
            template_id="draft-A",
            llm_model_id="draft-llm",
        )
    ]

    with pytest.raises(DDError) as excinfo:
        validate_membership_against_catalog(rows, catalog=catalog)

    assert excinfo.value.code is Err.INVALID_CONFIG
    assert excinfo.value.ctx["reason"] == "catalog_combos_missing"
    assert excinfo.value.ctx["missing"] == ["missing-combo"]
