from __future__ import annotations

from collections import OrderedDict
from pathlib import Path

import pytest

from daydreaming_dagster.spec_dsl import load_spec, compile_design
from daydreaming_dagster.utils.errors import DDError, Err

from daydreaming_dagster.cohorts.spec_planner import CohortPlan


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
    spec = load_spec(spec_path)
    rows = compile_design(spec, catalogs=_catalogs())

    plan = CohortPlan.from_design_rows(rows)

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
        CohortPlan.from_design_rows([bad_row])

    assert excinfo.value.code is Err.INVALID_CONFIG
    assert excinfo.value.ctx and excinfo.value.ctx.get("field") == "draft_llm"
