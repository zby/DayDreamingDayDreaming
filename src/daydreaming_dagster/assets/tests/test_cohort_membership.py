from __future__ import annotations

import json
from pathlib import Path
import shutil

import pandas as pd
import pytest
import yaml
from dagster import Failure, build_asset_context

SPEC_FIXTURES = Path(__file__).resolve().parents[4] / "tests" / "fixtures" / "spec_dsl"

from daydreaming_dagster.assets.group_cohorts import cohort_membership
from daydreaming_dagster.utils.ids import (
    compute_deterministic_gen_id,
    draft_signature,
    essay_signature,
    evaluation_signature,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_spec(
    root: Path,
    *,
    cohort_id: str,
    combos: list[str],
    draft_templates: list[str],
    essay_templates: list[str],
    evaluation_templates: list[str],
    generation_llms: list[str],
    evaluation_llms: list[str],
    replicates: dict[str, dict[str, int | str]] | None = None,
) -> None:
    spec_dir = root / "cohorts" / cohort_id / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)

    allowed_matrix = [
        [tpl, model] for tpl in evaluation_templates for model in evaluation_llms
    ]
    if not allowed_matrix:
        allowed_matrix = [["placeholder-eval", "placeholder-model"]]

    spec: dict[str, object] = {
        "axes": {
            "combo_id": [str(item) for item in combos],
            "draft_template": [str(item) for item in draft_templates],
            "draft_llm": [str(item) for item in generation_llms],
            "essay_template": [str(item) for item in essay_templates],
            "essay_llm": [str(item) for item in generation_llms],
            "evaluation_template": [str(item) for item in evaluation_templates],
            "evaluation_llm": [str(item) for item in evaluation_llms],
        },
        "rules": {
            "pairs": {
                "evaluation_bundle": {
                    "left": "evaluation_template",
                    "right": "evaluation_llm",
                    "allowed": allowed_matrix,
                }
            }
        },
        "output": {
            "field_order": [
                "combo_id",
                "draft_template",
                "draft_llm",
                "essay_template",
                "essay_llm",
                "evaluation_template",
                "evaluation_llm",
            ]
        },
    }

    if replicates:
        spec["replicates"] = replicates
        replicate_columns = [f"{axis}_replicate" for axis in replicates]
        spec["output"]["field_order"].extend(replicate_columns)

    (spec_dir / "config.yaml").write_text(
        yaml.safe_dump(spec, sort_keys=False),
        encoding="utf-8",
    )


@pytest.fixture()
def base_data_root(tmp_path: Path) -> Path:
    # Common raw tables used by all scenarios
    raw = tmp_path / "1_raw"
    raw.mkdir(parents=True, exist_ok=True)
    _write_csv(
        raw / "replication_config.csv",
        [
            {"stage": "draft", "replicates": 1},
            {"stage": "essay", "replicates": 1},
            {"stage": "evaluation", "replicates": 1},
        ],
    )
    _write_csv(
        raw / "draft_templates.csv",
        [{"template_id": "draft-A", "generator": "llm"}],
    )
    _write_csv(
        raw / "essay_templates.csv",
        [{"template_id": "essay-X", "generator": "llm"}],
    )
    _write_csv(
        raw / "evaluation_templates.csv",
        [{"template_id": "eval-1", "generator": "llm"}],
    )
    _write_csv(
        raw / "llm_models.csv",
        [
            {"id": "gen-model", "for_generation": True, "for_evaluation": False},
            {"id": "eval-model", "for_generation": False, "for_evaluation": True},
        ],
    )
    _write_csv(
        tmp_path / "combo_mappings.csv",
        [{"combo_id": "combo-1"}],
    )

    _write_spec(
        tmp_path,
        cohort_id="cohort-curated",
        combos=["combo-1"],
        draft_templates=["draft-A"],
        essay_templates=["essay-X"],
        evaluation_templates=["eval-1"],
        generation_llms=["gen-model"],
        evaluation_llms=["eval-model"],
        replicates={
            "draft_template": 1,
            "essay_template": 1,
            "evaluation_template": 1,
        },
    )
    for extra_id in ("cohort-conflict", "cohort-with-comments"):
        _write_spec(
            tmp_path,
            cohort_id=extra_id,
            combos=["combo-1"],
            draft_templates=["draft-A"],
            essay_templates=["essay-X"],
            evaluation_templates=["eval-1"],
            generation_llms=["gen-model"],
            evaluation_llms=["eval-model"],
            replicates={
                "draft_template": 1,
                "essay_template": 1,
                "evaluation_template": 1,
            },
        )

    (tmp_path / "2_tasks").mkdir(parents=True, exist_ok=True)
    (tmp_path / "gens" / "draft").mkdir(parents=True, exist_ok=True)
    (tmp_path / "gens" / "essay").mkdir(parents=True, exist_ok=True)
    (tmp_path / "gens" / "evaluation").mkdir(parents=True, exist_ok=True)
    return tmp_path


def test_cohort_membership_curated_includes_all_stages(base_data_root: Path) -> None:
    """Test that curated mode always includes draft + essay + eval rows in membership."""
    data_root = base_data_root
    cohort_id = "cohort-curated"

    # Seed existing draft/essay metadata referenced by selected_essays
    draft_src = compute_deterministic_gen_id(
        "draft",
        draft_signature("combo-1", "draft-A", "gen-model", 1),
    )
    essay_src = compute_deterministic_gen_id(
        "essay",
        essay_signature(draft_src, "essay-X", 1),
    )
    _write_json(
        data_root / "gens" / "draft" / draft_src / "metadata.json",
        {
            "stage": "draft",
            "gen_id": draft_src,
            "combo_id": "combo-1",
            "template_id": "draft-A",
            "llm_model_id": "gen-model",
            "replicate": 1,
            "origin_cohort_id": "legacy-cohort",
        },
    )
    _write_json(
        data_root / "gens" / "essay" / essay_src / "metadata.json",
            {
                "stage": "essay",
                "gen_id": essay_src,
                "template_id": "essay-X",
                "parent_gen_id": draft_src,
                "llm_model_id": "gen-model",
                "replicate": 1,
                "origin_cohort_id": "legacy-cohort",
            },
        )

    # Selected essays list puts membership in curated mode
    (data_root / "2_tasks" / "selected_essays.txt").write_text(f"{essay_src}\n", encoding="utf-8")

    # Pretend a prior deterministic evaluation exists (replicate=1)
    existing_eval_id = compute_deterministic_gen_id(
        "evaluation",
        evaluation_signature(essay_src, "eval-1", "eval-model", 1),
    )
    _write_json(
        data_root / "gens" / "evaluation" / existing_eval_id / "metadata.json",
        {
            "stage": "evaluation",
            "gen_id": existing_eval_id,
            "parent_gen_id": essay_src,
            "template_id": "eval-1",
            "llm_model_id": "eval-model",
            "replicate": 1,
            "origin_cohort_id": "legacy-cohort",
        },
    )

    context = build_asset_context(resources={"data_root": str(data_root)})
    df = cohort_membership(
        context,
        cohort_id=cohort_id,
        selected_combo_mappings=pd.DataFrame(),
    )

    # Unified behavior: membership includes existing draft + essay + existing eval
    assert set(df["stage"]) == {"draft", "essay", "evaluation"}

    drafts = df[df["stage"] == "draft"]["gen_id"].tolist()
    essays = df[df["stage"] == "essay"]["gen_id"].tolist()
    evaluations = df[df["stage"] == "evaluation"]["gen_id"].tolist()

    # All existing gen_ids should be in membership
    assert draft_src in drafts
    assert essay_src in essays
    assert existing_eval_id in evaluations

    # All stages present in membership
    assert len(drafts) >= 1
    assert len(essays) >= 1
    assert len(evaluations) >= 1


def test_cohort_membership_cartesian_multiple_replicates(tmp_path: Path) -> None:
    data_root = tmp_path
    raw = data_root / "1_raw"
    raw.mkdir(parents=True, exist_ok=True)

    _write_csv(
        raw / "replication_config.csv",
        [
            {"stage": "draft", "replicates": 2},
            {"stage": "essay", "replicates": 2},
            {"stage": "evaluation", "replicates": 1},
        ],
    )
    _write_csv(
        raw / "draft_templates.csv",
        [{"template_id": "draft-A", "generator": "llm"}],
    )
    _write_csv(
        raw / "essay_templates.csv",
        [{"template_id": "essay-X", "generator": "llm"}],
    )
    _write_csv(
        raw / "evaluation_templates.csv",
        [{"template_id": "eval-1", "generator": "llm"}],
    )
    _write_csv(
        raw / "llm_models.csv",
        [
            {"id": "gen-model", "for_generation": True, "for_evaluation": False},
            {"id": "eval-model", "for_generation": False, "for_evaluation": True},
        ],
    )
    (data_root / "gens" / "draft").mkdir(parents=True, exist_ok=True)
    (data_root / "gens" / "essay").mkdir(parents=True, exist_ok=True)
    (data_root / "gens" / "evaluation").mkdir(parents=True, exist_ok=True)

    _write_csv(data_root / "combo_mappings.csv", [{"combo_id": "combo-1"}])

    _write_spec(
        data_root,
        cohort_id="cohort-cart",
        combos=["combo-1"],
        draft_templates=["draft-A"],
        essay_templates=["essay-X"],
        evaluation_templates=["eval-1"],
        generation_llms=["gen-model"],
        evaluation_llms=["eval-model"],
        replicates={
            "draft_template": 2,
            "essay_template": 2,
            "evaluation_template": 1,
        },
    )

    context = build_asset_context(resources={"data_root": str(data_root)})
    selected_df = pd.DataFrame(
        [
            {
                "combo_id": "combo-1",
                "concept_id": "c1",
                "description_level": "paragraph",
                "k_max": 2,
            }
        ]
    )
    df = cohort_membership(
        context,
        cohort_id="cohort-cart",
        selected_combo_mappings=selected_df,
    )

    drafts = df[df["stage"] == "draft"]["gen_id"].tolist()
    essays = df[df["stage"] == "essay"]["gen_id"].tolist()
    evaluations = df[df["stage"] == "evaluation"]["gen_id"].tolist()

    assert len(drafts) == 2  # two draft replicates
    assert len(essays) == 4  # drafts × essay replicates (2 * 2)
    assert len(evaluations) == 4  # one eval replicate per essay replicate

    # Deterministic ids should follow signature ordering without duplicates
    expected_first_draft = compute_deterministic_gen_id(
        "draft",
        draft_signature("combo-1", "draft-A", "gen-model", 1),
    )
    expected_second_draft = compute_deterministic_gen_id(
        "draft",
        draft_signature("combo-1", "draft-A", "gen-model", 2),
    )
    assert {expected_first_draft, expected_second_draft} == set(drafts)

    expected_first_essay = compute_deterministic_gen_id(
        "essay",
        essay_signature(expected_first_draft, "essay-X", 1),
    )
    assert expected_first_essay in essays

    # Every evaluation must point back to an essay from the same deterministic set
    essay_set = set(essays)
    for eval_id in evaluations:
        meta = json.loads(
            (data_root / "gens" / "evaluation" / eval_id / "metadata.json").read_text(encoding="utf-8")
        )
        assert meta.get("parent_gen_id") in essay_set


def test_cohort_membership_uses_spec_plan(tmp_path: Path) -> None:
    data_root = tmp_path
    raw = data_root / "1_raw"
    raw.mkdir(parents=True, exist_ok=True)

    _write_csv(
        raw / "replication_config.csv",
        [
            {"stage": "draft", "replicates": 2},
            {"stage": "essay", "replicates": 2},
            {"stage": "evaluation", "replicates": 1},
        ],
    )
    _write_csv(
        raw / "draft_templates.csv",
        [{"template_id": "draft-A", "generator": "llm"}],
    )
    _write_csv(
        raw / "essay_templates.csv",
        [
            {"template_id": "essay-X", "generator": "llm"},
            {"template_id": "essay-Y", "generator": "llm"},
        ],
    )
    _write_csv(
        raw / "evaluation_templates.csv",
        [{"template_id": "eval-1", "generator": "llm"}],
    )
    _write_csv(
        raw / "llm_models.csv",
        [
            {"id": "draft-llm", "for_generation": True, "for_evaluation": False},
            {"id": "essay-llm", "for_generation": True, "for_evaluation": False},
            {"id": "eval-llm", "for_generation": False, "for_evaluation": True},
        ],
    )

    (data_root / "gens" / "draft").mkdir(parents=True, exist_ok=True)
    (data_root / "gens" / "essay").mkdir(parents=True, exist_ok=True)
    (data_root / "gens" / "evaluation").mkdir(parents=True, exist_ok=True)

    _write_csv(
        data_root / "combo_mappings.csv",
        [
            {
                "combo_id": "combo-1",
                "version": "v1",
                "concept_id": "concept-a",
                "description_level": "paragraph",
                "k_max": 2,
                "created_at": "",
            }
        ],
    )

    spec_target = data_root / "cohorts" / "spec-cohort" / "spec"
    shutil.copytree(SPEC_FIXTURES / "cohort_cartesian", spec_target)

    context = build_asset_context(resources={"data_root": str(data_root)})
    selected_df = pd.DataFrame({"combo_id": ["combo-1"]})

    df = cohort_membership(
        context,
        cohort_id="spec-cohort",
        selected_combo_mappings=selected_df,
    )

    assert set(df["stage"].unique()) == {"draft", "essay", "evaluation"}

    draft_rows = df[df["stage"] == "draft"].copy()
    assert len(draft_rows) == 2
    assert set(draft_rows["replicate"].astype(int)) == {1, 2}
    assert set(draft_rows["llm_model_id"]) == {"draft-llm"}

    essay_rows = df[df["stage"] == "essay"].copy()
    assert len(essay_rows) == 8
    assert set(essay_rows["template_id"]) == {"essay-X", "essay-Y"}
    assert set(essay_rows["replicate"].astype(int)) == {1, 2}
    assert set(essay_rows["llm_model_id"]) == {"essay-llm"}

    evaluation_rows = df[df["stage"] == "evaluation"].copy()
    assert len(evaluation_rows) == 8
    assert set(evaluation_rows["template_id"]) == {"eval-1"}
    assert set(evaluation_rows["llm_model_id"]) == {"eval-llm"}
    assert set(evaluation_rows["replicate"].astype(int)) == {1}

    assert set(df["combo_id"].unique()) == {"combo-1"}


def test_curfated_selection_conflict_raises(base_data_root: Path) -> None:
    data_root = base_data_root
    select_dir = data_root / "2_tasks"
    select_dir.mkdir(parents=True, exist_ok=True)
    (select_dir / "selected_essays.txt").write_text("e_123\n", encoding="utf-8")
    (select_dir / "selected_drafts.txt").write_text("d_456\n", encoding="utf-8")

    context = build_asset_context(resources={"data_root": str(data_root)})
    with pytest.raises(Failure) as err:
        cohort_membership(
            context,
            cohort_id="cohort-conflict",
            selected_combo_mappings=pd.DataFrame(),
        )
    failure = err.value
    assert failure.metadata["error_code"].value == "INVALID_CONFIG"
    ctx = failure.metadata.get("error_ctx")
    if ctx is not None:
        assert ctx.data.get("reason") == "multiple_curated_inputs"



def test_curated_with_comments(base_data_root: Path) -> None:
    """Test that comment lines (non-directive) are handled properly."""
    data_root = base_data_root
    draft_src = compute_deterministic_gen_id(
        "draft",
        draft_signature("combo-1", "draft-A", "gen-model", 1),
    )
    essay_src = compute_deterministic_gen_id(
        "essay",
        essay_signature(draft_src, "essay-X", 1),
    )
    _write_json(
        data_root / "gens" / "draft" / draft_src / "metadata.json",
        {
            "stage": "draft",
            "gen_id": draft_src,
            "combo_id": "combo-1",
            "template_id": "draft-A",
            "llm_model_id": "gen-model",
            "replicate": 1,
        },
    )
    _write_json(
        data_root / "gens" / "essay" / essay_src / "metadata.json",
        {
            "stage": "essay",
            "gen_id": essay_src,
            "template_id": "essay-X",
            "parent_gen_id": draft_src,
            "llm_model_id": "gen-model",
            "replicate": 1,
        },
    )

    # Comment lines should be ignored (but not treated as directives)
    (data_root / "2_tasks" / "selected_essays.txt").write_text(
        "# Selected essays for testing\n" + essay_src + "\n",
        encoding="utf-8",
    )

    context = build_asset_context(resources={"data_root": str(data_root)})
    df = cohort_membership(
        context,
        cohort_id="cohort-with-comments",
        selected_combo_mappings=pd.DataFrame(),
    )

    # Should succeed and include all stages
    assert set(df["stage"]) == {"draft", "essay", "evaluation"}
