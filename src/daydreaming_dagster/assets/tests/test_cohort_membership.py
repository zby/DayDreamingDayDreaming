from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from dagster import Failure, build_asset_context

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
        [{"template_id": "draft-A", "active": True, "generator": "llm"}],
    )
    _write_csv(
        raw / "essay_templates.csv",
        [{"template_id": "essay-X", "active": True, "generator": "llm"}],
    )
    _write_csv(
        raw / "evaluation_templates.csv",
        [{"template_id": "eval-1", "active": True, "generator": "llm"}],
    )
    _write_csv(
        raw / "llm_models.csv",
        [
            {"id": "gen-model", "for_generation": True, "for_evaluation": False},
            {"id": "eval-model", "for_generation": False, "for_evaluation": True},
        ],
    )
    (tmp_path / "2_tasks").mkdir(parents=True, exist_ok=True)
    (tmp_path / "gens" / "draft").mkdir(parents=True, exist_ok=True)
    (tmp_path / "gens" / "essay").mkdir(parents=True, exist_ok=True)
    (tmp_path / "gens" / "evaluation").mkdir(parents=True, exist_ok=True)
    return tmp_path


def test_cohort_membership_curated_regenerate_allocates_new_replicates(base_data_root: Path) -> None:
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

    # Pretend a prior deterministic run exists (replicate=1)
    existing_draft_id = draft_src
    existing_essay_id = essay_src
    existing_eval_id = compute_deterministic_gen_id(
        "evaluation",
        evaluation_signature(existing_essay_id, "eval-1", "eval-model", 1),
    )
    _write_json(
        data_root / "gens" / "evaluation" / existing_eval_id / "metadata.json",
        {
            "stage": "evaluation",
            "gen_id": existing_eval_id,
            "parent_gen_id": existing_essay_id,
            "template_id": "eval-1",
            "llm_model_id": "eval-model",
            "replicate": 1,
            "origin_cohort_id": "legacy-cohort",
        },
    )

    context = build_asset_context(resources={"data_root": str(data_root)})
    df = cohort_membership(context, cohort_id=cohort_id)

    expected_draft = compute_deterministic_gen_id(
        "draft",
        draft_signature("combo-1", "draft-A", "gen-model", 2),
    )
    expected_essay = compute_deterministic_gen_id(
        "essay",
        essay_signature(expected_draft, "essay-X", 1),
    )
    expected_eval = compute_deterministic_gen_id(
        "evaluation",
        evaluation_signature(expected_essay, "eval-1", "eval-model", 1),
    )

    assert set(df["stage"]) == {"draft", "essay", "evaluation"}
    assert df.set_index("stage").loc["draft", "gen_id"] == expected_draft
    assert df.set_index("stage").loc["essay", "gen_id"] == expected_essay
    assert df.set_index("stage").loc["evaluation", "gen_id"] == expected_eval

    # Metadata for the deterministic IDs should be seeded on disk.
    draft_meta = json.loads(
        (data_root / "gens" / "draft" / expected_draft / "metadata.json").read_text(encoding="utf-8")
    )
    assert draft_meta["origin_cohort_id"] == cohort_id
    assert draft_meta["replicate"] == 2

    essay_meta = json.loads(
        (data_root / "gens" / "essay" / expected_essay / "metadata.json").read_text(encoding="utf-8")
    )
    assert essay_meta["parent_gen_id"] == expected_draft
    assert essay_meta["origin_cohort_id"] == cohort_id

    eval_meta = json.loads(
        (data_root / "gens" / "evaluation" / expected_eval / "metadata.json").read_text(encoding="utf-8")
    )
    assert eval_meta["parent_gen_id"] == expected_essay
    assert eval_meta["origin_cohort_id"] == cohort_id


def test_cohort_membership_curated_reuse_drafts_creates_new_essays(base_data_root: Path) -> None:
    data_root = base_data_root
    cohort_id = "cohort-reuse-drafts"

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

    existing_draft_id = draft_src
    existing_essay_id = essay_src

    (data_root / "2_tasks" / "selected_essays.txt").write_text(
        "# mode: reuse-drafts\n" + essay_src + "\n",
        encoding="utf-8",
    )

    context = build_asset_context(resources={"data_root": str(data_root)})
    df = cohort_membership(context, cohort_id=cohort_id)

    new_essay_id = compute_deterministic_gen_id(
        "essay",
        essay_signature(existing_draft_id, "essay-X", 2),
    )
    new_eval_id = compute_deterministic_gen_id(
        "evaluation",
        evaluation_signature(new_essay_id, "eval-1", "eval-model", 1),
    )

    drafts = df[df["stage"] == "draft"]["gen_id"].tolist()
    essays = df[df["stage"] == "essay"]["gen_id"].tolist()
    evaluations = df[df["stage"] == "evaluation"]["gen_id"].tolist()

    assert drafts == [existing_draft_id]
    assert essays == [new_essay_id]
    assert evaluations == [new_eval_id]

    essay_meta = json.loads(
        (data_root / "gens" / "essay" / new_essay_id / "metadata.json").read_text(encoding="utf-8")
    )
    assert essay_meta["replicate"] == 2
    assert essay_meta["parent_gen_id"] == existing_draft_id


def test_cohort_membership_curated_reuse_essays_adds_new_evaluations(base_data_root: Path) -> None:
    data_root = base_data_root
    cohort_id = "cohort-reuse-essays"

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

    existing_draft_id = draft_src
    existing_essay_id = essay_src
    existing_eval_id = compute_deterministic_gen_id(
        "evaluation",
        evaluation_signature(existing_essay_id, "eval-1", "eval-model", 1),
    )

    for stage, gen_id in (("draft", existing_draft_id), ("essay", existing_essay_id), ("evaluation", existing_eval_id)):
        _write_json(
            data_root / "gens" / stage / gen_id / "metadata.json",
            {
                "stage": stage,
                "gen_id": gen_id,
                "parent_gen_id": existing_draft_id if stage != "draft" else "",
                "combo_id": "combo-1",
                "template_id": "draft-A" if stage == "draft" else ("essay-X" if stage == "essay" else "eval-1"),
                "llm_model_id": "gen-model" if stage != "evaluation" else "eval-model",
                "replicate": 1,
            },
        )

    (data_root / "2_tasks" / "selected_essays.txt").write_text(
        "# mode: reuse-essays\n" + essay_src + "\n",
        encoding="utf-8",
    )

    context = build_asset_context(resources={"data_root": str(data_root)})
    df = cohort_membership(context, cohort_id=cohort_id)

    new_eval_id = compute_deterministic_gen_id(
        "evaluation",
        evaluation_signature(existing_essay_id, "eval-1", "eval-model", 2),
    )

    drafts = df[df["stage"] == "draft"]["gen_id"].tolist()
    essays = df[df["stage"] == "essay"]["gen_id"].tolist()
    evaluations = df[df["stage"] == "evaluation"]["gen_id"].tolist()

    assert drafts == [existing_draft_id]
    assert essays == [existing_essay_id]
    assert evaluations == [new_eval_id]

    eval_meta = json.loads(
        (data_root / "gens" / "evaluation" / new_eval_id / "metadata.json").read_text(encoding="utf-8")
    )
    assert eval_meta["replicate"] == 2
    assert eval_meta["parent_gen_id"] == existing_essay_id

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
        [{"template_id": "draft-A", "active": True, "generator": "llm"}],
    )
    _write_csv(
        raw / "essay_templates.csv",
        [{"template_id": "essay-X", "active": True, "generator": "llm"}],
    )
    _write_csv(
        raw / "evaluation_templates.csv",
        [{"template_id": "eval-1", "active": True, "generator": "llm"}],
    )
    _write_csv(
        raw / "llm_models.csv",
        [
            {"id": "gen-model", "for_generation": True, "for_evaluation": False},
            {"id": "eval-model", "for_generation": False, "for_evaluation": True},
        ],
    )
    _write_csv(
        data_root / "2_tasks" / "selected_combo_mappings.csv",
        [{"combo_id": "combo-1", "concept_id": "c1"}],
    )

    (data_root / "gens" / "draft").mkdir(parents=True, exist_ok=True)
    (data_root / "gens" / "essay").mkdir(parents=True, exist_ok=True)
    (data_root / "gens" / "evaluation").mkdir(parents=True, exist_ok=True)

    context = build_asset_context(resources={"data_root": str(data_root)})
    df = cohort_membership(context, cohort_id="cohort-cart")

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


def test_curfated_selection_conflict_raises(base_data_root: Path) -> None:
    data_root = base_data_root
    select_dir = data_root / "2_tasks"
    select_dir.mkdir(parents=True, exist_ok=True)
    (select_dir / "selected_essays.txt").write_text("e_123\n", encoding="utf-8")
    (select_dir / "selected_drafts.txt").write_text("d_456\n", encoding="utf-8")

    context = build_asset_context(resources={"data_root": str(data_root)})
    with pytest.raises(Failure, match="Both selected_essays.txt and selected_drafts.txt are present"):
        cohort_membership(context, cohort_id="cohort-conflict")


def test_selected_drafts_reuse_essays_mode_invalid(base_data_root: Path) -> None:
    data_root = base_data_root
    select_dir = data_root / "2_tasks"
    select_dir.mkdir(parents=True, exist_ok=True)
    (select_dir / "selected_drafts.txt").write_text("# mode: reuse-essays\nd_123\n", encoding="utf-8")

    context = build_asset_context(resources={"data_root": str(data_root)})
    with pytest.raises(Failure, match="requires selected essays"):
        cohort_membership(context, cohort_id="cohort-invalid-mode")
