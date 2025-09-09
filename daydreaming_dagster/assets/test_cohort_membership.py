import json
from pathlib import Path

import pandas as pd
import pytest
from dagster import build_asset_context

from daydreaming_dagster.assets.group_cohorts import cohort_membership
from daydreaming_dagster.assets import group_task_definitions as tasks


def _write_json(p: Path, data: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def test_cohort_membership_curated_and_projection(tmp_path: Path, monkeypatch):
    data_root = tmp_path

    # Minimal eval axes
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"template_id": "eval-1", "active": True, "parser": "numeric_line"},
    ]).to_csv(data_root / "1_raw" / "evaluation_templates.csv", index=False)
    pd.DataFrame([
        {"id": "gen-1", "model": "provider/gen-1", "for_generation": True, "for_evaluation": False},
        {"id": "eval-1", "model": "provider/eval-1", "for_generation": False, "for_evaluation": True},
    ]).to_csv(data_root / "1_raw" / "llm_models.csv", index=False)

    # Seed prior gens metadata (draft -> essay)
    draft_src = "D0001"
    essay_src = "E0001"
    _write_json(
        data_root / "gens" / "draft" / draft_src / "metadata.json",
        {
            "stage": "draft",
            "gen_id": draft_src,
            "combo_id": "combo-1",
            "template_id": "draft-tpl",
            "model_id": "gen-1",
        },
    )
    _write_json(
        data_root / "gens" / "essay" / essay_src / "metadata.json",
        {
            "stage": "essay",
            "gen_id": essay_src,
            "parent_gen_id": draft_src,
            "template_id": "essay-tpl",
            "model_id": "gen-1",
        },
    )

    # Selection list
    (data_root / "2_tasks").mkdir(parents=True, exist_ok=True)
    (data_root / "2_tasks" / "selected_essays.txt").write_text(f"{essay_src}\n", encoding="utf-8")

    ctx = build_asset_context(resources={"data_root": str(data_root)})
    cid = "cohort-unit-test"

    # Build membership
    mdf = cohort_membership(ctx, cid)
    assert not mdf.empty
    # Membership persists to file
    out_path = data_root / "cohorts" / cid / "membership.csv"
    assert out_path.exists()

    # Expect one draft, one essay, >=1 evaluation
    assert (mdf["stage"] == "draft").sum() == 1
    assert (mdf["stage"] == "essay").sum() == 1
    assert (mdf["stage"] == "evaluation").sum() >= 1

    # Parent integrity in memory
    draft_id = mdf[mdf["stage"] == "draft"]["gen_id"].iloc[0]
    essay_row = mdf[mdf["stage"] == "essay"].iloc[0]
    assert essay_row["parent_gen_id"] == draft_id

    # Projection by task assets from membership (read from disk via resolved cohort)
    monkeypatch.setenv("DD_COHORT", cid)
    # Drafts
    dctx = build_asset_context(resources={"data_root": str(data_root)})
    ddf = tasks.draft_generation_tasks(dctx, content_combinations=[])
    assert not ddf.empty and set(ddf.columns) >= {"gen_id", "cohort_id", "draft_task_id"}

    # Essays
    ectx = build_asset_context(resources={"data_root": str(data_root)})
    edf = tasks.essay_generation_tasks(ectx, content_combinations=[], draft_generation_tasks=ddf)
    assert not edf.empty and set(edf.columns) >= {"gen_id", "cohort_id", "essay_task_id", "parent_gen_id"}

    # Evaluations
    vctx = build_asset_context(resources={"data_root": str(data_root)})
    vdf = tasks.evaluation_tasks(vctx, essay_generation_tasks=edf, draft_generation_tasks=ddf)
    assert not vdf.empty and set(vdf.columns) >= {"gen_id", "cohort_id", "evaluation_task_id", "parent_gen_id"}


def test_cohort_membership_missing_parent_fails(tmp_path: Path, monkeypatch):
    data_root = tmp_path

    # Minimal eval axes
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"template_id": "eval-1", "active": True},
    ]).to_csv(data_root / "1_raw" / "evaluation_templates.csv", index=False)
    pd.DataFrame([
        {"id": "eval-1", "model": "provider/eval-1", "for_generation": False, "for_evaluation": True},
    ]).to_csv(data_root / "1_raw" / "llm_models.csv", index=False)

    # Essay metadata points to a non-existent draft parent
    essay_src = "E404"
    _write_json(
        data_root / "gens" / "essay" / essay_src / "metadata.json",
        {
            "stage": "essay",
            "gen_id": essay_src,
            "parent_gen_id": "D-MISSING",
            "template_id": "essay-tpl",
            "model_id": "gen-1",
        },
    )
    (data_root / "2_tasks").mkdir(parents=True, exist_ok=True)
    (data_root / "2_tasks" / "selected_essays.txt").write_text(f"{essay_src}\n", encoding="utf-8")

    ctx = build_asset_context(resources={"data_root": str(data_root)})
    with pytest.raises(Exception):
        cohort_membership(ctx, "cohort-parent-fail")
