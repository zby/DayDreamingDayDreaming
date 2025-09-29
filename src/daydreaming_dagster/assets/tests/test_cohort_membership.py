import json
from pathlib import Path

import pandas as pd
import pytest
from dagster import build_asset_context

from daydreaming_dagster.assets.group_cohorts import cohort_membership
from daydreaming_dagster.utils import ids as ids_utils
from daydreaming_dagster.utils.ids import draft_signature, essay_signature, compute_deterministic_gen_id


def _write_json(p: Path, data: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def test_cohort_membership_curated_and_projection(tmp_path: Path, monkeypatch):
    data_root = tmp_path

    # Minimal eval axes
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    # Replication config required: set all stages to 1
    pd.DataFrame([
        {"stage": "draft", "replicates": 1},
        {"stage": "essay", "replicates": 1},
        {"stage": "evaluation", "replicates": 1},
    ]).to_csv(data_root / "1_raw" / "replication_config.csv", index=False)
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

    # DataFrame is slim (stage/gen only)
    assert list(mdf.columns) == ["stage", "gen_id"]

    # Expect one draft, one essay, >=1 evaluation
    assert (mdf["stage"] == "draft").sum() == 1
    assert (mdf["stage"] == "essay").sum() == 1
    assert (mdf["stage"] == "evaluation").sum() >= 1

    # Parent integrity in memory
    draft_id = mdf[mdf["stage"] == "draft"]["gen_id"].iloc[0]
    essay_id = mdf[mdf["stage"] == "essay"]["gen_id"].iloc[0]
    essay_meta = json.loads((data_root / "gens" / "essay" / essay_id / "metadata.json").read_text(encoding="utf-8"))
    assert essay_meta["parent_gen_id"] == draft_id

    # Projection layer removed; verify membership rows directly
    drafts = mdf[mdf["stage"] == "draft"]
    essays = mdf[mdf["stage"] == "essay"]
    evals = mdf[mdf["stage"] == "evaluation"]
    assert not drafts.empty and not essays.empty and not evals.empty


def test_cohort_membership_deterministic_draft_ids(tmp_path: Path, monkeypatch):
    data_root = tmp_path
    monkeypatch.setattr(ids_utils, "DETERMINISTIC_GEN_IDS_ENABLED", True, raising=False)

    # Minimal config
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"stage": "draft", "replicates": 1},
        {"stage": "essay", "replicates": 1},
        {"stage": "evaluation", "replicates": 1},
    ]).to_csv(data_root / "1_raw" / "replication_config.csv", index=False)

    pd.DataFrame([
        {"template_id": "eval-1", "active": True},
    ]).to_csv(data_root / "1_raw" / "evaluation_templates.csv", index=False)

    pd.DataFrame([
        {"id": "m-gen", "model": "provider/m-gen", "for_generation": True, "for_evaluation": False},
        {"id": "m-eval", "model": "provider/m-eval", "for_generation": False, "for_evaluation": True},
    ]).to_csv(data_root / "1_raw" / "llm_models.csv", index=False)

    # Existing draft and essay metadata
    draft_id = "D100"
    essay_id = "E100"
    _write_json(
        data_root / "gens" / "draft" / draft_id / "metadata.json",
        {
            "stage": "draft",
            "gen_id": draft_id,
            "combo_id": "combo-1",
            "template_id": "draft-tpl",
            "llm_model_id": "m-gen",
            "model_id": "m-gen",
        },
    )
    _write_json(
        data_root / "gens" / "essay" / essay_id / "metadata.json",
        {
            "stage": "essay",
            "gen_id": essay_id,
            "parent_gen_id": draft_id,
            "template_id": "essay-tpl",
            "llm_model_id": "m-gen",
            "model_id": "m-gen",
        },
    )

    # Selected essays
    (data_root / "2_tasks").mkdir(parents=True, exist_ok=True)
    (data_root / "2_tasks" / "selected_essays.txt").write_text(f"{essay_id}\n", encoding="utf-8")

    ctx = build_asset_context(resources={"data_root": str(data_root)})
    cid = "cohort-det"
    mdf = cohort_membership(ctx, cid)

    draft_rows = mdf[mdf["stage"] == "draft"]
    assert len(draft_rows) == 1
    gen_id = draft_rows.iloc[0]["gen_id"]
    expected_sig = draft_signature("combo-1", "draft-tpl", "m-gen", 1)
    expected_id = compute_deterministic_gen_id("draft", expected_sig)
    assert gen_id == expected_id

    essay_rows = mdf[mdf["stage"] == "essay"]
    assert len(essay_rows) == 1
    essay_id = essay_rows.iloc[0]["gen_id"]
    expected_essay_sig = essay_signature(expected_id, "essay-tpl", 1)
    expected_essay_id = compute_deterministic_gen_id("essay", expected_essay_sig)
    assert essay_id == expected_essay_id


def test_cohort_membership_missing_parent_fails(tmp_path: Path, monkeypatch):
    data_root = tmp_path

    # Minimal eval axes
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"stage": "draft", "replicates": 1},
        {"stage": "essay", "replicates": 1},
        {"stage": "evaluation", "replicates": 1},
    ]).to_csv(data_root / "1_raw" / "replication_config.csv", index=False)
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


def test_evaluation_combo_id_matches_parent_essay(tmp_path: Path):
    data_root = tmp_path
    # Minimal eval axes: one eval template, one eval model
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"stage": "draft", "replicates": 1},
        {"stage": "essay", "replicates": 1},
        {"stage": "evaluation", "replicates": 1},
    ]).to_csv(data_root / "1_raw" / "replication_config.csv", index=False)
    pd.DataFrame([
        {"template_id": "eval-1", "active": True},
    ]).to_csv(data_root / "1_raw" / "evaluation_templates.csv", index=False)
    pd.DataFrame([
        {"id": "eval-1", "model": "provider/eval-1", "for_generation": False, "for_evaluation": True},
    ]).to_csv(data_root / "1_raw" / "llm_models.csv", index=False)

    # Seed two independent draft->essay chains with distinct combo_ids
    chains = [
        ("D100", "E100", "combo-A", "draft-tpl-A", "essay-tpl-A", "gen-A"),
        ("D200", "E200", "combo-B", "draft-tpl-B", "essay-tpl-B", "gen-B"),
    ]
    for d_id, e_id, combo, d_tpl, e_tpl, g_mid in chains:
        _write_json(
            data_root / "gens" / "draft" / d_id / "metadata.json",
            {
                "stage": "draft",
                "gen_id": d_id,
                "combo_id": combo,
                "template_id": d_tpl,
                "model_id": g_mid,
            },
        )
        _write_json(
            data_root / "gens" / "essay" / e_id / "metadata.json",
            {
                "stage": "essay",
                "gen_id": e_id,
                "parent_gen_id": d_id,
                "template_id": e_tpl,
                "model_id": g_mid,
            },
        )

    # Select both essays
    (data_root / "2_tasks").mkdir(parents=True, exist_ok=True)
    (data_root / "2_tasks" / "selected_essays.txt").write_text("\n".join([c[1] for c in chains]) + "\n", encoding="utf-8")

    ctx = build_asset_context(resources={"data_root": str(data_root)})
    cid = "cohort-combo-prop"
    mdf = cohort_membership(ctx, cid)

    # Build mapping essay gen_id -> combo_id from metadata
    essays = mdf[mdf["stage"] == "essay"]["gen_id"].astype(str).tolist()
    essay_combo: dict[str, str] = {}
    for essay_id in essays:
        meta = json.loads((data_root / "gens" / "essay" / essay_id / "metadata.json").read_text(encoding="utf-8"))
        essay_combo[essay_id] = meta.get("combo_id")

    # For each evaluation row, ensure its combo_id equals its parent essay's combo_id
    for eval_id in mdf[mdf["stage"] == "evaluation"]["gen_id"].astype(str):
        meta = json.loads((data_root / "gens" / "evaluation" / eval_id / "metadata.json").read_text(encoding="utf-8"))
        parent = str(meta.get("parent_gen_id"))
        assert parent in essays
        assert meta.get("combo_id") == essay_combo[parent]


def test_cohort_membership_evaluation_only_mode(tmp_path: Path):
    data_root = tmp_path

    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"stage": "draft", "replicates": 1},
        {"stage": "essay", "replicates": 1},
        {"stage": "evaluation", "replicates": 2},
    ]).to_csv(data_root / "1_raw" / "replication_config.csv", index=False)
    pd.DataFrame([
        {"template_id": "eval-1", "active": True},
        {"template_id": "eval-2", "active": True},
    ]).to_csv(data_root / "1_raw" / "evaluation_templates.csv", index=False)
    pd.DataFrame([
        {"id": "eval-model-a", "model": "provider/eval-a", "for_generation": False, "for_evaluation": True},
        {"id": "eval-model-b", "model": "provider/eval-b", "for_generation": False, "for_evaluation": True},
    ]).to_csv(data_root / "1_raw" / "llm_models.csv", index=False)

    draft_src = "D500"
    essay_src = "E500"
    _write_json(
        data_root / "gens" / "draft" / draft_src / "metadata.json",
        {
            "stage": "draft",
            "gen_id": draft_src,
            "combo_id": "combo-Z",
            "template_id": "draft-z",
            "model_id": "gen-z",
        },
    )
    _write_json(
        data_root / "gens" / "essay" / essay_src / "metadata.json",
        {
            "stage": "essay",
            "gen_id": essay_src,
            "parent_gen_id": draft_src,
            "combo_id": "combo-Z",
            "template_id": "essay-z",
            "model_id": "gen-z",
        },
    )

    (data_root / "2_tasks").mkdir(parents=True, exist_ok=True)
    (data_root / "2_tasks" / "selected_essays.txt").write_text(
        "# mode: evaluation-only\n" f"{essay_src}\n",
        encoding="utf-8",
    )

    ctx = build_asset_context(resources={"data_root": str(data_root)})
    cid = "cohort-eval-only"
    mdf = cohort_membership(ctx, cid)

    assert not mdf.empty
    assert (mdf["stage"] == "draft").sum() == 0
    assert (mdf["stage"] == "essay").sum() == 0
    eval_rows = mdf[mdf["stage"] == "evaluation"]
    assert len(eval_rows) == 8  # 2 templates * 2 models * 2 replicates
    meta_records = [
        json.loads((data_root / "gens" / "evaluation" / gid / "metadata.json").read_text(encoding="utf-8"))
        for gid in eval_rows["gen_id"].astype(str)
    ]
    assert {m.get("parent_gen_id") for m in meta_records} == {essay_src}
    assert {m.get("combo_id") for m in meta_records} == {"combo-Z"}


def test_cohort_membership_evaluation_only_fill_up(tmp_path: Path):
    data_root = tmp_path
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"stage": "draft", "replicates": 1},
        {"stage": "essay", "replicates": 1},
        {"stage": "evaluation", "replicates": 2},
    ]).to_csv(data_root / "1_raw" / "replication_config.csv", index=False)
    pd.DataFrame([
        {"template_id": "eval-1", "active": True},
    ]).to_csv(data_root / "1_raw" / "evaluation_templates.csv", index=False)
    pd.DataFrame([
        {"id": "eval-model", "model": "provider/eval", "for_generation": False, "for_evaluation": True},
    ]).to_csv(data_root / "1_raw" / "llm_models.csv", index=False)

    draft_src = "D900"
    essay_src = "E900"
    _write_json(
        data_root / "gens" / "draft" / draft_src / "metadata.json",
        {
            "stage": "draft",
            "gen_id": draft_src,
            "combo_id": "combo-fill",
            "template_id": "draft-fill",
            "model_id": "gen-fill",
        },
    )
    _write_json(
        data_root / "gens" / "essay" / essay_src / "metadata.json",
        {
            "stage": "essay",
            "gen_id": essay_src,
            "parent_gen_id": draft_src,
            "combo_id": "combo-fill",
            "template_id": "essay-fill",
            "model_id": "gen-fill",
        },
    )

    existing_eval = "EV-existing"
    _write_json(
        data_root / "gens" / "evaluation" / existing_eval / "metadata.json",
        {
            "stage": "evaluation",
            "gen_id": existing_eval,
            "parent_gen_id": essay_src,
            "template_id": "eval-1",
            "llm_model_id": "eval-model",
        },
    )

    (data_root / "2_tasks").mkdir(parents=True, exist_ok=True)
    (data_root / "2_tasks" / "selected_essays.txt").write_text(
        "# mode: evaluation-only\n# skip-existing-evaluations\n" f"{essay_src}\n",
        encoding="utf-8",
    )

    ctx = build_asset_context(resources={"data_root": str(data_root)})
    cid = "cohort-eval-fill"
    mdf = cohort_membership(ctx, cid)

    eval_rows = mdf[mdf["stage"] == "evaluation"]
    # Expect only one new evaluation row to top up to 2 replicates
    assert len(eval_rows) == 1
    meta = json.loads((data_root / "gens" / "evaluation" / eval_rows["gen_id"].iloc[0] / "metadata.json").read_text(encoding="utf-8"))
    assert meta.get("parent_gen_id") == essay_src
    assert meta.get("combo_id") == "combo-fill"
    assert meta.get("replicate") == 2


def test_cohort_membership_evaluation_only_parent_combo_fallback(tmp_path: Path):
    data_root = tmp_path
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"stage": "draft", "replicates": 1},
        {"stage": "essay", "replicates": 1},
        {"stage": "evaluation", "replicates": 1},
    ]).to_csv(data_root / "1_raw" / "replication_config.csv", index=False)
    pd.DataFrame([
        {"template_id": "eval-1", "active": True},
    ]).to_csv(data_root / "1_raw" / "evaluation_templates.csv", index=False)
    pd.DataFrame([
        {"id": "eval-model", "model": "provider/eval", "for_generation": False, "for_evaluation": True},
    ]).to_csv(data_root / "1_raw" / "llm_models.csv", index=False)

    draft_src = "D-parent"
    essay_src = "E-parent"
    _write_json(
        data_root / "gens" / "draft" / draft_src / "metadata.json",
        {
            "stage": "draft",
            "gen_id": draft_src,
            "combo_id": "combo-parent",
            "template_id": "draft-parent",
            "model_id": "gen-parent",
        },
    )
    _write_json(
        data_root / "gens" / "essay" / essay_src / "metadata.json",
        {
            "stage": "essay",
            "gen_id": essay_src,
            "parent_gen_id": draft_src,
            "template_id": "essay-parent",
            "model_id": "gen-parent",
        },
    )

    (data_root / "2_tasks").mkdir(parents=True, exist_ok=True)
    (data_root / "2_tasks" / "selected_essays.txt").write_text(
        "# mode: evaluation-only\n" f"{essay_src}\n",
        encoding="utf-8",
    )

    ctx = build_asset_context(resources={"data_root": str(data_root)})
    cid = "cohort-eval-parent-fallback"
    mdf = cohort_membership(ctx, cid)

    eval_rows = mdf[mdf["stage"] == "evaluation"]
    assert not eval_rows.empty
    combos = {
        json.loads((data_root / "gens" / "evaluation" / gid / "metadata.json").read_text(encoding="utf-8")).get("combo_id")
        for gid in eval_rows["gen_id"].astype(str)
    }
    assert combos == {"combo-parent"}


def test_evaluation_only_requires_essay_ids(tmp_path: Path):
    data_root = tmp_path
    (data_root / "1_raw").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"stage": "draft", "replicates": 1},
        {"stage": "essay", "replicates": 1},
        {"stage": "evaluation", "replicates": 1},
    ]).to_csv(data_root / "1_raw" / "replication_config.csv", index=False)
    pd.DataFrame([
        {"template_id": "eval-1", "active": True},
    ]).to_csv(data_root / "1_raw" / "evaluation_templates.csv", index=False)
    pd.DataFrame([
        {"id": "eval-model", "model": "provider/eval", "for_generation": False, "for_evaluation": True},
    ]).to_csv(data_root / "1_raw" / "llm_models.csv", index=False)

    draft_src = "D-only"
    _write_json(
        data_root / "gens" / "draft" / draft_src / "metadata.json",
        {
            "stage": "draft",
            "gen_id": draft_src,
            "combo_id": "combo-X",
            "template_id": "draft-x",
            "model_id": "gen-x",
        },
    )

    (data_root / "2_tasks").mkdir(parents=True, exist_ok=True)
    (data_root / "2_tasks" / "selected_essays.txt").write_text(
        "# mode: evaluation-only\n" f"{draft_src}\n",
        encoding="utf-8",
    )

    ctx = build_asset_context(resources={"data_root": str(data_root)})
    with pytest.raises(Exception):
        cohort_membership(ctx, "cohort-eval-only-invalid")
