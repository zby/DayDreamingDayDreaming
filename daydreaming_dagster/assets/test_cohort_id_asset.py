import json
from types import SimpleNamespace
from daydreaming_dagster.models.content_combination import ContentCombination

import pandas as pd
from dagster import build_asset_context


def _stub_tables(monkeypatch, m):
    draft_templates_df = pd.DataFrame([
        {"template_id": "draft-A", "active": True},
        {"template_id": "draft-old", "active": False},
    ])
    essay_templates_df = pd.DataFrame([
        {"template_id": "essay-X", "active": True},
    ])
    evaluation_templates_df = pd.DataFrame([
        {"template_id": "eval-1", "active": True},
        {"template_id": "eval-old", "active": False},
    ])
    models_df = pd.DataFrame(
        [
            {"id": "gen-model-1", "for_generation": True, "for_evaluation": False},
            {"id": "eval-model-1", "for_generation": False, "for_evaluation": True},
        ]
    )
    monkeypatch.setattr(m, "read_draft_templates", lambda _root: draft_templates_df)
    monkeypatch.setattr(m, "read_essay_templates", lambda _root: essay_templates_df)
    monkeypatch.setattr(m, "read_evaluation_templates", lambda _root: evaluation_templates_df)
    monkeypatch.setattr(m, "read_llm_models", lambda _root: models_df)


def test_cohort_id_deterministic_and_manifest_written(tmp_path, monkeypatch):
    import daydreaming_dagster.assets.group_task_definitions as m

    _stub_tables(monkeypatch, m)

    ctx = build_asset_context(resources={"data_root": str(tmp_path)}, asset_config={})
    combos = [
        ContentCombination(contents=[{"name": "a", "content": "x"}], combo_id="c1", concept_ids=["a"]),
        ContentCombination(contents=[{"name": "b", "content": "y"}], combo_id="c2", concept_ids=["b"]),
    ]

    cid1 = m.cohort_id(ctx, content_combinations=combos)
    cid2 = m.cohort_id(ctx, content_combinations=combos)

    assert isinstance(cid1, str) and cid1.startswith("cohort-")
    assert cid1 == cid2  # deterministic for same manifest

    manifest_path = tmp_path / "cohorts" / cid1 / "manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["combos"] == ["c1", "c2"]
    assert manifest["templates"]["draft"] == ["draft-A"]
    assert manifest["templates"]["essay"] == ["essay-X"]
    assert manifest["templates"]["evaluation"] == ["eval-1"]
    assert manifest["llms"]["generation"] == ["gen-model-1"]
    assert manifest["llms"]["evaluation"] == ["eval-model-1"]


def test_cohort_id_override_precedence_config_over_env(tmp_path, monkeypatch):
    import daydreaming_dagster.assets.group_task_definitions as m

    _stub_tables(monkeypatch, m)
    monkeypatch.setenv("DD_COHORT", "ENV-COHORT-123")

    ctx = build_asset_context(
        resources={"data_root": str(tmp_path)},
        asset_config={"override": "CONFIG-COHORT-999"},
    )
    combos = [ContentCombination(contents=[{"name": "x", "content": "p"}], combo_id="x", concept_ids=["x"])]
    cid = m.cohort_id(ctx, content_combinations=combos)
    assert cid == "CONFIG-COHORT-999"
    assert (tmp_path / "cohorts" / cid / "manifest.json").exists()

    # Without asset_config, env var should be used
    ctx2 = build_asset_context(resources={"data_root": str(tmp_path)}, asset_config={})
    cid_env = m.cohort_id(ctx2, content_combinations=combos)
    assert cid_env == "ENV-COHORT-123"
    assert (tmp_path / "cohorts" / cid_env / "manifest.json").exists()


def test_draft_generation_tasks_includes_cohort_id(tmp_path, monkeypatch):
    import daydreaming_dagster.assets.group_task_definitions as m

    # Stub draft templates and models to produce one task
    draft_templates_df = pd.DataFrame([{"template_id": "draft-A", "active": True}])
    models_df = pd.DataFrame([
        {"id": "gen-model-1", "model": "provider/model-1", "for_generation": True},
    ])
    monkeypatch.setattr(m, "read_draft_templates", lambda _root: draft_templates_df)
    monkeypatch.setattr(m, "read_llm_models", lambda _root: models_df)

    # Reserve function should receive run_id=cohort_id; return value reflects it for verification
    calls = []

    def fake_reserve(stage, task_id, run_id=None):
        calls.append((stage, task_id, run_id))
        return f"gid::{stage}::{task_id}::run={run_id}"

    monkeypatch.setattr(m, "reserve_gen_id", fake_reserve)

    # Override via environment to avoid Dagster dependency on cohort asset during direct invocation
    monkeypatch.setenv("DD_COHORT", "COH-1")

    ctx = build_asset_context(resources={"data_root": str(tmp_path)}, asset_config={})
    combos = [ContentCombination(contents=[{"name": "a", "content": "x"}], combo_id="combo-1", concept_ids=["a"])]
    df = m.draft_generation_tasks(ctx, content_combinations=combos)

    assert not df.empty
    assert set(df.columns) >= {"gen_id", "cohort_id", "draft_task_id"}
    assert (df["cohort_id"] == "COH-1").all()
    # Ensure reserve_gen_id saw the cohort run id
    assert all(c[-1] == "COH-1" for c in calls)
    assert df["gen_id"].astype(str).str.contains("run=COH-1").all()
