import json

import pandas as pd
from dagster import build_asset_context

from daydreaming_dagster.models.content_combination import ContentCombination


def _stub_tables(monkeypatch, module):
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

    def _read_templates(_root, kind: str, filter_active: bool = True):
        if kind == "draft":
            return draft_templates_df
        if kind == "essay":
            return essay_templates_df
        if kind == "evaluation":
            return evaluation_templates_df
        return pd.DataFrame([])

    monkeypatch.setattr(module, "read_templates", _read_templates)
    monkeypatch.setattr(module, "read_llm_models", lambda _root: models_df)


def test_cohort_id_deterministic_and_manifest_written(tmp_path, monkeypatch):
    import daydreaming_dagster.assets.group_cohorts as module

    _stub_tables(monkeypatch, module)

    context = build_asset_context(resources={"data_root": str(tmp_path)}, asset_config={})
    combos = [
        ContentCombination(contents=[{"name": "a", "content": "x"}], combo_id="c1", concept_ids=["a"]),
        ContentCombination(contents=[{"name": "b", "content": "y"}], combo_id="c2", concept_ids=["b"]),
    ]

    cid1 = module.cohort_id(context, content_combinations=combos)
    cid2 = module.cohort_id(context, content_combinations=combos)

    assert isinstance(cid1, str) and cid1.startswith("cohort-")
    assert cid1 == cid2

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
    import daydreaming_dagster.assets.group_cohorts as module

    _stub_tables(monkeypatch, module)
    monkeypatch.setenv("DD_COHORT", "ENV-COHORT-123")

    context = build_asset_context(
        resources={"data_root": str(tmp_path)},
        asset_config={"override": "CONFIG-COHORT-999"},
    )
    combos = [ContentCombination(contents=[{"name": "x", "content": "p"}], combo_id="x", concept_ids=["x"])]

    cid_config = module.cohort_id(context, content_combinations=combos)
    assert cid_config == "CONFIG-COHORT-999"
    assert (tmp_path / "cohorts" / cid_config / "manifest.json").exists()

    context_env = build_asset_context(resources={"data_root": str(tmp_path)}, asset_config={})
    cid_env = module.cohort_id(context_env, content_combinations=combos)
    assert cid_env == "ENV-COHORT-123"
    assert (tmp_path / "cohorts" / cid_env / "manifest.json").exists()
