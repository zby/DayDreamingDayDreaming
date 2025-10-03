from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml
from dagster import build_asset_context

from daydreaming_dagster.models.content_combination import ContentCombination


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
            "combo_id": {
                "levels": [str(item) for item in combos],
                "catalog_lookup": {"catalog": "combos"},
            },
            "draft_template": {
                "levels": [str(item) for item in draft_templates],
                "catalog_lookup": {"catalog": "draft_templates"},
            },
            "draft_llm": {
                "levels": [str(item) for item in generation_llms],
                "catalog_lookup": {"catalog": "generation_llms"},
            },
            "essay_template": {
                "levels": [str(item) for item in essay_templates],
                "catalog_lookup": {"catalog": "essay_templates"},
            },
            "essay_llm": {
                "levels": [str(item) for item in generation_llms],
                "catalog_lookup": {"catalog": "essay_llms"},
            },
            "evaluation_template": {
                "levels": [str(item) for item in evaluation_templates],
                "catalog_lookup": {"catalog": "evaluation_templates"},
            },
            "evaluation_llm": {
                "levels": [str(item) for item in evaluation_llms],
                "catalog_lookup": {"catalog": "evaluation_llms"},
            },
        },
        "rules": [
            {
                "pair": {
                    "left": "evaluation_template",
                    "right": "evaluation_llm",
                    "name": "evaluation_bundle",
                    "allowed": allowed_matrix,
                }
            }
        ],
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
        replicate_columns = [
            cfg.get("column")
            for cfg in replicates.values()
            if isinstance(cfg, dict) and cfg.get("column")
        ]
        spec["output"]["field_order"].extend(replicate_columns)

    (spec_dir / "config.yaml").write_text(
        yaml.safe_dump(spec, sort_keys=False),
        encoding="utf-8",
    )


def _setup_data_root(root: Path) -> None:
    raw = root / "1_raw"
    raw.mkdir(parents=True, exist_ok=True)
    _write_csv(
        raw / "replication_config.csv",
        [
            {"stage": "draft", "replicates": 2},
            {"stage": "essay", "replicates": 1},
            {"stage": "evaluation", "replicates": 3},
        ],
    )
    _write_csv(
        raw / "draft_templates.csv",
        [{"template_id": "draft-A"}, {"template_id": "draft-old"}],
    )
    _write_csv(
        raw / "essay_templates.csv",
        [{"template_id": "essay-X"}],
    )
    _write_csv(
        raw / "evaluation_templates.csv",
        [{"template_id": "eval-1"}, {"template_id": "eval-old"}],
    )
    _write_csv(
        raw / "llm_models.csv",
        [
            {"id": "gen-model-1", "for_generation": True, "for_evaluation": False},
            {"id": "eval-model-1", "for_generation": False, "for_evaluation": True},
        ],
    )
    _write_csv(
        root / "combo_mappings.csv",
        [{"combo_id": "c1"}, {"combo_id": "c2"}, {"combo_id": "x"}],
    )


def test_cohort_id_deterministic_and_manifest_written(tmp_path):
    import daydreaming_dagster.assets.group_cohorts as module

    _setup_data_root(tmp_path)
    _write_spec(
        tmp_path,
        cohort_id="cohort-demo",
        combos=["c1", "c2"],
        draft_templates=["draft-A"],
        essay_templates=["essay-X"],
        evaluation_templates=["eval-1"],
        generation_llms=["gen-model-1"],
        evaluation_llms=["eval-model-1"],
        replicates={
            "draft_template": {"count": 2, "column": "draft_replicate"},
            "evaluation_template": {"count": 3, "column": "evaluation_replicate"},
        },
    )

    context = build_asset_context(
        resources={"data_root": str(tmp_path)},
        asset_config={"override": "cohort-demo"},
    )
    combos = [
        ContentCombination(contents=[{"name": "a", "content": "x"}], combo_id="c1", concept_ids=["a"]),
        ContentCombination(contents=[{"name": "b", "content": "y"}], combo_id="c2", concept_ids=["b"]),
    ]

    cid1 = module.cohort_id(context, content_combinations=combos)
    cid2 = module.cohort_id(context, content_combinations=combos)

    assert cid1 == "cohort-demo"
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
    assert manifest["replication"] == {"draft": 2, "essay": 1, "evaluation": 3}


def test_cohort_id_override_precedence_config_over_env(tmp_path, monkeypatch):
    import daydreaming_dagster.assets.group_cohorts as module

    _setup_data_root(tmp_path)
    _write_spec(
        tmp_path,
        cohort_id="CONFIG-COHORT-999",
        combos=["x"],
        draft_templates=["draft-A"],
        essay_templates=["essay-X"],
        evaluation_templates=["eval-1"],
        generation_llms=["gen-model-1"],
        evaluation_llms=["eval-model-1"],
        replicates={
            "draft_template": {"count": 2, "column": "draft_replicate"},
            "evaluation_template": {"count": 3, "column": "evaluation_replicate"},
        },
    )
    _write_spec(
        tmp_path,
        cohort_id="ENV-COHORT-123",
        combos=["x"],
        draft_templates=["draft-A"],
        essay_templates=["essay-X"],
        evaluation_templates=["eval-1"],
        generation_llms=["gen-model-1"],
        evaluation_llms=["eval-model-1"],
        replicates={
            "draft_template": {"count": 2, "column": "draft_replicate"},
            "evaluation_template": {"count": 3, "column": "evaluation_replicate"},
        },
    )

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
