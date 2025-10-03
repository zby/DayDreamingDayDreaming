from __future__ import annotations

from pathlib import Path
import yaml

import pandas as pd
import pytest
from dagster import build_asset_context

from daydreaming_dagster.assets.group_cohorts import cohort_membership
from daydreaming_dagster.cohorts.spec_migration import generate_spec_bundle


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


@pytest.mark.parametrize("essay_templates", [["essay-X", "essay-Y"]])
def test_generate_spec_round_trip(tmp_path: Path, essay_templates: list[str]) -> None:
    data_root = tmp_path / "data"
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
            {"template_id": template, "generator": "llm"}
            for template in essay_templates
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

    _write_csv(
        data_root / "combo_mappings.csv",
        [{"combo_id": "combo-1"}],
    )

    (data_root / "gens" / "draft").mkdir(parents=True, exist_ok=True)
    (data_root / "gens" / "essay").mkdir(parents=True, exist_ok=True)
    (data_root / "gens" / "evaluation").mkdir(parents=True, exist_ok=True)

    cohort_id = "demo-cohort"
    spec_dir = data_root / "cohorts" / cohort_id / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)
    spec = {
        "axes": {
            "combo_id": {
                "levels": ["combo-1"],
                "catalog_lookup": {"catalog": "combos"},
            },
            "draft_template": {
                "levels": ["draft-A"],
                "catalog_lookup": {"catalog": "draft_templates"},
            },
            "draft_llm": {
                "levels": ["draft-llm"],
                "catalog_lookup": {"catalog": "generation_llms"},
            },
            "essay_template": {
                "levels": [str(template) for template in essay_templates],
                "catalog_lookup": {"catalog": "essay_templates"},
            },
            "essay_llm": {
                "levels": ["essay-llm"],
                "catalog_lookup": {"catalog": "essay_llms"},
            },
            "evaluation_template": {
                "levels": ["eval-1"],
                "catalog_lookup": {"catalog": "evaluation_templates"},
            },
            "evaluation_llm": {
                "levels": ["eval-llm"],
                "catalog_lookup": {"catalog": "evaluation_llms"},
            },
        },
        "rules": [
            {
                "pair": {
                    "left": "evaluation_template",
                    "right": "evaluation_llm",
                    "name": "evaluation_bundle",
                    "allowed": [["eval-1", "eval-llm"]],
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
                "draft_replicate",
                "essay_replicate",
                "evaluation_replicate",
            ]
        },
        "replicates": {
            "draft_template": {"count": 2, "column": "draft_replicate"},
            "essay_template": {"count": 2, "column": "essay_replicate"},
            "evaluation_template": {"count": 1, "column": "evaluation_replicate"},
        },
    }
    (spec_dir / "config.yaml").write_text(
        yaml.safe_dump(spec, sort_keys=False),
        encoding="utf-8",
    )
    combo_df = pd.DataFrame(
        [
            {
                "combo_id": "combo-1",
                "concept_id": "c1",
                "description_level": "paragraph",
                "k_max": 2,
            }
        ]
    )

    context = build_asset_context(resources={"data_root": str(data_root)})
    baseline_df = cohort_membership(
        context,
        cohort_id=cohort_id,
        selected_combo_mappings=combo_df,
    )

    spec_dir = generate_spec_bundle(data_root, cohort_id, overwrite=True)
    assert spec_dir.exists()
    config_text = (spec_dir / "config.yaml").read_text(encoding="utf-8")
    assert "@file:items/cohort_rows.yaml" in config_text

    roundtrip_df = cohort_membership(
        build_asset_context(resources={"data_root": str(data_root)}),
        cohort_id=cohort_id,
        selected_combo_mappings=combo_df,
    )

    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(["stage", "gen_id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        _normalize(baseline_df),
        _normalize(roundtrip_df),
        check_like=True,
    )

    item_path = spec_dir / "items" / "cohort_rows.yaml"
    assert item_path.exists()
    assert "combo-1" in item_path.read_text(encoding="utf-8")
