from __future__ import annotations

import json
from pathlib import Path
import textwrap

import pandas as pd
import yaml
from dagster import build_asset_context

from daydreaming_dagster.assets.group_cohorts import cohort_membership
from daydreaming_dagster.utils.ids import compute_deterministic_gen_id, draft_signature, essay_signature


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_spec(root: Path, cohort_id: str) -> None:
    spec_dir = root / "cohorts" / cohort_id / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)
    spec = {
        "axes": {
            "combo_id": ["combo-1"],
            "draft_template": ["draft-A"],
            "draft_llm": ["gen-model"],
            "essay_template": ["essay-X"],
            "essay_llm": ["gen-model"],
            "evaluation_template": ["eval-1"],
            "evaluation_llm": ["eval-model"],
        },
        "rules": {
            "pairs": {
                "evaluation_bundle": {
                    "left": "evaluation_template",
                    "right": "evaluation_llm",
                    "allowed": [["eval-1", "eval-model"]],
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
    (spec_dir / "config.yaml").write_text(
        yaml.safe_dump(spec, sort_keys=False),
        encoding="utf-8",
    )


def test_essay_rows_reference_draft_ids(tmp_path: Path) -> None:
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
    _write_csv(raw / "draft_templates.csv", [{"template_id": "draft-A", "generator": "llm"}])
    _write_csv(raw / "essay_templates.csv", [{"template_id": "essay-X", "generator": "llm"}])
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
    _write_csv(tmp_path / "combo_mappings.csv", [{"combo_id": "combo-1"}])
    _write_spec(tmp_path, "cohort-essay")
    for stage in ("draft", "essay", "evaluation"):
        (tmp_path / "gens" / stage).mkdir(parents=True, exist_ok=True)

    context = build_asset_context(resources={"data_root": str(tmp_path)})
    selected_df = pd.DataFrame(
        [
            {
                "combo_id": "combo-1",
                "concept_id": "c1",
                "description_level": "paragraph",
                "k_max": 1,
            }
        ]
    )
    df = cohort_membership(
        context,
        cohort_id="cohort-essay",
        selected_combo_mappings=selected_df,
    )

    draft_id = compute_deterministic_gen_id(
        "draft",
        draft_signature("combo-1", "draft-A", "gen-model", 1),
    )
    essay_id = compute_deterministic_gen_id(
        "essay",
        essay_signature(draft_id, "essay-X", 1),
    )

    essay_rows = df[df["stage"] == "essay"].reset_index(drop=True)
    assert len(essay_rows) == 1
    assert essay_rows.loc[0, "gen_id"] == essay_id

    essay_meta = json.loads(
        (tmp_path / "gens" / "essay" / essay_id / "metadata.json").read_text(encoding="utf-8")
    )
    assert essay_meta["parent_gen_id"] == draft_id
