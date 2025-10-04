from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from dagster import build_asset_context

from daydreaming_dagster.assets.group_cohorts import (
    selected_combo_mappings,
    content_combinations,
)
from daydreaming_dagster.utils.errors import DDError, Err


def _write_manifest(root: Path, cohort_id: str, combos: list[str]) -> None:
    manifest_dir = root / "cohorts" / cohort_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "combos": combos,
        "templates": {"draft": [], "essay": [], "evaluation": []},
        "llms": {"generation": [], "evaluation": []},
        "replication": {},
    }
    (manifest_dir / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def _write_concepts(root: Path, concept_rows: list[dict[str, str]]) -> None:
    raw_dir = root / "1_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(concept_rows).to_csv(raw_dir / "concepts_metadata.csv", index=False)
    desc_dir = raw_dir / "concepts"
    (desc_dir / "descriptions-paragraph").mkdir(parents=True, exist_ok=True)
    for row in concept_rows:
        concept_id = row["concept_id"]
        (desc_dir / "descriptions-paragraph" / f"{concept_id}.txt").write_text(
            f"Description for {concept_id}", encoding="utf-8"
        )


class TestContentAssets:
    def test_selected_combo_mappings_requires_catalog(self, tmp_path: Path) -> None:
        cohort_id = "cohort-missing"
        _write_manifest(tmp_path, cohort_id, ["combo-1"])

        context = build_asset_context(resources={"data_root": str(tmp_path)})

        from dagster import Failure

        with pytest.raises(Failure) as err:
            selected_combo_mappings(context, cohort_id=cohort_id)

        failure = err.value
        assert failure.metadata.get("error_code").value == "DATA_MISSING"

    def test_content_combinations_from_manifest(self, tmp_path: Path) -> None:
        cohort_id = "cohort-ok"
        combos = [
            {
                "combo_id": "combo-1",
                "version": "v1",
                "concept_id": "concept-a",
                "description_level": "paragraph",
                "k_max": 2,
                "created_at": "",
            },
            {
                "combo_id": "combo-1",
                "version": "v1",
                "concept_id": "concept-b",
                "description_level": "paragraph",
                "k_max": 2,
                "created_at": "",
            },
        ]
        _write_manifest(tmp_path, cohort_id, ["combo-1"])
        _write_concepts(
            tmp_path,
            [
                {"concept_id": "concept-a", "name": "Concept A"},
                {"concept_id": "concept-b", "name": "Concept B"},
            ],
        )
        pd.DataFrame(combos).to_csv(tmp_path / "combo_mappings.csv", index=False)

        context = build_asset_context(resources={"data_root": str(tmp_path)})

        mappings_df = selected_combo_mappings(context, cohort_id=cohort_id)
        assert not mappings_df.empty
        assert set(mappings_df["concept_id"].astype(str)) == {"concept-a", "concept-b"}

        combos_out = content_combinations(context, cohort_id=cohort_id)
        assert len(combos_out) == 1
        combo = combos_out[0]
        assert combo.combo_id == "combo-1"
        names = {entry.get("name") for entry in combo.contents}
        assert names == {"Concept A", "Concept B"}
