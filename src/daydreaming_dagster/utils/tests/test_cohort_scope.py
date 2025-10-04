from pathlib import Path
import json

import pandas as pd

import pytest

from daydreaming_dagster.utils.cohort_scope import CohortScope
from daydreaming_dagster.utils.errors import DDError, Err


def _write_metadata(root: Path, stage: str, gen_id: str, data: dict):
    path = root / "gens" / stage / gen_id
    path.mkdir(parents=True, exist_ok=True)
    (path / "metadata.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def test_stage_gen_ids_reads_membership(tmp_path: Path):
    data_root = tmp_path
    (data_root / "cohorts" / "demo").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"stage": "draft", "gen_id": "d1"},
            {"stage": "essay", "gen_id": "e1"},
            {"stage": "draft", "gen_id": "d1"},
        ]
    ).to_csv(data_root / "cohorts" / "demo" / "membership.csv", index=False)

    _write_metadata(
        data_root,
        "draft",
        "d1",
        {
            "template_id": "Draft-Alpha",
            "combo_id": "Combo-1",
            "llm_model_id": "Model-X",
            "replicate": 1,
        },
    )

    scope = CohortScope(data_root)

    assert scope.stage_gen_ids("demo", "draft") == ["d1", "d1"]
    assert scope.stage_gen_ids("demo", "essay") == ["e1"]


def test_membership_missing_columns_raises(tmp_path: Path) -> None:
    data_root = tmp_path
    (data_root / "cohorts" / "bad").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"foo": "x"}]).to_csv(data_root / "cohorts" / "bad" / "membership.csv", index=False)

    scope = CohortScope(data_root)
    with pytest.raises(DDError) as err:
        scope.stage_gen_ids("bad", "draft")
    assert err.value.code is Err.INVALID_CONFIG
