from pathlib import Path
import json

import pandas as pd

from daydreaming_dagster.utils.cohort_scope import CohortScope
from daydreaming_dagster.utils.ids import draft_signature, compute_deterministic_gen_id


def _write_metadata(root: Path, stage: str, gen_id: str, data: dict):
    path = root / "gens" / stage / gen_id
    path.mkdir(parents=True, exist_ok=True)
    (path / "metadata.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def test_signature_lookup(tmp_path: Path):
    data_root = tmp_path
    (data_root / "cohorts" / "test").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"stage": "draft", "gen_id": "d1"},
    ]).to_csv(data_root / "cohorts" / "test" / "membership.csv", index=False)

    meta = {
        "template_id": "Draft-Alpha",
        "combo_id": "Combo-1",
        "llm_model_id": "Model-X",
        "replicate": 1,
    }
    _write_metadata(data_root, "draft", "d1", meta)

    scope = CohortScope(data_root)

    sig = scope.signature_for_gen("draft", "d1")
    assert sig == draft_signature("Combo-1", "Draft-Alpha", "Model-X", 1)

    det_id = compute_deterministic_gen_id("draft", sig)
    assert det_id.startswith("d_")
    assert scope.find_existing_gen_id("draft", sig) == "d1"
