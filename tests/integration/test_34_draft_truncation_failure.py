from pathlib import Path
import json

import pytest

from daydreaming_dagster.unified.stage_services import render_template
from daydreaming_dagster.assets.group_generation_draft import _draft_response_impl as draft_response_impl
from daydreaming_dagster.resources.experiment_config import ExperimentConfig


pytestmark = pytest.mark.integration


class _Log:
    def info(self, *_args, **_kwargs):
        pass


class _Ctx:
    def __init__(self, partition_key: str, data_root: Path, llm):
        class _Res:
            pass

        self.partition_key = partition_key
        self.log = _Log()
        self.resources = _Res()
        self.resources.data_root = str(data_root)
        self.resources.openrouter_client = llm
        self.resources.experiment_config = ExperimentConfig(min_draft_lines=3)

    def add_output_metadata(self, _md: dict):
        pass


def _write_membership(data_root: Path, rows: list[dict]):
    import pandas as pd

    cohort = "T-INTEG"
    cdir = data_root / "cohorts" / cohort
    cdir.mkdir(parents=True, exist_ok=True)
    cols = [
        "stage",
        "gen_id",
        "cohort_id",
        "parent_gen_id",
        "combo_id",
        "template_id",
        "llm_model_id",
    ]
    norm = []
    for r in rows:
        d = {k: r.get(k, "") for k in cols}
        if not d["cohort_id"]:
            d["cohort_id"] = cohort
        norm.append(d)
    df = pd.DataFrame(norm, columns=cols)
    (cdir / "membership.csv").write_text(df.to_csv(index=False), encoding="utf-8")


@pytest.mark.llm_cfg(lines=3, truncated=True)
def test_draft_truncation_failure_writes_debug_then_raises(tiny_data_root: Path, mock_llm, canon_meta):
    draft_id = "d-trunc"
    _write_membership(
        tiny_data_root,
        [
            {
                "stage": "draft",
                "gen_id": draft_id,
                "template_id": "test-draft",
                "llm_model_id": "m-gen",
            }
        ],
    )

    prompt = render_template(
        "draft", "test-draft", {"concepts": [{"name": "A"}, {"name": "B"}]},
        templates_root=tiny_data_root / "1_raw" / "templates",
    )

    ctx = _Ctx(draft_id, tiny_data_root, mock_llm)
    with pytest.raises(Exception):
        _ = draft_response_impl(ctx, prompt)

    ddir = tiny_data_root / "gens" / "draft" / draft_id
    assert (ddir / "raw.txt").exists()
    assert (ddir / "metadata.json").exists()
    assert not (ddir / "parsed.txt").exists()

    # Verify metadata.truncated == true (canonicalized)
    m = canon_meta(json.loads((ddir / "metadata.json").read_text(encoding="utf-8")))
    assert m.get("truncated") is True
