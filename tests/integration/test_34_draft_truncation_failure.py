from pathlib import Path
import json

import pytest

from daydreaming_dagster.unified.stage_services import render_template
from daydreaming_dagster.unified.stage_services import draft_response_asset as draft_response_impl
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from tests.helpers.membership import write_membership_csv


pytestmark = pytest.mark.integration


"""Integration test: draft truncation failure records truncated flag + raw/metadata."""


def _write_membership(data_root: Path, rows: list[dict]):
    write_membership_csv(data_root, rows)


@pytest.mark.llm_cfg(lines=3, truncated=True)
def test_draft_truncation_failure_writes_debug_then_raises(tiny_data_root: Path, mock_llm, canon_meta, make_ctx):
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

    ctx = make_ctx(draft_id, tiny_data_root, llm=mock_llm, min_draft_lines=3)
    with pytest.raises(Exception):
        _ = draft_response_impl(ctx, prompt)

    ddir = tiny_data_root / "gens" / "draft" / draft_id
    assert (ddir / "raw.txt").exists()
    assert (ddir / "metadata.json").exists()
    assert not (ddir / "parsed.txt").exists()

    # Verify metadata.truncated == true (canonicalized)
    m = canon_meta(json.loads((ddir / "metadata.json").read_text(encoding="utf-8")))
    assert m.get("truncated") is True
