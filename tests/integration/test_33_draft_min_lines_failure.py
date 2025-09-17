from pathlib import Path
import os

import pytest
from dagster import Failure

from daydreaming_dagster.assets.group_draft import draft_prompt, draft_raw, draft_parsed
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from tests.helpers.membership import write_membership_csv


pytestmark = pytest.mark.integration


"""Integration test: draft min_lines failure still writes raw+metadata."""


def _write_membership(data_root: Path, rows: list[dict]):
    write_membership_csv(data_root, rows)


@pytest.mark.llm_cfg(lines=1)
def test_draft_min_lines_failure_writes_debug_then_raises(tiny_data_root: Path, mock_llm, make_ctx):
    draft_id = "d-minfail"
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

    layer = GensDataLayer.from_root(tiny_data_root)
    layer.write_main_metadata(
        "draft",
        draft_id,
        {
            "template_id": "test-draft",
            "mode": "llm",
            "combo_id": "c-only",
            "llm_model_id": "m-gen",
        },
    )

    os.environ["GEN_TEMPLATES_ROOT"] = str(tiny_data_root / "1_raw" / "templates")

    content_combinations = [
        {
            "combo_id": "c-only",
            "contents": [{"name": "OnlyOne"}],
        }
    ]

    ctx = make_ctx(draft_id, tiny_data_root, llm=mock_llm, min_draft_lines=3)
    prompt_text = draft_prompt(ctx, content_combinations)
    raw_text = draft_raw(ctx, prompt_text)
    with pytest.raises(Failure):
        _ = draft_parsed(ctx, raw_text)

    # Filesystem: raw/metadata exist; parsed.txt absent (prompt not written here)
    ddir = tiny_data_root / "gens" / "draft" / draft_id
    assert (ddir / "raw.txt").exists()
    assert (ddir / "metadata.json").exists()
    assert not (ddir / "parsed.txt").exists()
