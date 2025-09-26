from pathlib import Path
import json

import pytest

from daydreaming_dagster.assets.group_essay import essay_prompt, essay_raw, essay_parsed
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from tests.helpers.membership import write_membership_csv


pytestmark = pytest.mark.integration


"""Integration test: essay copy-mode mirrors draft content and metadata."""


def _write_membership(data_root: Path, rows: list[dict]):
    write_membership_csv(data_root, rows)


def test_essay_copy_mode_writes_only_parsed_and_metadata(tiny_data_root: Path, make_ctx, mock_llm):
    # Prepare parent draft parsed text
    parent_draft_id = "d-copy"
    draft_dir = tiny_data_root / "gens" / "draft" / parent_draft_id
    draft_dir.mkdir(parents=True, exist_ok=True)
    (draft_dir / "parsed.txt").write_text("X\nY\nZ\n", encoding="utf-8")

    # Membership rows to link essay to parent draft; template is generator=copy in tiny_data_root
    essay_id = "e-copy"
    _write_membership(
        tiny_data_root,
        [
            {
                "stage": "draft",
                "gen_id": parent_draft_id,
                "template_id": "test-draft",
                "llm_model_id": "m-gen",
            },
            {
                "stage": "essay",
                "gen_id": essay_id,
                "parent_gen_id": parent_draft_id,
                "template_id": "test-essay-copy",
                "llm_model_id": "m-gen",
            },
        ],
    )

    layer = GensDataLayer.from_root(tiny_data_root)
    layer.write_main_metadata(
        "essay",
        essay_id,
        {
            "stage": "essay",
            "gen_id": essay_id,
            "template_id": "test-essay-copy",
            "mode": "copy",
            "parent_gen_id": parent_draft_id,
        },
    )

    # Execute essay asset chain in copy mode (prompt -> raw -> parsed)
    ctx = make_ctx(essay_id, tiny_data_root, min_draft_lines=1, llm=mock_llm)
    prompt_text = essay_prompt(ctx)
    raw_text = essay_raw(ctx, prompt_text)
    parsed_text = essay_parsed(ctx)

    # Assert files match copy-mode expectations
    edir = tiny_data_root / "gens" / "essay" / essay_id
    assert (edir / "parsed.txt").exists()
    assert (edir / "metadata.json").exists()
    assert (edir / "raw.txt").exists()

    assert raw_text == "X\nY\nZ\n"
    assert parsed_text == raw_text
    assert (edir / "parsed.txt").read_text(encoding="utf-8") == parsed_text
    assert (edir / "raw.txt").read_text(encoding="utf-8") == raw_text

    meta = json.loads((edir / "metadata.json").read_text(encoding="utf-8"))
    assert meta["stage"] == "essay"
    assert meta["gen_id"] == essay_id
    assert meta.get("mode") == "copy"
    raw_meta = json.loads((edir / "raw_metadata.json").read_text(encoding="utf-8"))
    assert raw_meta.get("input_mode") == "copy"
    assert raw_meta.get("finish_reason") == "copy"
    # llm_model_id should be absent or None in copy mode
    assert meta.get("llm_model_id") in (None, "")
