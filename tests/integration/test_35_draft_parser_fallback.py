from pathlib import Path
import os

import pytest

from daydreaming_dagster.assets.group_draft import draft_prompt, draft_raw, draft_parsed
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from tests.helpers.membership import write_membership_csv


pytestmark = pytest.mark.integration


"""Integration test: draft parser fallback produces parsed text."""


def _write_membership(data_root: Path, rows: list[dict]):
    write_membership_csv(data_root, rows)


def test_draft_parser_fallback_produces_parsed_text_when_configured(tiny_data_root: Path, make_ctx):
    # For this test, supply an LLM that emits an <essay> block, as the default mock_llm
    # does not include tags required by the 'essay_block' parser configured in tiny_data_root.
    class _TaggedLLM:
        def generate_with_info(self, prompt: str, *, model: str, max_tokens=None):
            body = "<essay>Parsed Body</essay>\nSCORE: 5.0"
            return body, {"finish_reason": "stop", "truncated": False, "usage": {}}

    draft_id = "d-parse"
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
            "combo_id": "c-ab",
            "llm_model_id": "m-gen",
        },
    )

    os.environ["GEN_TEMPLATES_ROOT"] = str(tiny_data_root / "1_raw" / "templates")

    content_combinations = [
        {
            "combo_id": "c-ab",
            "contents": [{"name": "A"}, {"name": "B"}],
        }
    ]

    ctx = make_ctx(draft_id, tiny_data_root, llm=_TaggedLLM(), min_draft_lines=1)
    prompt_text = draft_prompt(ctx, content_combinations)
    raw_text = draft_raw(ctx, prompt_text)
    parsed = draft_parsed(ctx, raw_text)

    ddir = tiny_data_root / "gens" / "draft" / draft_id
    assert (ddir / "parsed.txt").exists()
    assert (ddir / "parsed.txt").read_text(encoding="utf-8").strip() == "Parsed Body"
    # Function returns raw text uniformly; parsed file holds the extracted body
    assert "Parsed Body" in parsed
