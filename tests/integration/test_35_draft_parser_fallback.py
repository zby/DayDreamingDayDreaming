from pathlib import Path

import pytest

from daydreaming_dagster.unified.stage_services import draft_response_asset as draft_response_impl
from daydreaming_dagster.unified.stage_services import render_template
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
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

    # Render a valid prompt and run
    prompt = render_template(
        "draft", "test-draft", {"concepts": [{"name": "A"}, {"name": "B"}]},
        templates_root=tiny_data_root / "1_raw" / "templates",
    )
    ctx = make_ctx(draft_id, tiny_data_root, llm=_TaggedLLM(), min_draft_lines=1)
    parsed = draft_response_impl(ctx, prompt)

    ddir = tiny_data_root / "gens" / "draft" / draft_id
    assert (ddir / "parsed.txt").exists()
    assert (ddir / "parsed.txt").read_text(encoding="utf-8").strip() == "Parsed Body"
    # Function returns raw text uniformly; parsed file holds the extracted body
    assert "Parsed Body" in parsed
