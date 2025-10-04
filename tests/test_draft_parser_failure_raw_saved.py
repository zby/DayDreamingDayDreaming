import os
from pathlib import Path

import pandas as pd
import pytest

from dagster import Failure

from daydreaming_dagster.assets.group_draft import draft_raw, draft_parsed
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from tests.helpers.membership import write_membership_csv


class _FakeLLM:
    def __init__(self, text: str):
        self._text = text

    def generate(self, *_args, **_kwargs):
        return self._text

    # Provide the info-style API to match production client
    def generate_with_info(self, *_args, **_kwargs):
        return self._text, {"finish_reason": "stop", "truncated": False}


def _write_draft_templates_csv(dir_path: Path, template_id: str, parser: str):
    df = pd.DataFrame(
        [
            {
                "template_id": template_id,
                "template_name": "Test Draft Template",
                "description": "Uses parser for extraction",
                "parser": parser,
                # Intentionally omit 'generator' column to trigger Failure in resolve_generator_mode
            }
        ]
    )
    out = dir_path / "1_raw" / "draft_templates.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)


def _write_membership(dir_path: Path, *, gen_id: str):
    write_membership_csv(
        dir_path,
        [
            {
                "stage": "draft",
                "gen_id": gen_id,
            }
        ],
        cohort="TEST",
    )


def _seed_metadata(tmp_path: Path, gen_id: str, template_id: str):
    layer = GensDataLayer.from_root(tmp_path)
    layer.write_main_metadata(
        "draft",
        gen_id,
        {
            "stage": "draft",
            "gen_id": gen_id,
            "template_id": template_id,
            "mode": "llm",
            "combo_id": "comboX",
            "llm_model_id": "sonnet-4",
        },
    )


def test_draft_parser_strict_requires_tags(tmp_path: Path, make_ctx):
    template_id = "deliberate-rolling-thread-strict"
    _write_draft_templates_csv(tmp_path, template_id, parser="essay_block")

    gen_id = "d_gen_strict"
    _write_membership(tmp_path, gen_id=gen_id)
    raw_text = "Line A\nLine B\nLine C\n"
    _seed_metadata(tmp_path, gen_id, template_id)

    os.environ["GEN_TEMPLATES_ROOT"] = str(tmp_path / "1_raw" / "templates")
    ctx = make_ctx(gen_id, tmp_path, llm=_FakeLLM(raw_text), min_draft_lines=3)
    result = draft_raw(ctx, draft_prompt="ignored")
    assert result == raw_text

    with pytest.raises(Failure):
        draft_parsed(ctx)


def test_draft_parser_lenient_passes_without_tags(tmp_path: Path, make_ctx):
    # Arrange: draft template with parser=essay_block_lenient, raw text has no structured tags
    template_id = "deliberate-rolling-thread-test"
    _write_draft_templates_csv(tmp_path, template_id, parser="essay_block_lenient")

    gen_id = "d_gen_123"
    _write_membership(tmp_path, gen_id=gen_id)

    # RAW content with >=3 lines to pass early validation, but no <essay> tags for the parser
    raw_text = "Line A\nLine B\nLine C\n"
    _seed_metadata(tmp_path, gen_id, template_id)

    os.environ["GEN_TEMPLATES_ROOT"] = str(tmp_path / "1_raw" / "templates")
    ctx = make_ctx(gen_id, tmp_path, llm=_FakeLLM(raw_text), min_draft_lines=3)

    result = draft_raw(ctx, draft_prompt="ignored")
    assert result == raw_text

    parsed_text = draft_parsed(ctx)
    assert parsed_text.strip() == raw_text.strip()

    # In membership-first mode, failures may occur before RAW write depending on setup.
    # If RAW exists, ensure it matches the LLM output; otherwise proceed.
    raw_fp = tmp_path / "gens" / "draft" / gen_id / "raw.txt"
    assert raw_fp.exists()
    content = raw_fp.read_text(encoding="utf-8")
    assert content == raw_text

    parsed_fp = tmp_path / "gens" / "draft" / gen_id / "parsed.txt"
    assert parsed_fp.exists()
    assert parsed_fp.read_text(encoding="utf-8").strip() == raw_text.strip()
