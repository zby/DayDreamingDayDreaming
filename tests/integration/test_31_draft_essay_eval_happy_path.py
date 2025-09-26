import json
import os
from pathlib import Path

import pytest

from daydreaming_dagster.assets.group_draft import draft_prompt, draft_raw, draft_parsed
from daydreaming_dagster.assets.group_essay import essay_prompt, essay_raw, essay_parsed
from daydreaming_dagster.assets.group_evaluation import (
    evaluation_prompt,
    evaluation_raw,
    evaluation_parsed,
)
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from tests.helpers.membership import write_membership_csv


pytestmark = pytest.mark.integration


"""Integration happy-path test for draft → essay → evaluation chain."""


def _write_membership(data_root: Path, rows: list[dict]):
    write_membership_csv(data_root, rows)


def _read_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def test_happy_path_draft_essay_eval_chain(tiny_data_root: Path, mock_llm, make_ctx):
    # Arrange membership rows (one draft -> one essay(llm) -> one evaluation)
    draft_id = "d1"
    essay_id = "e1"
    eval_id = "v1"
    _write_membership(
        tiny_data_root,
        [
            {
                "stage": "draft",
                "gen_id": draft_id,
                "template_id": "test-draft",
                "combo_id": "c-test",
                "llm_model_id": "m-gen",
            },
            {
                "stage": "essay",
                "gen_id": essay_id,
                "parent_gen_id": draft_id,
                "template_id": "test-essay-llm",
                "llm_model_id": "m-gen",
            },
            {
                "stage": "evaluation",
                "gen_id": eval_id,
                "parent_gen_id": essay_id,
                "template_id": "test-eval",
                "llm_model_id": "m-eval",
            },
        ],
    )

    layer = GensDataLayer.from_root(tiny_data_root)
    layer.write_main_metadata(
        "draft",
        draft_id,
        {
            "stage": "draft",
            "gen_id": draft_id,
            "template_id": "test-draft",
            "mode": "llm",
            "combo_id": "c-test",
            "llm_model_id": "m-gen",
        },
    )
    layer.write_main_metadata(
        "essay",
        essay_id,
        {
            "stage": "essay",
            "gen_id": essay_id,
            "template_id": "test-essay-llm",
            "mode": "llm",
            "parent_gen_id": draft_id,
            "llm_model_id": "m-gen",
        },
    )
    layer.write_main_metadata(
        "evaluation",
        eval_id,
        {
            "stage": "evaluation",
            "gen_id": eval_id,
            "template_id": "test-eval",
            "mode": "llm",
            "parent_gen_id": essay_id,
            "llm_model_id": "m-eval",
        },
    )

    # Ensure templates resolve from the tiny data root for all stages
    os.environ["GEN_TEMPLATES_ROOT"] = str(tiny_data_root / "1_raw" / "templates")

    content_combinations = [
        {
            "combo_id": "c-test",
            "contents": [{"name": "Alpha"}, {"name": "Beta"}],
        }
    ]

    # Use a tagged LLM so the configured 'essay_block' parser produces a parsed.txt
    class _TaggedLLM:
        def generate_with_info(self, prompt: str, *, model: str, max_tokens=None):
            body = "<essay>LineA\nLineB\nLineC</essay>\nSCORE: 7.0"
            return body, {"finish_reason": "stop", "truncated": False, "usage": {}}

    dctx = make_ctx(draft_id, tiny_data_root, llm=_TaggedLLM(), min_draft_lines=3)
    draft_prompt_text = draft_prompt(dctx, content_combinations)
    draft_raw_text = draft_raw(dctx, draft_prompt_text)
    draft_parsed_text = draft_parsed(dctx)

    # Essay (LLM mode): asset chain uses upstream parsed draft
    ectx = make_ctx(essay_id, tiny_data_root, llm=mock_llm, min_draft_lines=3)
    essay_prompt_text = essay_prompt(ectx)
    essay_raw_text = essay_raw(ectx, essay_prompt_text)
    essay_parsed_text = essay_parsed(ectx)

    # Evaluation asset chain uses essay parsed output
    vctx = make_ctx(eval_id, tiny_data_root, llm=mock_llm, min_draft_lines=3)
    evaluation_prompt_text = evaluation_prompt(vctx)
    evaluation_raw_text = evaluation_raw(vctx, evaluation_prompt_text)
    evaluation_parsed_text = evaluation_parsed(vctx)

    # Assert filesystem layout and basic invariants
    ddir = tiny_data_root / "gens" / "draft" / draft_id
    edir = tiny_data_root / "gens" / "essay" / essay_id
    vdir = tiny_data_root / "gens" / "evaluation" / eval_id

    # Draft: raw/metadata exist; parsed may be absent depending on parser fallback
    assert (ddir / "raw.txt").exists()
    assert (ddir / "metadata.json").exists()

    # Essay: all files exist; parsed equals normalized raw (prompt is not written here)
    assert (edir / "raw.txt").exists()
    assert (edir / "parsed.txt").exists()
    assert (edir / "metadata.json").exists()
    essay_raw_file = (edir / "raw.txt").read_text(encoding="utf-8").replace("\r\n", "\n")
    essay_parsed_file = (edir / "parsed.txt").read_text(encoding="utf-8")
    assert essay_parsed_file == essay_raw_file

    # Evaluation: parsed equals single float line 7.0 (default tail_score)
    assert (vdir / "raw.txt").exists()
    assert (vdir / "parsed.txt").exists()
    assert (vdir / "metadata.json").exists()
    assert (vdir / "parsed.txt").read_text(encoding="utf-8") == "7.0\n"

    # Canonical metadata checks (drop duration, baselined files)
    dmeta = _read_json(ddir / "metadata.json")
    emeta = _read_json(edir / "metadata.json")
    vmeta = _read_json(vdir / "metadata.json")

    # Draft canonical fields from main metadata
    assert dmeta["stage"] == "draft"
    assert dmeta["gen_id"] == draft_id
    assert dmeta["template_id"] == "test-draft"
    assert dmeta["mode"] == "llm"
    assert dmeta.get("llm_model_id") == "m-gen"

    # Essay canonical fields
    assert emeta["stage"] == "essay"
    assert emeta["gen_id"] == essay_id
    assert emeta["template_id"] == "test-essay-llm"
    assert emeta["mode"] == "llm"
    assert emeta.get("llm_model_id") == "m-gen"

    # Evaluation canonical fields
    assert vmeta["stage"] == "evaluation"
    assert vmeta["gen_id"] == eval_id
    assert vmeta["template_id"] == "test-eval"
    assert vmeta["mode"] == "llm"
    assert vmeta.get("llm_model_id") == "m-eval"

    # Raw metadata reflects runtime details
    draft_raw_meta = _read_json(ddir / "raw_metadata.json")
    essay_raw_meta = _read_json(edir / "raw_metadata.json")
    evaluation_raw_meta = _read_json(vdir / "raw_metadata.json")

    assert draft_raw_meta.get("finish_reason") == "stop"
    assert draft_raw_meta.get("truncated") is False
    assert draft_raw_meta.get("mode") == "llm"

    assert essay_raw_meta.get("finish_reason") == "stop"
    assert essay_raw_meta.get("truncated") is False
    assert essay_raw_meta.get("mode") == "llm"

    assert evaluation_raw_meta.get("finish_reason") == "stop"
    assert evaluation_raw_meta.get("truncated") is False
    assert evaluation_raw_meta.get("mode") == "llm"

    eval_parsed_meta = _read_json(vdir / "parsed_metadata.json")
    assert eval_parsed_meta.get("parser_name") == "in_last_line"
