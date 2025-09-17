import json
import os
from pathlib import Path

import pandas as pd

from daydreaming_dagster.assets.group_essay import essay_prompt, essay_raw, essay_parsed
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer


class _FakeLLM:
    def generate_with_info(self, *_args, **_kwargs):
        return "", {"finish_reason": "stop", "truncated": False}


def _write_minimal_essay_templates_csv(dir_path: Path):
    df = pd.DataFrame(
        [
            {
                "template_id": "copy-from-drafts-v1",
                "template_name": "Copy From Drafts",
                "description": "Copy draft output as essay text (no LLM)",
                "active": True,
                "generator": "copy",
            }
        ]
    )
    out = dir_path / "1_raw" / "essay_templates.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)


def test_essay_response_copy_mode_returns_draft_content(tmp_path: Path, make_ctx):
    _write_minimal_essay_templates_csv(tmp_path)

    # Prepare a parent draft generation on filesystem
    parent_gen_id = "genX"
    draft_dir = tmp_path / "gens" / "draft" / parent_gen_id
    draft_dir.mkdir(parents=True, exist_ok=True)
    essay_template = "copy-from-drafts-v1"
    essay_task_id = f"essay_for_{parent_gen_id}_{essay_template}"
    essay_gen_id = "e_gen_999"

    draft_content = "Line A\nLine B\nLine C\n"
    (draft_dir / "parsed.txt").write_text(draft_content, encoding="utf-8")

    # Seed membership.csv to resolve parent and template
    cohort = "TEST-COPY"
    (tmp_path / "cohorts" / cohort).mkdir(parents=True, exist_ok=True)
    membership = pd.DataFrame([
        {"stage": "draft", "gen_id": parent_gen_id, "cohort_id": cohort, "parent_gen_id": "", "combo_id": "c1", "template_id": "deliberate-rolling-thread-v2", "llm_model_id": "unused_model"},
        {"stage": "essay", "gen_id": essay_gen_id, "cohort_id": cohort, "parent_gen_id": parent_gen_id, "combo_id": "c1", "template_id": essay_template, "llm_model_id": "unused_model"},
    ])
    (tmp_path / "cohorts" / cohort / "membership.csv").write_text(membership.to_csv(index=False), encoding="utf-8")

    layer = GensDataLayer.from_root(tmp_path)
    layer.write_main_metadata(
        "essay",
        essay_gen_id,
        {
            "stage": "essay",
            "gen_id": essay_gen_id,
            "template_id": essay_template,
            "mode": "copy",
            "parent_gen_id": parent_gen_id,
        },
    )

    os.environ["GEN_TEMPLATES_ROOT"] = str(tmp_path / "1_raw" / "templates")

    ctx = make_ctx(essay_gen_id, tmp_path, min_draft_lines=1, llm=_FakeLLM())

    prompt_text = essay_prompt(ctx)
    raw_text = essay_raw(ctx, prompt_text)
    parsed_text = essay_parsed(ctx, raw_text)

    assert raw_text == draft_content
    assert parsed_text == draft_content

    raw_meta = json.loads((tmp_path / "gens" / "essay" / essay_gen_id / "raw_metadata.json").read_text(encoding="utf-8"))
    parsed_meta = json.loads((tmp_path / "gens" / "essay" / essay_gen_id / "parsed_metadata.json").read_text(encoding="utf-8"))

    assert raw_meta.get("mode") == "copy"
    assert parsed_meta.get("parent_gen_id") == parent_gen_id
