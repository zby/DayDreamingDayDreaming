from pathlib import Path
import os

import pytest

from daydreaming_dagster.assets.group_evaluation import (
    evaluation_prompt,
    evaluation_raw,
    evaluation_parsed,
)
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from tests.helpers.membership import write_membership_csv

pytestmark = pytest.mark.integration

def _write_membership(data_root: Path, rows: list[dict]):
    write_membership_csv(data_root, rows)

@pytest.mark.llm_cfg(tail_score="8.5")
def test_evaluation_parser_in_last_line_emits_single_float_line(tiny_data_root: Path, mock_llm, make_ctx):
    # Create a minimal essay parent document
    essay_parent_id = "e-for-eval"
    e_dir = tiny_data_root / "gens" / "essay" / essay_parent_id
    e_dir.mkdir(parents=True, exist_ok=True)
    (e_dir / "parsed.txt").write_text("Lorem ipsum dolor...", encoding="utf-8")

    # Membership row for evaluation linking to essay parent and using test-eval template
    eval_id = "v-parse"
    _write_membership(
        tiny_data_root,
        [
            {
                "stage": "evaluation",
                "gen_id": eval_id,
                "parent_gen_id": essay_parent_id,
                "template_id": "test-eval",
                "llm_model_id": "m-eval",
            }
        ],
    )

    layer = GensDataLayer.from_root(tiny_data_root)
    layer.write_main_metadata(
        "evaluation",
        eval_id,
        {
            "template_id": "test-eval",
            "mode": "llm",
            "parent_gen_id": essay_parent_id,
            "llm_model_id": "m-eval",
        },
    )

    os.environ["GEN_TEMPLATES_ROOT"] = str(tiny_data_root / "1_raw" / "templates")

    ctx = make_ctx(eval_id, tiny_data_root, llm=mock_llm, min_draft_lines=3)
    prompt_text = evaluation_prompt(ctx)
    raw_text = evaluation_raw(ctx, prompt_text)
    parsed_text = evaluation_parsed(ctx, raw_text)

    vdir = tiny_data_root / "gens" / "evaluation" / eval_id
    assert (vdir / "parsed.txt").exists()
    assert (vdir / "parsed.txt").read_text(encoding="utf-8") == "8.5\n"
    assert parsed_text == "8.5\n"
