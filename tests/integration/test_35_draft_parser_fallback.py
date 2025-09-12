from pathlib import Path

import pytest

from daydreaming_dagster.assets.group_generation_draft import _draft_response_impl as draft_response_impl
from daydreaming_dagster.unified.stage_services import render_template
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
        self.resources.experiment_config = ExperimentConfig(min_draft_lines=1)

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


def test_draft_parser_fallback_produces_parsed_text_when_configured(tiny_data_root: Path):
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
        "draft", "test-draft", {"concepts": [{"name": "A"}, {"name": "B"}]}
    )
    ctx = _Ctx(draft_id, tiny_data_root, _TaggedLLM())
    parsed = draft_response_impl(ctx, prompt)

    ddir = tiny_data_root / "gens" / "draft" / draft_id
    assert (ddir / "parsed.txt").exists()
    assert (ddir / "parsed.txt").read_text(encoding="utf-8").strip() == "Parsed Body"
    # Also ensure function return matches parsed text
    assert parsed.strip() == "Parsed Body"
