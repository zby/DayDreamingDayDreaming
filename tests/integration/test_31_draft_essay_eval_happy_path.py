import json
from pathlib import Path

import pytest

from daydreaming_dagster.unified.stage_services import render_template, execute_evaluation_llm
from daydreaming_dagster.assets.group_generation_draft import _draft_response_impl as draft_response_impl
from daydreaming_dagster.assets.group_generation_essays import _essay_response_impl as essay_response_impl
from daydreaming_dagster.assets.group_evaluation import evaluation_response
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from dagster import build_op_context


pytestmark = pytest.mark.integration


class _Log:
    def info(self, *_args, **_kwargs):
        pass


class _Ctx:
    """Minimal Dagster-like context for calling asset impls directly."""

    def __init__(self, partition_key: str, data_root: Path, llm):
        class _Res:
            pass

        self.partition_key = partition_key
        self.log = _Log()
        self._output_md = {}
        self.resources = _Res()
        self.resources.data_root = str(data_root)
        self.resources.openrouter_client = llm
        self.resources.experiment_config = ExperimentConfig(min_draft_lines=3)

    def add_output_metadata(self, md: dict):
        # Store last write for inspection if needed
        self._output_md = md


def _write_membership(
    data_root: Path,
    rows: list[dict],
):
    cohort = "T-INTEG"
    cdir = data_root / "cohorts" / cohort
    cdir.mkdir(parents=True, exist_ok=True)
    import pandas as pd

    # Normalize to canonical columns
    cols = [
        "stage",
        "gen_id",
        "cohort_id",
        "parent_gen_id",
        "combo_id",
        "template_id",
        "llm_model_id",
    ]
    nr = []
    for r in rows:
        d = {k: r.get(k, "") for k in cols}
        if not d["cohort_id"]:
            d["cohort_id"] = cohort
        nr.append(d)
    df = pd.DataFrame(nr, columns=cols)
    (cdir / "membership.csv").write_text(df.to_csv(index=False), encoding="utf-8")


def _read_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def test_happy_path_draft_essay_eval_chain(tiny_data_root: Path, mock_llm, canon_meta):
    import os
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

    # Draft: render a simple prompt and run LLM path
    draft_prompt_text = render_template(
        "draft",
        "test-draft",
        {"concepts": [{"name": "Alpha"}, {"name": "Beta"}]},
        templates_root=tiny_data_root / "1_raw" / "templates",
    )
    # Use a tagged LLM so the configured 'essay_block' parser produces a parsed.txt
    class _TaggedLLM:
        def generate_with_info(self, prompt: str, *, model: str, max_tokens=None):
            body = "<essay>LineA\nLineB\nLineC</essay>\nSCORE: 7.0"
            return body, {"finish_reason": "stop", "truncated": False, "usage": {}}

    dctx = _Ctx(draft_id, tiny_data_root, _TaggedLLM())
    _ = draft_response_impl(dctx, draft_prompt_text)

    # Ensure templates_root resolves templates from tiny_data_root
    os.environ["GEN_TEMPLATES_ROOT"] = str(tiny_data_root / "1_raw" / "templates")

    # Essay (LLM mode): implement uses CSV to resolve mode and loads draft parsed text
    ectx = _Ctx(essay_id, tiny_data_root, mock_llm)
    _ = essay_response_impl(ectx, essay_prompt="ok")

    # Evaluation: run via stage_services directly
    doc_text = (tiny_data_root / "gens" / "essay" / essay_id / "parsed.txt").read_text(encoding="utf-8")
    _ = execute_evaluation_llm(
        llm=mock_llm,
        out_dir=tiny_data_root / "gens",
        gen_id=eval_id,
        template_id="test-eval",
        prompt_text=render_template("evaluation", "test-eval", {"response": doc_text}),
        model="m-eval",
        parser_name="in_last_line",
        max_tokens=2048,
        parent_gen_id=essay_id,
    )

    # Assert filesystem layout and basic invariants
    ddir = tiny_data_root / "gens" / "draft" / draft_id
    edir = tiny_data_root / "gens" / "essay" / essay_id
    vdir = tiny_data_root / "gens" / "evaluation" / eval_id

    # Draft: prompt/raw/metadata exist; parsed may be absent depending on parser fallback
    assert (ddir / "prompt.txt").exists()
    assert (ddir / "raw.txt").exists()
    assert (ddir / "metadata.json").exists()

    # Essay: all files exist; parsed equals normalized raw
    assert (edir / "prompt.txt").exists()
    assert (edir / "raw.txt").exists()
    assert (edir / "parsed.txt").exists()
    assert (edir / "metadata.json").exists()
    essay_raw = (edir / "raw.txt").read_text(encoding="utf-8").replace("\r\n", "\n")
    essay_parsed = (edir / "parsed.txt").read_text(encoding="utf-8")
    assert essay_parsed == essay_raw

    # Evaluation: parsed equals single float line 7.0 (default tail_score)
    assert (vdir / "prompt.txt").exists()
    assert (vdir / "raw.txt").exists()
    assert (vdir / "parsed.txt").exists()
    assert (vdir / "metadata.json").exists()
    assert (vdir / "parsed.txt").read_text(encoding="utf-8") == "7.0\n"

    # Canonical metadata checks (drop duration, baselined files)
    dmeta = canon_meta(_read_json(ddir / "metadata.json"))
    emeta = canon_meta(_read_json(edir / "metadata.json"))
    vmeta = canon_meta(_read_json(vdir / "metadata.json"))

    # Draft canonical fields
    assert dmeta["stage"] == "draft"
    assert dmeta["gen_id"] == draft_id
    assert dmeta["template_id"] == "test-draft"
    assert dmeta["mode"] == "llm"
    assert dmeta.get("llm_model_id") == "m-gen"
    assert dmeta.get("finish_reason") == "stop"
    assert dmeta.get("truncated") is False
    assert set(dmeta.get("files", {}).keys()) == {"prompt", "raw"} or set(dmeta.get("files", {}).keys()) == {"prompt", "raw", "parsed"}

    # Essay canonical fields
    assert emeta["stage"] == "essay"
    assert emeta["gen_id"] == essay_id
    assert emeta["template_id"] == "test-essay-llm"
    assert emeta["mode"] == "llm"
    assert emeta.get("llm_model_id") == "m-gen"
    assert emeta.get("finish_reason") == "stop"
    assert emeta.get("truncated") is False
    # Metadata may omit files.parsed in LLM path (written after metadata for debuggability)
    assert set(emeta.get("files", {}).keys()) in ({"prompt", "raw"}, {"prompt", "raw", "parsed"})

    # Evaluation canonical fields
    assert vmeta["stage"] == "evaluation"
    assert vmeta["gen_id"] == eval_id
    assert vmeta["template_id"] == "test-eval"
    assert vmeta["mode"] == "llm"
    assert vmeta.get("llm_model_id") == "m-eval"
    assert vmeta.get("finish_reason") == "stop"
    assert vmeta.get("truncated") is False
    # Parser configured via evaluation_templates.csv
    assert vmeta.get("parser_name") == "in_last_line"
    assert set(vmeta.get("files", {}).keys()) in ({"prompt", "raw"}, {"prompt", "raw", "parsed"})
