import pandas as pd
from pathlib import Path
import pytest

from dagster import Failure

from daydreaming_dagster.assets.group_generation_draft import _draft_response_impl as draft_response_impl
from daydreaming_dagster.assets.group_generation_essays import _essay_response_impl as essay_response_impl


class _FakeLogger:
    def info(self, *_args, **_kwargs):
        pass


class _Cfg:
    draft_generation_max_tokens = 128
    essay_generation_max_tokens = 64
    save_raw_draft_enabled = True
    save_raw_essay_enabled = True
    raw_draft_dir_override = None
    raw_essay_dir_override = None


class _FakeLLMTruncated:
    def __init__(self, text: str, completion_tokens: int, max_tokens: int):
        self._text = text
        self._ct = completion_tokens
        self._mt = max_tokens

    def generate_with_info(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: int | None = None):
        info = {
            "finish_reason": "length",
            "truncated": True,
            "usage": {"completion_tokens": self._ct, "max_tokens": self._mt},
        }
        return self._text, info

    # Compatibility fallback
    def generate(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: int | None = None):
        return self._text


class _Ctx:
    def __init__(self, partition_key: str, data_root: Path, cfg, llm):
        class _Res:
            pass

        self.partition_key = partition_key
        self.log = _FakeLogger()
        self.resources = _Res()
        self.resources.data_root = str(data_root)
        self.resources.experiment_config = cfg
        self.resources.openrouter_client = llm

    def add_output_metadata(self, _meta: dict):
        pass


def _write_minimal_templates(tmp: Path, draft_template: str = "t_draft", essay_template: str = "t_essay", generator: str = "llm"):
    raw = tmp / "1_raw"
    raw.mkdir(parents=True, exist_ok=True)
    # Draft templates CSV: parser column empty (identity)
    pd.DataFrame([
        {"template_id": draft_template, "template_name": "T Draft", "active": True, "parser": ""}
    ]).to_csv(raw / "draft_templates.csv", index=False)
    # Essay templates CSV: generator llm
    pd.DataFrame([
        {"template_id": essay_template, "template_name": "T Essay", "active": True, "generator": generator}
    ]).to_csv(raw / "essay_templates.csv", index=False)


def test_draft_truncation_fails_and_saves_raw(tmp_path: Path):
    _write_minimal_templates(tmp_path)
    draft_task_id = "cid_t_draft_modelX"
    tasks = pd.DataFrame([
        {"draft_task_id": draft_task_id, "combo_id": "cid", "draft_template": "t_draft", "generation_model_name": "modelX"}
    ])
    cfg = _Cfg()
    # Provide >=3 lines to pass the early min_lines check so truncation path is exercised
    text = "X\n" * 5
    ctx = _Ctx(draft_task_id, tmp_path, cfg, _FakeLLMTruncated(text, completion_tokens=cfg.draft_generation_max_tokens, max_tokens=cfg.draft_generation_max_tokens))

    with pytest.raises(Failure) as ei:
        draft_response_impl(ctx, draft_prompt="ignored", draft_generation_tasks=tasks)

    # Ensure RAW saved
    rp = tmp_path / "3_generation" / "draft_responses_raw"
    files = list(rp.glob(f"{draft_task_id}_v*.txt"))
    assert files, "RAW draft not saved on truncation"
    assert files[0].read_text(encoding="utf-8").startswith("X")
    # Ensure failure reason mentions truncation
    assert "truncated" in str(ei.value).lower()


def test_essay_truncation_fails_and_saves_raw(tmp_path: Path):
    _write_minimal_templates(tmp_path)
    essay_task_id = "eid_t_essay_modelY"
    tasks = pd.DataFrame([
        {
            "essay_task_id": essay_task_id,
            "draft_task_id": "did_t_draft_modelY",
            "combo_id": "cid",
            "draft_template": "t_draft",
            "essay_template": "t_essay",
            "generation_model_name": "modelY",
            "generation_model": "modelY",
        }
    ])
    cfg = _Cfg()
    text = "Y\n" * 5
    ctx = _Ctx(essay_task_id, tmp_path, cfg, _FakeLLMTruncated(text, completion_tokens=cfg.essay_generation_max_tokens, max_tokens=cfg.essay_generation_max_tokens))

    with pytest.raises(Failure) as ei:
        essay_response_impl(ctx, essay_prompt="ignored", essay_generation_tasks=tasks)

    # Ensure RAW saved
    rp = tmp_path / "3_generation" / "essay_responses_raw"
    files = list(rp.glob(f"{essay_task_id}_v*.txt"))
    assert files, "RAW essay not saved on truncation"
    assert files[0].read_text(encoding="utf-8").startswith("Y")
    assert "truncated" in str(ei.value).lower()
