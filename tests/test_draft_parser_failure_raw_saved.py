import pandas as pd
from pathlib import Path
import pytest

from daydreaming_dagster.assets.groups.group_generation_draft import _draft_response_impl as draft_response_impl
from dagster import Failure


class _FakeLogger:
    def info(self, *_args, **_kwargs):
        pass


class _FakeContext:
    def __init__(self, partition_key: str, data_root: Path, experiment_config, llm):
        class _Res:
            pass

        self.partition_key = partition_key
        self.log = _FakeLogger()
        self._meta = {}
        self.resources = _Res()
        self.resources.data_root = str(data_root)
        self.resources.experiment_config = experiment_config
        self.resources.openrouter_client = llm

    def add_output_metadata(self, meta: dict):
        self._meta.update(meta)


class _FakeLLM:
    def __init__(self, text: str):
        self._text = text

    def generate(self, *_args, **_kwargs):
        return self._text


class _Cfg:
    # Ensure raw is saved and default path is used
    save_raw_draft_enabled = True
    raw_draft_dir_override = None
    draft_generation_max_tokens = 1024
    min_draft_lines = 3


def _write_draft_templates_csv(dir_path: Path, template_id: str, parser: str):
    df = pd.DataFrame(
        [
            {
                "template_id": template_id,
                "template_name": "Test Draft Template",
                "description": "Uses parser for extraction",
                "active": True,
                "parser": parser,
            }
        ]
    )
    out = dir_path / "1_raw" / "draft_templates.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)


def test_draft_parser_failure_saves_raw_then_fails(tmp_path: Path):
    # Arrange: draft template with parser=essay_block, but raw text has no <essay> block
    template_id = "deliberate-rolling-thread-test"
    _write_draft_templates_csv(tmp_path, template_id, parser="essay_block")

    draft_task_id = f"comboX_{template_id}_sonnet-4"
    tasks = pd.DataFrame(
        [
            {
                "draft_task_id": draft_task_id,
                "combo_id": "comboX",
                "draft_template": template_id,
                "generation_model": "sonnet-4",
                "generation_model_name": "sonnet-4",
            }
        ]
    )

    # RAW content with >=3 lines to pass early validation, but no <essay> tags for the parser
    raw_text = "Line A\nLine B\nLine C\n"
    ctx = _FakeContext(
        partition_key=draft_task_id,
        data_root=tmp_path,
        experiment_config=_Cfg(),
        llm=_FakeLLM(raw_text),
    )

    # Act + Assert: draft_response should raise Failure due to parser error
    with pytest.raises(Failure):
        _ = draft_response_impl(ctx, draft_prompt="ignored", draft_generation_tasks=tasks)

    # And RAW should be saved to data/3_generation/draft_responses_raw/<id>_v1.txt
    raw_dir = tmp_path / "3_generation" / "draft_responses_raw"
    assert raw_dir.exists(), "RAW directory not created"
    files = list(raw_dir.glob(f"{draft_task_id}_v*.txt"))
    assert files, "RAW file not written"
    content = files[0].read_text(encoding="utf-8")
    assert content == raw_text
