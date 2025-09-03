import pandas as pd
from pathlib import Path

from daydreaming_dagster.assets.two_phase_generation import _essay_response_impl as essay_response_impl


class _FakeLogger:
    def info(self, *_args, **_kwargs):
        pass


class _FakeDraftIO:
    def __init__(self, content: str):
        self._content = content

    def load_input(self, _ctx):
        return self._content


class _FakeContext:
    def __init__(self, partition_key: str, data_root: Path, draft_io):
        class _Res:
            pass

        self.partition_key = partition_key
        self.log = _FakeLogger()
        self._meta = {}
        self.resources = _Res()
        self.resources.data_root = str(data_root)
        self.resources.draft_response_io_manager = draft_io
        # LLM present but unused in copy mode
        class _LLM:
            def generate(self, *_args, **_kwargs):
                return ""

        self.resources.openrouter_client = _LLM()

    def add_output_metadata(self, meta: dict):
        self._meta.update(meta)


def _write_minimal_essay_templates_csv(dir_path: Path):
    df = pd.DataFrame(
        [
            {
                "template_id": "copy-from-links-v1",
                "template_name": "Copy From Links",
                "description": "Copy links output as essay text (no LLM)",
                "active": True,
                "generator": "copy",
            }
        ]
    )
    out = dir_path / "1_raw" / "essay_templates.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)


def test_essay_response_copy_mode_returns_links_content(tmp_path: Path):
    _write_minimal_essay_templates_csv(tmp_path)

    link_task_id = "comboX_deliberate-rolling-thread-v2_sonnet-4"
    essay_template = "copy-from-links-v1"
    essay_task_id = f"{link_task_id}_{essay_template}"

    links_content = "Line A\nLine B\nLine C\n"

    tasks = pd.DataFrame(
        [
            {
                "essay_task_id": essay_task_id,
                "link_task_id": link_task_id,
                "link_template": "deliberate-rolling-thread-v2",
                "essay_template": essay_template,
                "generation_model_name": "unused",
            }
        ]
    )

    ctx = _FakeContext(
        partition_key=essay_task_id,
        data_root=tmp_path,
        draft_io=_FakeDraftIO(links_content),
    )

    result = essay_response_impl(ctx, essay_prompt="COPY_MODE", essay_generation_tasks=tasks)

    assert result == links_content
    mode = ctx._meta.get("mode")
    src = ctx._meta.get("source_link_task_id")
    assert (getattr(mode, "text", mode)) == "copy"
    assert (getattr(src, "text", src)) == link_task_id
