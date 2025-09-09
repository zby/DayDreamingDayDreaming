import pandas as pd
from pathlib import Path

from daydreaming_dagster.assets.group_generation_essays import _essay_response_impl as essay_response_impl
from daydreaming_dagster.resources.experiment_config import ExperimentConfig


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
        # Provide experiment configuration expected by assets
        self.resources.experiment_config = ExperimentConfig()
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


def test_essay_response_copy_mode_returns_draft_content(tmp_path: Path):
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

    tasks = pd.DataFrame(
        [
            {
                "essay_task_id": essay_task_id,
                "parent_gen_id": parent_gen_id,
                "draft_template": "deliberate-rolling-thread-v2",
                "essay_template": essay_template,
                "generation_model_name": "unused",
                "gen_id": essay_gen_id,
            }
        ]
    )

    ctx = _FakeContext(
        partition_key=essay_gen_id,
        data_root=tmp_path,
        draft_io=_FakeDraftIO(draft_content),
    )

    result = essay_response_impl(ctx, essay_prompt="COPY_MODE", essay_generation_tasks=tasks)

    assert result == draft_content
    mode = ctx._meta.get("mode")
    src_parent = ctx._meta.get("parent_gen_id")
    assert (getattr(mode, "text", mode)) == "copy"
    assert (getattr(src_parent, "text", src_parent)) == parent_gen_id
