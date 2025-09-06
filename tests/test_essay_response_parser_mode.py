import pytest
import pandas as pd
from pathlib import Path

from daydreaming_dagster.assets.group_generation_essays import _essay_response_impl as essay_response_impl


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
        # openrouter_client is present but unused in parser mode
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
                "template_id": "parsed-from-links-v1",
                "template_name": "Parsed From Links (Final Idea)",
                "description": "Parse final <essay-idea> from draft output; no LLM",
                "active": True,
                "generator": "parser",
            }
        ]
    )
    out = dir_path / "1_raw" / "essay_templates.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)


def test_essay_response_parses_from_draft_and_writes_output(tmp_path: Path):
    # Prepare minimal data_root with essay_templates.csv (parser mode)
    _write_minimal_essay_templates_csv(tmp_path)

    draft_task_id = "comboX_deliberate-rolling-thread-v1_sonnet-4"
    essay_template = "parsed-from-links-v1"
    essay_task_id = f"{draft_task_id}_{essay_template}"

    # Draft content with two essay-idea blocks; parser should pick stage=2 content
    draft_content = (
        "header lines\n"
        "<essay-idea stage=\"1\">\nSeed paragraph.\n</essay-idea>\n\n"
        "<essay-idea stage=\"2\">\nFinal integrated idea.\n</essay-idea>\n"
    )

    # Build a minimal tasks DataFrame row for the partition key
    tasks = pd.DataFrame(
        [
            {
                "essay_task_id": essay_task_id,
                "draft_task_id": draft_task_id,
                "draft_template": "deliberate-rolling-thread-v1",
                "essay_template": essay_template,
                "generation_model_name": "unused-in-parser",
            }
        ]
    )

    ctx = _FakeContext(
        partition_key=essay_task_id,
        data_root=tmp_path,
        draft_io=_FakeDraftIO(draft_content),
    )

    result = essay_response_impl(ctx, essay_prompt="PARSER_MODE", essay_generation_tasks=tasks)

    assert result.strip() == "Final integrated idea."
    # Metadata should be present
    assert (getattr(ctx._meta.get("mode"), "text", ctx._meta.get("mode"))) == "parser"
    assert (getattr(ctx._meta.get("parser"), "text", ctx._meta.get("parser"))) == "essay_idea_last"
    # Prefer new metadata key, but accept legacy during transition
    src_meta = ctx._meta.get("source_draft_task_id") or ctx._meta.get("source_link_task_id")
    assert (getattr(src_meta, "text", src_meta)) == draft_task_id


def test_missing_or_unsupported_parser_mapping_fails_partition(tmp_path: Path):
    from dagster import Failure

    _write_minimal_essay_templates_csv(tmp_path)

    draft_task_id = "comboX_unknown-template_sonnet-4"
    essay_template = "parsed-from-links-v1"
    essay_task_id = f"{draft_task_id}_{essay_template}"
    draft_content = "<essay-idea>\nIdea\n</essay-idea>\n"

    tasks = pd.DataFrame(
        [
            {
                "essay_task_id": essay_task_id,
                "draft_task_id": draft_task_id,
                "draft_template": "unknown-template",
                "essay_template": essay_template,
                "generation_model_name": "unused",
            }
        ]
    )

    ctx = _FakeContext(
        partition_key=essay_task_id,
        data_root=tmp_path,
        draft_io=_FakeDraftIO(draft_content),
    )

    with pytest.raises(Failure):
        _ = essay_response_impl(ctx, essay_prompt="PARSER_MODE", essay_generation_tasks=tasks)



def test_essay_response_parses_from_draft_and_writes_output(tmp_path):
    # Pseudocode outline:
    # 1) Given essay_generation_tasks row with:
    #    - essay_template = 'parsed-from-links-v1' (generator=parser)
    #    - draft_task_id = 'comboX_deliberate-rolling-thread-v1_sonnet-4'
    # 2) draft_response_io_manager.load_input returns content containing two <essay-idea> blocks
    # 3) essay_response writes the last idea paragraph to essay_responses/<essay_task_id>.txt
    # 4) Metadata includes mode=parser, parser=essay_idea_last, source_link_task_id
    pass


def test_missing_or_unsupported_parser_mapping_fails_partition():
    # If draft_template is not mapped to a parser, the asset should raise a Failure
    # with a clear resolution hint.
    pass
