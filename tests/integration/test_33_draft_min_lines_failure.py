from pathlib import Path
import json

import pytest

from daydreaming_dagster.unified.stage_services import render_template
from daydreaming_dagster.unified.stage_services import draft_response_asset as draft_response_impl
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from tests.helpers.membership import write_membership_csv


pytestmark = pytest.mark.integration


class _Log:
    def info(self, *_args, **_kwargs):
        pass


class _Ctx:
    def __init__(self, partition_key: str, data_root: Path, llm, *, min_lines: int):
        class _Res:
            pass

        self.partition_key = partition_key
        self.log = _Log()
        self.resources = _Res()
        self.resources.data_root = str(data_root)
        self.resources.openrouter_client = llm
        self.resources.experiment_config = ExperimentConfig(min_draft_lines=min_lines)

    def add_output_metadata(self, _md: dict):
        pass


def _write_membership(data_root: Path, rows: list[dict]):
    write_membership_csv(data_root, rows)


@pytest.mark.llm_cfg(lines=1)
def test_draft_min_lines_failure_writes_debug_then_raises(tiny_data_root: Path, mock_llm):
    draft_id = "d-minfail"
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

    # Prepare a valid prompt
    prompt = render_template(
        "draft", "test-draft", {"concepts": [{"name": "OnlyOne"}]},
        templates_root=tiny_data_root / "1_raw" / "templates",
    )

    ctx = _Ctx(draft_id, tiny_data_root, mock_llm, min_lines=3)
    with pytest.raises(Exception):
        _ = draft_response_impl(ctx, prompt)

    # Filesystem: raw/metadata exist; parsed.txt absent (prompt not written here)
    ddir = tiny_data_root / "gens" / "draft" / draft_id
    assert (ddir / "raw.txt").exists()
    assert (ddir / "metadata.json").exists()
    assert not (ddir / "parsed.txt").exists()
