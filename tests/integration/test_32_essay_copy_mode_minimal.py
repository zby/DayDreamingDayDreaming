from pathlib import Path
import json

import pytest

from daydreaming_dagster.unified.stage_services import essay_response_asset as essay_response_impl
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from tests.helpers.membership import write_membership_csv


pytestmark = pytest.mark.integration


class _Log:
    def info(self, *_args, **_kwargs):
        pass


class _Ctx:
    def __init__(self, partition_key: str, data_root: Path, llm=None):
        class _Res:
            pass

        self.partition_key = partition_key
        self.log = _Log()
        self.resources = _Res()
        self.resources.data_root = str(data_root)
        self.resources.openrouter_client = llm  # unused in copy mode
        self.resources.experiment_config = ExperimentConfig(min_draft_lines=1)

    def add_output_metadata(self, _md: dict):
        pass


def _write_membership(data_root: Path, rows: list[dict]):
    write_membership_csv(data_root, rows)


def test_essay_copy_mode_writes_only_parsed_and_metadata(tiny_data_root: Path):
    # Prepare parent draft parsed text
    parent_draft_id = "d-copy"
    draft_dir = tiny_data_root / "gens" / "draft" / parent_draft_id
    draft_dir.mkdir(parents=True, exist_ok=True)
    (draft_dir / "parsed.txt").write_text("X\nY\nZ\n", encoding="utf-8")

    # Membership rows to link essay to parent draft; template is generator=copy in tiny_data_root
    essay_id = "e-copy"
    _write_membership(
        tiny_data_root,
        [
            {
                "stage": "draft",
                "gen_id": parent_draft_id,
                "template_id": "test-draft",
                "llm_model_id": "m-gen",
            },
            {
                "stage": "essay",
                "gen_id": essay_id,
                "parent_gen_id": parent_draft_id,
                "template_id": "test-essay-copy",
                "llm_model_id": "m-gen",
            },
        ],
    )

    # Call impl directly in copy mode (resolved via CSV, not override)
    ctx = _Ctx(essay_id, tiny_data_root)
    _ = essay_response_impl(ctx, essay_prompt="irrelevant")

    # Assert only parsed.txt and metadata.json exist
    edir = tiny_data_root / "gens" / "essay" / essay_id
    assert (edir / "parsed.txt").exists()
    assert (edir / "metadata.json").exists()
    assert not (edir / "prompt.txt").exists()
    assert not (edir / "raw.txt").exists()

    meta = json.loads((edir / "metadata.json").read_text(encoding="utf-8"))
    assert meta.get("mode") == "copy"
    files = meta.get("files", {})
    assert set(files.keys()) == {"parsed"}
    # llm_model_id should be absent or None in copy mode
    assert meta.get("llm_model_id") in (None, "")
