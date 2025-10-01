from __future__ import annotations

import types
from pathlib import Path

import pytest

from daydreaming_dagster.checks import documents_checks as checks
from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.types import STAGES


def _make_context(data_root: Path, partition_key: str = "G1"):
    resources = types.SimpleNamespace(data_root=data_root)
    return types.SimpleNamespace(partition_key=partition_key, resources=resources)


def test_checks_by_stage_exports_expected_functions():
    assert set(checks.CHECKS_BY_STAGE.keys()) == set(STAGES)
    assert checks.CHECKS_BY_STAGE["draft"] is checks.draft_files_exist_check
    assert checks.CHECKS_BY_STAGE["essay"] is checks.essay_files_exist_check
    assert checks.CHECKS_BY_STAGE["evaluation"] is checks.evaluation_files_exist_check


def test_files_exist_check_metadata(tmp_path: Path):
    ctx = _make_context(tmp_path)
    result = checks._files_exist_check_impl(ctx, "draft")
    expected_path = tmp_path / "gens" / "draft" / "G1"

    assert not result.passed
    assert "gen_dir" in result.metadata
    assert result.metadata["gen_dir"].path == str(expected_path)


@pytest.mark.parametrize("stage", STAGES)
def test_files_exist_check_passes_when_parsed_exists(tmp_path: Path, stage: str):
    ctx = _make_context(tmp_path, partition_key="ID42")
    paths = Paths.from_str(tmp_path)
    parsed_path = paths.parsed_path(stage, "ID42")
    parsed_path.parent.mkdir(parents=True, exist_ok=True)
    parsed_path.write_text("body", encoding="utf-8")

    result = checks._files_exist_check_impl(ctx, stage)

    assert result.passed
    assert result.metadata["gen_dir"].path == str(paths.generation_dir(stage, "ID42"))
