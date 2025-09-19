from pathlib import Path

import pytest
from dagster import AssetKey, build_input_context, build_output_context

from daydreaming_dagster.resources.io_managers import InMemoryIOManager


def _build_contexts(partition_key: str):
    upstream = build_output_context(asset_key=AssetKey(["draft_raw"]), partition_key=partition_key)
    input_ctx = build_input_context(
        partition_key=partition_key,
        asset_key=AssetKey(["draft_parsed"]),
        upstream_output=upstream,
    )
    return upstream, input_ctx


def test_fallback_reads_raw_txt(tmp_path: Path):
    gens_dir = tmp_path / "gens" / "draft" / "gid1"
    gens_dir.mkdir(parents=True, exist_ok=True)
    raw_path = gens_dir / "raw.txt"
    raw_path.write_text("fallback raw", encoding="utf-8")

    manager = InMemoryIOManager(fallback_data_root=tmp_path)
    _, input_ctx = _build_contexts("gid1")

    assert manager.load_input(input_ctx) == "fallback raw"


def test_fallback_missing_raises(tmp_path: Path):
    manager = InMemoryIOManager(fallback_data_root=tmp_path)
    _, input_ctx = _build_contexts("gid-missing")

    with pytest.raises(KeyError):
        manager.load_input(input_ctx)
