from pathlib import Path

import pytest
from dagster import AssetKey, build_input_context, build_output_context

from daydreaming_dagster.resources.io_managers import InMemoryIOManager, RehydratingIOManager
from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.resources.gens_prompt_io_manager import GensPromptIOManager
from daydreaming_dagster.resources.llm_client import LLMClientResource


def _build_contexts(partition_key: str):
    upstream = build_output_context(asset_key=AssetKey(["draft_raw"]), partition_key=partition_key)
    input_ctx = build_input_context(
        partition_key=partition_key,
        asset_key=AssetKey(["draft_parsed"]),
        upstream_output=upstream,
    )
    return upstream, input_ctx


def test_rehydrating_manager_reads_raw_txt(tmp_path: Path):
    gens_dir = tmp_path / "gens" / "draft" / "gid1"
    gens_dir.mkdir(parents=True, exist_ok=True)
    raw_path = gens_dir / "raw.txt"
    raw_path.write_text("fallback raw", encoding="utf-8")

    manager = RehydratingIOManager(tmp_path)
    _, input_ctx = _build_contexts("gid1")

    assert manager.load_input(input_ctx) == "fallback raw"


def test_rehydrating_manager_missing_disk_artifact(tmp_path: Path):
    manager = RehydratingIOManager(tmp_path)
    _, input_ctx = _build_contexts("gid-missing")

    with pytest.raises(DDError) as err:
        manager.load_input(input_ctx)
    assert err.value.code is Err.DATA_MISSING


def test_in_memory_manager_missing_value_raises(tmp_path: Path):
    manager = InMemoryIOManager()
    _, input_ctx = _build_contexts("gid-missing")

    with pytest.raises(DDError) as err:
        manager.load_input(input_ctx)
    assert err.value.code is Err.DATA_MISSING


def test_gens_prompt_io_manager_invalid_stage(tmp_path: Path):
    gens_root = tmp_path / "gens"
    gens_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(DDError) as err:
        GensPromptIOManager(gens_root, stage="invalid")
    assert err.value.code is Err.INVALID_CONFIG


def test_gens_prompt_io_manager_requires_str_output(tmp_path: Path):
    gens_root = tmp_path / "gens"
    gens_root.mkdir(parents=True, exist_ok=True)
    manager = GensPromptIOManager(gens_root, stage="draft")

    ctx = build_output_context(asset_key=AssetKey(["draft_prompt"]), partition_key="G1")
    with pytest.raises(DDError) as err:
        manager.handle_output(ctx, 123)
    assert err.value.code is Err.INVALID_CONFIG


def test_llm_client_requires_api_key(tmp_path: Path):
    client = LLMClientResource(api_key=None, data_root=str(tmp_path))

    with pytest.raises(DDError) as err:
        client.generate("hi", model="foo", max_tokens=16)
    assert err.value.code is Err.INVALID_CONFIG


def test_llm_client_invalid_max_tokens(tmp_path: Path, monkeypatch):
    client = LLMClientResource(api_key="dummy", data_root=str(tmp_path))

    # Patch OpenAI to avoid actual instantiation side effects
    monkeypatch.setattr("daydreaming_dagster.resources.llm_client.OpenAI", lambda **kwargs: object())

    with pytest.raises(DDError) as err:
        client.generate_with_info("prompt", model="foo", max_tokens=0)
    assert err.value.code is Err.INVALID_CONFIG
