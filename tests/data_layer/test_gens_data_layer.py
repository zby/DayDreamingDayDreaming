from __future__ import annotations

import json
from pathlib import Path

import pytest

from daydreaming_dagster.data_layer.gens_data_layer import (
    GensDataLayer,
    resolve_generation_metadata,
)
from daydreaming_dagster.utils.errors import DDError, Err


@pytest.fixture
def layer(tmp_path: Path) -> GensDataLayer:
    return GensDataLayer.from_root(tmp_path)


def _write_metadata(layer: GensDataLayer, stage: str, gen_id: str, payload: dict) -> Path:
    path = layer.paths.metadata_path(stage, gen_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_write_input_round_trip(layer: GensDataLayer) -> None:
    stage = "draft"
    gen_id = "G1"
    layer.reserve_generation(stage, gen_id)
    text = "Hello world"

    target = layer.write_input(stage, gen_id, text)

    assert target.exists()
    assert target.read_text(encoding="utf-8") == text


def test_input_exists_force(layer: GensDataLayer) -> None:
    stage = "draft"
    gen_id = "G2"

    assert layer.input_exists(stage, gen_id) is False

    layer.write_input(stage, gen_id, "prompt")
    assert layer.input_exists(stage, gen_id) is True
    assert layer.input_exists(stage, gen_id, force=True) is False


def test_write_main_metadata_round_trip(layer: GensDataLayer) -> None:
    payload = {"template_id": "tpl", "mode": "copy", "parent_gen_id": "P1"}
    path = layer.write_main_metadata("essay", "E1", payload)
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8")) == payload


def test_write_raw_and_parsed_round_trip(layer: GensDataLayer) -> None:
    layer.write_raw("draft", "G1", "RAW")
    layer.write_parsed("draft", "G1", "PARSED")
    assert layer.read_raw("draft", "G1") == "RAW"
    assert layer.read_parsed("draft", "G1") == "PARSED"


def test_raw_and_parsed_metadata_round_trip(layer: GensDataLayer) -> None:
    raw_meta = {"mode": "llm", "truncated": False}
    parsed_meta = {"parser_name": "identity", "success": True}
    layer.write_raw_metadata("essay", "E1", raw_meta)
    layer.write_parsed_metadata("essay", "E1", parsed_meta)
    assert layer.read_raw_metadata("essay", "E1") == raw_meta
    assert layer.read_parsed_metadata("essay", "E1") == parsed_meta


def test_delete_downstream_from_input(layer: GensDataLayer) -> None:
    stage = "draft"
    gen_id = "cleanup"
    layer.reserve_generation(stage, gen_id)
    layer.write_raw(stage, gen_id, "raw")
    layer.write_raw_metadata(stage, gen_id, {"mode": "llm"})
    layer.write_parsed(stage, gen_id, "parsed")
    layer.write_parsed_metadata(stage, gen_id, {"parser_name": "identity"})

    layer.delete_downstream_artifacts(stage, gen_id, from_stage="input")

    assert layer.raw_exists(stage, gen_id) is False
    assert not layer.paths.raw_metadata_path(stage, gen_id).exists()
    assert layer.parsed_exists(stage, gen_id) is False
    assert not layer.paths.parsed_metadata_path(stage, gen_id).exists()


def test_delete_downstream_from_raw(layer: GensDataLayer) -> None:
    stage = "draft"
    gen_id = "cleanup"
    layer.reserve_generation(stage, gen_id)
    layer.write_raw(stage, gen_id, "raw")
    layer.write_raw_metadata(stage, gen_id, {"mode": "llm"})
    layer.write_parsed(stage, gen_id, "parsed")
    layer.write_parsed_metadata(stage, gen_id, {"parser_name": "identity"})

    layer.delete_downstream_artifacts(stage, gen_id, from_stage="raw")

    assert layer.raw_exists(stage, gen_id) is True
    assert layer.paths.raw_metadata_path(stage, gen_id).exists()
    assert layer.parsed_exists(stage, gen_id) is False
    assert not layer.paths.parsed_metadata_path(stage, gen_id).exists()


def test_delete_downstream_invalid_stage(layer: GensDataLayer) -> None:
    with pytest.raises(DDError) as err:
        layer.delete_downstream_artifacts("draft", "cleanup", from_stage="parsed")

    assert err.value.code is Err.INVALID_CONFIG
    assert err.value.ctx.get("reason") == "unsupported_downstream_cleanup"


def test_read_parsed_missing(layer: GensDataLayer) -> None:
    with pytest.raises(DDError) as err:
        layer.read_parsed("essay", "missing")
    assert err.value.code is Err.DATA_MISSING
    assert err.value.ctx.get("artifact") == "parsed"


def test_parsed_exists(layer: GensDataLayer) -> None:
    stage = "evaluation"
    gen_id = "V1"

    # Should return False when file doesn't exist
    assert layer.parsed_exists(stage, gen_id) is False

    # Should return True after file is written
    layer.write_parsed(stage, gen_id, "parsed content")
    assert layer.parsed_exists(stage, gen_id) is True
    assert layer.parsed_exists(stage, gen_id, force=True) is False


def test_raw_exists_force(layer: GensDataLayer) -> None:
    stage = "draft"
    gen_id = "R1"

    assert layer.raw_exists(stage, gen_id) is False

    layer.write_raw(stage, gen_id, "raw content")
    assert layer.raw_exists(stage, gen_id) is True
    assert layer.raw_exists(stage, gen_id, force=True) is False


def test_read_main_metadata_missing(layer: GensDataLayer) -> None:
    with pytest.raises(DDError) as err:
        layer.read_main_metadata("draft", "missing")
    assert err.value.code is Err.DATA_MISSING
    assert err.value.ctx.get("artifact") == "metadata"


def test_read_main_metadata_malformed(layer: GensDataLayer) -> None:
    path = layer.paths.metadata_path("draft", "bad")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not-json", encoding="utf-8")

    with pytest.raises(DDError) as err:
        layer.read_main_metadata("draft", "bad")
    assert err.value.code is Err.PARSER_FAILURE
    assert err.value.ctx.get("artifact") == "metadata"


def test_resolve_metadata_copy_requires_parent(layer: GensDataLayer) -> None:
    _write_metadata(
        layer,
        "draft",
        "no-parent",
        {
            "template_id": "tpl",
            "mode": "copy",
        },
    )

    with pytest.raises(DDError) as err:
        resolve_generation_metadata(layer, "draft", "no-parent")
    assert err.value.code is Err.INVALID_CONFIG
    assert err.value.ctx.get("reason") == "missing_parent"


def test_resolve_metadata_stage_requires_parent(layer: GensDataLayer) -> None:
    _write_metadata(
        layer,
        "essay",
        "no-parent",
        {
            "template_id": "tpl",
            "mode": "llm",
        },
    )

    with pytest.raises(DDError) as err:
        resolve_generation_metadata(layer, "essay", "no-parent")
    assert err.value.code is Err.INVALID_CONFIG
    assert err.value.ctx.get("reason") == "missing_parent"


def test_resolve_metadata_success(layer: GensDataLayer) -> None:
    _write_metadata(
        layer,
        "evaluation",
        "E1",
        {
            "template_id": "eval-tpl",
            "mode": "copy",
            "parent_gen_id": "ESS1",
            "combo_id": "combo-3",
            "origin_cohort_id": "cohort-a",
        },
    )

    meta = resolve_generation_metadata(layer, "evaluation", "E1")
    assert meta.stage == "evaluation"
    assert meta.template_id == "eval-tpl"
    assert meta.parent_gen_id == "ESS1"
    assert meta.mode == "copy"
    assert meta.combo_id == "combo-3"
    assert meta.origin_cohort_id == "cohort-a"
