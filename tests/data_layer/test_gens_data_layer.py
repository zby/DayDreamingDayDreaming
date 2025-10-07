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
