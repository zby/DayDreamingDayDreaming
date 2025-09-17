from __future__ import annotations

import types
from pathlib import Path

import daydreaming_dagster.unified.stage_inputs as stage_inputs
from daydreaming_dagster.data_layer.gens_data_layer import (
    GensDataLayer,
    resolve_generation_metadata,
)
from daydreaming_dagster.data_layer.paths import Paths


def test_stage_input_helper_copy(tmp_path: Path) -> None:
    paths = Paths.from_str(tmp_path)
    parent_dir = paths.generation_dir("draft", "P1")
    parent_dir.mkdir(parents=True, exist_ok=True)
    (parent_dir / "parsed.txt").write_text("Copy text", encoding="utf-8")
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "essay",
        "E1",
        {"template_id": "essay-tpl", "mode": "copy", "parent_gen_id": "P1"},
    )
    text, info = stage_inputs._stage_input_asset(
        data_layer=data_layer,
        stage="essay",
        gen_id="E1",
    )

    assert text == "Copy text"
    assert info["input_mode"] == "copy"
    assert info["copied_from"].endswith("parsed.txt")
    assert data_layer.paths.input_path("essay", "E1").read_text(encoding="utf-8") == "Copy text"


def test_stage_input_helper_draft(tmp_path: Path, monkeypatch) -> None:
    paths = Paths.from_str(tmp_path)
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "draft",
        "D1",
        {"template_id": "draft-tpl", "mode": "llm", "combo_id": "c1"},
    )
    combo = types.SimpleNamespace(combo_id="c1", contents=["idea"])

    monkeypatch.setattr(
        stage_inputs,
        "render_template",
        lambda stage, template, values: "rendered",
    )
    text, info = stage_inputs._stage_input_asset(
        data_layer=data_layer,
        stage="draft",
        gen_id="D1",
        content_combinations=[combo],
    )

    assert text == "rendered"
    assert info["input_mode"] == "prompt"
    assert info["combo_id"] == "c1"
    assert data_layer.paths.input_path("draft", "D1").read_text(encoding="utf-8") == "rendered"


def test_stage_input_asset_wires_metadata(tmp_path: Path, monkeypatch) -> None:
    paths = Paths.from_str(tmp_path)
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "essay",
        "E1",
        {"template_id": "essay-tpl", "mode": "copy", "parent_gen_id": "P1"},
    )
    parent_dir = paths.generation_dir("draft", "P1")
    parent_dir.mkdir(parents=True, exist_ok=True)
    (parent_dir / "parsed.txt").write_text("Parent", encoding="utf-8")

    ctx = types.SimpleNamespace(
        partition_key="E1",
        resources=types.SimpleNamespace(data_root=str(tmp_path)),
        captured_metadata=None,
        add_output_metadata=lambda md: setattr(ctx, "captured_metadata", md),
    )

    stage_inputs.stage_input_asset(ctx, "essay")

    assert ctx.captured_metadata["mode"].value == "copy"
    assert ctx.captured_metadata["input_length"].value == len("Parent")
