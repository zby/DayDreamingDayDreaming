from __future__ import annotations

import json
import types
from pathlib import Path

import daydreaming_dagster.unified.stage_parsed as stage_parsed
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer, resolve_generation_metadata
from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.resources.experiment_config import ExperimentConfig, StageSettings


def _write_main_and_raw(tmp_path: Path, stage: str, gen_id: str, main: dict, raw: dict | None = None, raw_text: str = "raw") -> Paths:
    paths = Paths.from_str(tmp_path)
    layer = GensDataLayer.from_root(tmp_path)
    layer.reserve_generation(stage, gen_id)
    layer.write_main_metadata(stage, gen_id, main)
    layer.write_raw(stage, gen_id, raw_text)
    if raw is not None:
        layer.write_raw_metadata(stage, gen_id, raw)
    return paths


def test_stage_parsed_helper_identity(tmp_path: Path) -> None:
    paths = _write_main_and_raw(
        tmp_path,
        "draft",
        "D1",
        {"template_id": "tpl", "mode": "llm", "combo_id": "c1"},
        raw={"input_mode": "prompt", "truncated": False},
        raw_text="Line1\nLine2",
    )
    layer = GensDataLayer.from_root(tmp_path)

    parsed_text, parsed_metadata = stage_parsed._stage_parsed_asset(
        layer=layer,
        stage="draft",
        gen_id="D1",
        raw_text="Line1\nLine2",
        parser_name="identity",
        raw_metadata={"input_mode": "prompt", "truncated": False, "combo_id": "c1"},
        stage_settings=StageSettings(generation_max_tokens=20480, min_lines=1),
        min_lines_override=None,
        fail_on_truncation=True,
    )

    assert parsed_text == "Line1\nLine2"
    parsed_path = paths.parsed_path("draft", "D1")
    assert parsed_path.read_text(encoding="utf-8") == "Line1\nLine2"
    meta_file = json.loads(paths.parsed_metadata_path("draft", "D1").read_text(encoding="utf-8"))
    assert meta_file["parser_name"] == "identity"
    assert parsed_metadata["combo_id"] == "c1"
    assert parsed_metadata["parsed_path"] == str(parsed_path)


def test_stage_parsed_helper_truncation_guard(tmp_path: Path) -> None:
    layer = GensDataLayer.from_root(tmp_path)
    layer.write_main_metadata("essay", "E1", {"template_id": "tpl", "mode": "copy", "parent_gen_id": "D1"})
    try:
        stage_parsed._stage_parsed_asset(
            layer=layer,
            stage="essay",
            gen_id="E1",
            raw_text="text",
            parser_name="identity",
            raw_metadata={"truncated": True},
            stage_settings=StageSettings(),
            min_lines_override=None,
            fail_on_truncation=True,
        )
    except ValueError as err:
        assert "truncated" in str(err)
    else:
        raise AssertionError("Expected ValueError due to truncation")


def test_stage_parsed_asset_wires_metadata(tmp_path: Path, monkeypatch) -> None:
    paths = _write_main_and_raw(
        tmp_path,
        "draft",
        "D1",
        {"template_id": "tpl", "mode": "llm", "combo_id": "c7"},
        raw={"truncated": False, "input_mode": "prompt"},
        raw_text="RAW",
    )

    ctx = types.SimpleNamespace(
        partition_key="D1",
        resources=types.SimpleNamespace(
            data_root=str(tmp_path),
            experiment_config=ExperimentConfig(
                stage_config={
                    "draft": StageSettings(generation_max_tokens=20480, min_lines=1),
                    "essay": StageSettings(generation_max_tokens=20480),
                    "evaluation": StageSettings(generation_max_tokens=20480),
                }
            ),
        ),
        captured=None,
        add_output_metadata=lambda md: setattr(ctx, "captured", md),
    )

    monkeypatch.setattr(stage_parsed, "resolve_parser_name", lambda root, stage, tpl, override: "identity")

    out = stage_parsed.stage_parsed_asset(ctx, "draft", raw_text="RAW")

    assert out == "RAW"
    parsed_path = paths.parsed_path("draft", "D1")
    assert parsed_path.read_text(encoding="utf-8") == "RAW"
    assert ctx.captured["parser_name"].value == "identity"
    assert ctx.captured["parsed_path"].value == str(parsed_path)
