from __future__ import annotations

import types
from pathlib import Path

import pytest

import daydreaming_dagster.unified.stage_inputs as stage_inputs
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.resources.experiment_config import ExperimentConfig, StageSettings
from daydreaming_dagster.utils.errors import DDError, Err


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
    data_layer.write_raw("essay", "E1", "old raw")
    data_layer.write_raw_metadata("essay", "E1", {"mode": "llm"})
    data_layer.write_parsed("essay", "E1", "old parsed")
    data_layer.write_parsed_metadata("essay", "E1", {"parser_name": "identity"})
    text, info = stage_inputs._stage_input_asset(
        data_layer=data_layer,
        stage="essay",
        gen_id="E1",
    )

    assert text == "Copy text"
    assert info["input_mode"] == "copy"
    assert info["copied_from"].endswith("parsed.txt")
    assert "reused" not in info
    assert (
        data_layer.paths.input_path("essay", "E1").read_text(encoding="utf-8")
        == "Copy text"
    )
    assert data_layer.raw_exists("essay", "E1") is False
    assert data_layer.parsed_exists("essay", "E1") is False


def test_stage_input_helper_draft(tmp_path: Path, monkeypatch) -> None:
    paths = Paths.from_str(tmp_path)
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "draft",
        "D1",
        {"template_id": "draft-tpl", "mode": "llm", "combo_id": "c1"},
    )
    data_layer.write_raw("draft", "D1", "old raw")
    data_layer.write_raw_metadata("draft", "D1", {"mode": "llm"})
    data_layer.write_parsed("draft", "D1", "old parsed")
    data_layer.write_parsed_metadata("draft", "D1", {"parser_name": "identity"})
    combo = types.SimpleNamespace(combo_id="c1", contents=["idea"])

    monkeypatch.setattr(
        stage_inputs,
        "render_template",
        lambda stage, template, values, *, paths: "rendered",
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
    assert "reused" not in info
    assert (
        data_layer.paths.input_path("draft", "D1").read_text(encoding="utf-8")
        == "rendered"
    )
    assert data_layer.raw_exists("draft", "D1") is False
    assert data_layer.parsed_exists("draft", "D1") is False


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
    assert "reused" not in ctx.captured_metadata


def test_stage_input_helper_reuses_existing_prompt(tmp_path: Path, monkeypatch) -> None:
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "draft",
        "D1",
        {"template_id": "draft-tpl", "mode": "llm", "combo_id": "c1"},
    )
    data_layer.write_input("draft", "D1", "existing prompt")
    data_layer.write_raw("draft", "D1", "raw")
    data_layer.write_raw_metadata("draft", "D1", {"mode": "llm"})

    def _fail(*_args, **_kwargs):
        raise AssertionError("should not render")

    monkeypatch.setattr(stage_inputs, "render_template", _fail)

    text, info = stage_inputs._stage_input_asset(
        data_layer=data_layer,
        stage="draft",
        gen_id="D1",
        reuse_existing=True,
    )

    assert text == "existing prompt"
    assert "reused" not in info
    assert info["input_mode"] == "prompt"
    assert data_layer.read_input("draft", "D1") == "existing prompt"
    assert data_layer.raw_exists("draft", "D1") is True


def test_stage_input_asset_reuse_skips_combinations(tmp_path: Path, monkeypatch) -> None:
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "draft",
        "D1",
        {"template_id": "draft-tpl", "mode": "llm", "combo_id": "c1"},
    )
    data_layer.write_input("draft", "D1", "existing prompt")
    data_layer.write_raw("draft", "D1", "raw")
    data_layer.write_raw_metadata("draft", "D1", {"mode": "llm"})

    ctx = types.SimpleNamespace(
        partition_key="D1",
        resources=types.SimpleNamespace(
            data_root=str(tmp_path),
            experiment_config=ExperimentConfig(
                stage_config={"draft": StageSettings(generation_max_tokens=1024)}
            ),
        ),
        captured_metadata=None,
        add_output_metadata=lambda md: setattr(ctx, "captured_metadata", md),
    )

    def _fail_again(*_args, **_kwargs):
        raise AssertionError("should not render")

    monkeypatch.setattr(stage_inputs, "render_template", _fail_again)

    out = stage_inputs.stage_input_asset(ctx, "draft")

    assert out == "existing prompt"
    assert "reused" not in ctx.captured_metadata
    assert ctx.captured_metadata["input_mode"].value == "prompt"
    assert data_layer.raw_exists("draft", "D1") is True


def test_stage_input_copy_missing_parent(tmp_path: Path) -> None:
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "essay",
        "E2",
        {"template_id": "essay-tpl", "mode": "copy"},
    )

    with pytest.raises(DDError) as err:
        stage_inputs._stage_input_asset(
            data_layer=data_layer,
            stage="essay",
            gen_id="E2",
        )

    assert err.value.code is Err.INVALID_CONFIG
    assert err.value.ctx.get("reason") == "missing_parent"


def test_stage_input_missing_combo(tmp_path: Path, monkeypatch) -> None:
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "draft",
        "D2",
        {"template_id": "draft-tpl", "mode": "llm", "combo_id": "C9"},
    )

    monkeypatch.setattr(stage_inputs, "render_template", lambda *_args, **_kwargs: "")

    with pytest.raises(DDError) as err:
        stage_inputs._stage_input_asset(
            data_layer=data_layer,
            stage="draft",
            gen_id="D2",
            content_combinations=[],
        )

    assert err.value.code is Err.DATA_MISSING
    assert err.value.ctx.get("reason") == "combo_not_found"


def test_stage_input_reuse_missing_prompt_errors(tmp_path: Path) -> None:
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        "draft",
        "D3",
        {"template_id": "draft-tpl", "mode": "llm", "combo_id": "c1"},
    )

    with pytest.raises(DDError) as err:
        stage_inputs._stage_input_asset(
            data_layer=data_layer,
            stage="draft",
            gen_id="D3",
            reuse_existing=True,
        )

    assert err.value.code is Err.DATA_MISSING
    assert err.value.ctx.get("reason") == "input_missing_for_reuse"


@pytest.mark.parametrize(
    "stage, gen_id, template_id, metadata_extras",
    [
        (
            "draft",
            "D_missing",
            "draft-missing",
            {"mode": "llm", "combo_id": "C1"},
        ),
        (
            "essay",
            "E_missing",
            "essay-missing",
            {"mode": "llm", "parent_gen_id": "D_parent"},
        ),
        (
            "evaluation",
            "V_missing",
            "eval-missing",
            {"mode": "llm", "parent_gen_id": "E_parent"},
        ),
    ],
)
def test_stage_input_missing_template(
    tmp_path: Path,
    stage: str,
    gen_id: str,
    template_id: str,
    metadata_extras: dict[str, str],
) -> None:
    paths = Paths.from_str(tmp_path)
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.write_main_metadata(
        stage,
        gen_id,
        {"template_id": template_id, **metadata_extras},
    )

    combos = None
    if stage == "draft":
        combos = [types.SimpleNamespace(combo_id="C1", contents=["idea"])]
    if stage in {"essay", "evaluation"}:
        parent_stage = stage_inputs.effective_parent_stage(stage)
        parent_id = metadata_extras.get("parent_gen_id")
        assert parent_id is not None
        parent_dir = paths.generation_dir(parent_stage, parent_id)
        parent_dir.mkdir(parents=True, exist_ok=True)
        (parent_dir / "parsed.txt").write_text("Parent text", encoding="utf-8")

    kwargs = {}
    if combos is not None:
        kwargs["content_combinations"] = combos

    with pytest.raises(DDError) as err:
        stage_inputs._stage_input_asset(
            data_layer=data_layer,
            stage=stage,
            gen_id=gen_id,
            **kwargs,
        )

    assert err.value.code is Err.MISSING_TEMPLATE
    assert err.value.ctx.get("template_id") == template_id
    assert err.value.ctx.get("path") == str(paths.template_file(stage, template_id))
