from __future__ import annotations

import json
import types
from pathlib import Path
from typing import Any, Dict

import daydreaming_dagster.unified.stage_raw as stage_raw
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.resources.experiment_config import ExperimentConfig, StageSettings


def _context(tmp_path: Path, partition_key: str, *, llm: Any | None = None):
    captured: Dict[str, Any] = {}

    def _capture(md):
        captured["metadata"] = md

    experiment_config = ExperimentConfig(
        stage_config={
            "draft": StageSettings(generation_max_tokens=256, min_lines=2),
            "essay": StageSettings(generation_max_tokens=512),
            "evaluation": StageSettings(generation_max_tokens=128),
        }
    )

    resources = {
        "data_root": str(tmp_path),
        "experiment_config": experiment_config,
    }
    if llm is not None:
        resources["openrouter_client"] = llm

    return types.SimpleNamespace(
        partition_key=partition_key,
        resources=types.SimpleNamespace(**resources),
        run=types.SimpleNamespace(run_id="RUN-123"),
        add_output_metadata=_capture,
        _captured=captured,
    )


class _StubLLM:
    def __init__(self, *, text: str, finish_reason: str = "stop", truncated: bool = False, usage: dict | None = None):
        self._text = text
        self._info = {
            "finish_reason": finish_reason,
            "truncated": truncated,
            "usage": usage or {"total_tokens": 42},
        }
        self.calls: list[dict[str, Any]] = []

    def generate_with_info(self, prompt: str, *, model: str, max_tokens=None):
        self.calls.append({"prompt": prompt, "model": model, "max_tokens": max_tokens})
        return self._text, dict(self._info)


def _prepare_generation(tmp_path: Path, stage: str, gen_id: str, metadata: Dict[str, Any]) -> Paths:
    paths = Paths.from_str(tmp_path)
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.reserve_generation(stage, gen_id)
    data_layer.write_main_metadata(stage, gen_id, metadata)
    return paths


def _assert_subset(actual: Dict[str, Any], expected: Dict[str, Any]) -> None:
    for key, value in expected.items():
        assert actual.get(key) == value


def test_stage_raw_llm_persists_expected_files(tmp_path: Path):
    paths = _prepare_generation(
        tmp_path,
        "draft",
        "D1",
        {
            "template_id": "draft-tpl",
            "mode": "llm",
            "llm_model_id": "model-x",
            "origin_cohort_id": "C1",
            "combo_id": "combo-1",
            "replicate": 3,
        },
    )

    llm = _StubLLM(text="RAW-OUT")
    ctx = _context(tmp_path, "D1", llm=llm)

    out = stage_raw.stage_raw_asset(ctx, "draft", prompt_text="PROMPT TEXT")

    assert out == "RAW-OUT"
    assert llm.calls == [
        {"prompt": "PROMPT TEXT", "model": "model-x", "max_tokens": 256}
    ]

    raw_path = paths.raw_path("draft", "D1")
    assert raw_path.read_text(encoding="utf-8") == "RAW-OUT"

    raw_meta = json.loads(paths.raw_metadata_path("draft", "D1").read_text(encoding="utf-8"))
    _assert_subset(
        raw_meta,
        {
            "mode": "llm",
            "llm_model_id": "model-x",
            "function": "draft_raw",
            "run_id": "RUN-123",
            "input_mode": "prompt",
            "origin_cohort_id": "C1",
            "combo_id": "combo-1",
            "replicate": 3,
            "raw_path": str(raw_path),
            "raw_metadata_path": str(paths.raw_metadata_path("draft", "D1")),
        },
    )

    md = ctx._captured["metadata"]
    _assert_subset(
        {
            "function": md["function"].value,
            "gen_id": md["gen_id"].value,
            "mode": md["mode"].value,
            "raw_path": md["raw_path"].value,
        },
        {
            "function": "draft_raw",
            "gen_id": "D1",
            "mode": "llm",
            "raw_path": str(raw_path),
        },
    )
    assert md["raw_metadata"].value == raw_meta


def test_stage_raw_copy_marks_input_mode(tmp_path: Path):
    paths = _prepare_generation(
        tmp_path,
        "essay",
        "E1",
        {
            "template_id": "essay-tpl",
            "mode": "copy",
            "parent_gen_id": "D5",
            "origin_cohort_id": "C9",
        },
    )

    parent_dir = paths.generation_dir("draft", "D5")
    parent_dir.mkdir(parents=True, exist_ok=True)
    (parent_dir / "parsed.txt").write_text("Parent text", encoding="utf-8")

    ctx = _context(tmp_path, "E1")

    out = stage_raw.stage_raw_asset(ctx, "essay", prompt_text="Parent text")

    assert out == "Parent text"

    raw_path = paths.raw_path("essay", "E1")
    assert raw_path.read_text(encoding="utf-8") == "Parent text"

    raw_meta = json.loads(paths.raw_metadata_path("essay", "E1").read_text(encoding="utf-8"))
    _assert_subset(
        raw_meta,
        {
            "mode": "copy",
            "input_mode": "copy",
            "origin_cohort_id": "C9",
            "raw_path": str(raw_path),
            "raw_metadata_path": str(paths.raw_metadata_path("essay", "E1")),
        },
    )

    md = ctx._captured["metadata"]
    _assert_subset(
        {
            "function": md["function"].value,
            "mode": md["mode"].value,
            "raw_path": md["raw_path"].value,
        },
        {
            "function": "essay_raw",
            "mode": "copy",
            "raw_path": str(raw_path),
        },
    )
