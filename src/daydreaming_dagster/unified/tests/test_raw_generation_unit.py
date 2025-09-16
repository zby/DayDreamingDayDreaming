from __future__ import annotations

from pathlib import Path
import json

from daydreaming_dagster.unified.raw_generation import (
    RawGenerationResult,
    perform_llm_raw_generation,
    perform_copy_raw_generation,
)


class _StubLLM:
    def __init__(self, text: str, *, finish_reason: str = "stop", truncated: bool = False, usage: dict | None = None):
        self._text = text
        self._info = {
            "finish_reason": finish_reason,
            "truncated": truncated,
            "usage": usage or {"total_tokens": 42},
        }

    def generate_with_info(self, prompt: str, *, model: str, max_tokens=None):
        return self._text.replace("${PROMPT}", prompt), dict(self._info)


def test_perform_llm_raw_generation(tmp_path: Path):
    llm = _StubLLM("hello world")
    data_root = tmp_path

    result = perform_llm_raw_generation(
        stage="draft",
        llm_client=llm,
        data_root=data_root,
        gen_id="G1",
        template_id="tpl",
        prompt_text="Prompt",
        llm_model_id="model-x",
        max_tokens=128,
        metadata_extras={"run_id": "RUN123"},
    )

    assert isinstance(result, RawGenerationResult)
    assert result.raw_path.read_text(encoding="utf-8") == "hello world"

    raw_meta_path = (tmp_path / "gens" / "draft" / "G1" / "raw_metadata.json")
    assert raw_meta_path.exists()
    raw_meta = json.loads(raw_meta_path.read_text(encoding="utf-8"))
    assert raw_meta["mode"] == "llm"
    assert raw_meta["llm_model_id"] == "model-x"
    assert raw_meta["usage"]["total_tokens"] == 42


def test_perform_copy_raw_generation(tmp_path: Path):
    data_root = tmp_path
    source_dir = tmp_path / "gens" / "essay" / "E1"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "parsed.txt"
    source_file.write_text("Essay text", encoding="utf-8")

    result = perform_copy_raw_generation(
        stage="evaluation",
        data_root=data_root,
        gen_id="EV1",
        source_stage="essay",
        source_gen_id="E1",
        metadata_extras={"cohort_id": "C1"},
    )

    assert result.raw_path.read_text(encoding="utf-8") == "Essay text"
    raw_meta_path = tmp_path / "gens" / "evaluation" / "EV1" / "raw_metadata.json"
    raw_meta = json.loads(raw_meta_path.read_text(encoding="utf-8"))
    assert raw_meta["mode"] == "copy"
    assert raw_meta["copied_from"].endswith("parsed.txt")
    assert raw_meta["cohort_id"] == "C1"
