from __future__ import annotations

import json
from pathlib import Path

from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.utils.metadata_splitter import (
    MetadataSplit,
    migrate_generation_metadata,
    split_legacy_metadata,
)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_split_llm_metadata(tmp_path: Path) -> None:
    paths = Paths.from_str(tmp_path)
    stage = "draft"
    gen_id = "G1"

    gen_dir = paths.generation_dir(stage, gen_id)
    gen_dir.mkdir(parents=True)
    raw_path = paths.raw_path(stage, gen_id)
    raw_path.write_text("Raw text", encoding="utf-8")
    parsed_path = paths.parsed_path(stage, gen_id)
    parsed_path.write_text("Parsed text", encoding="utf-8")

    legacy = {
        "stage": stage,
        "gen_id": gen_id,
        "template_id": "draft-template",
        "llm_model_id": "model-x",
        "parent_gen_id": "nan",
        "parser_name": "nan",
        "mode": "llm",
        "files": {"raw": str(raw_path.resolve()), "parsed": str(parsed_path.resolve())},
        "finish_reason": "stop",
        "truncated": False,
        "usage": {"total_tokens": 123},
        "duration_s": 42.0,
        "function": "draft_response",
        "run_id": "RUN123",
        "cohort_id": "lost-drafts",
        "combo_id": "combo_v1_123",
    }

    result = split_legacy_metadata(stage=stage, gen_id=gen_id, legacy=legacy, paths=paths)
    assert isinstance(result, MetadataSplit)
    assert result.main["template_id"] == "draft-template"
    assert result.main["mode"] == "llm"
    assert result.main["combo_id"] == "combo_v1_123"
    assert result.raw["function"] == "draft_raw"
    assert result.raw["legacy_function"] == "draft_response"
    assert result.raw["run_id"] == "RUN123"
    assert result.raw["usage"]["total_tokens"] == 123
    assert result.raw["raw_path"].endswith("raw.txt")
    assert result.parsed["function"] == "draft_parsed"
    assert result.parsed["parser_name"] == "legacy"
    assert result.parsed["success"] is True
    assert result.parsed["parsed_path"].endswith("parsed.txt")
    assert result.parsed["combo_id"] == "combo_v1_123"


def test_split_minimal_copy_metadata(tmp_path: Path) -> None:
    paths = Paths.from_str(tmp_path)
    stage = "essay"
    gen_id = "E1"
    gen_dir = paths.generation_dir(stage, gen_id)
    gen_dir.mkdir(parents=True)

    legacy = {
        "template_id": "essay-template",
        "model_id": "model-copy",
        "source_file": "data/legacy/essay.txt",
        "combo_id": "combo_v1_abc",
    }

    result = split_legacy_metadata(stage=stage, gen_id=gen_id, legacy=legacy, paths=paths)
    assert result.main["mode"] == "copy"
    assert result.main["template_id"] == "essay-template"
    assert result.raw["mode"] == "copy"
    assert result.raw["input_mode"] == "copy"
    assert result.raw["copied_from"] == "data/legacy/essay.txt"
    assert result.raw["function"] == "essay_raw"
    assert result.parsed["function"] == "essay_parsed"
    assert result.parsed["parser_name"] == "legacy"
    assert result.parsed["success"] is False


def test_migrate_generation_metadata_writes_files(tmp_path: Path) -> None:
    data_root = tmp_path
    paths = Paths.from_str(data_root)
    stage = "evaluation"
    gen_id = "EV1"
    gen_dir = paths.generation_dir(stage, gen_id)
    gen_dir.mkdir(parents=True)
    paths.raw_path(stage, gen_id).write_text("Raw", encoding="utf-8")
    paths.parsed_path(stage, gen_id).write_text("Parsed", encoding="utf-8")

    legacy_meta = {
        "template_id": "eval-template",
        "llm_model_id": "eval-model",
        "parent_gen_id": "essay-123",
        "parser_name": "score_parser",
        "mode": "llm",
        "files": {"raw": "", "parsed": ""},
        "finish_reason": "stop",
        "truncated": False,
        "usage": {"total_tokens": 77},
        "duration_s": 5.5,
        "function": "evaluation_response",
        "run_id": "RUN999",
        "cohort_id": "cohort-x",
    }
    (gen_dir / "metadata.json").write_text(json.dumps(legacy_meta), encoding="utf-8")

    stats = migrate_generation_metadata(data_root, dry_run=False)
    assert stats["converted"] == 1

    main_payload = _read_json(gen_dir / "metadata.json")
    raw_payload = _read_json(gen_dir / "raw_metadata.json")
    parsed_payload = _read_json(gen_dir / "parsed_metadata.json")

    assert main_payload["template_id"] == "eval-template"
    assert raw_payload["function"] == "evaluation_raw"
    assert raw_payload["usage"]["total_tokens"] == 77
    assert parsed_payload["function"] == "evaluation_parsed"
    assert parsed_payload["parser_name"] == "score_parser"
    assert parsed_payload["success"] is True
