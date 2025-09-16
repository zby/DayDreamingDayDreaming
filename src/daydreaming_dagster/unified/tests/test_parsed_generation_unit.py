from __future__ import annotations

import json
from pathlib import Path

from daydreaming_dagster.unified.parsed_generation import (
    ParsedGenerationResult,
    perform_parsed_generation,
)


def test_perform_parsed_generation_success(tmp_path: Path):
    raw_metadata = {"input_mode": "prompt", "truncated": False}
    result = perform_parsed_generation(
        stage="draft",
        gen_id="D1",
        data_root=tmp_path,
        raw_text="Line 1\nLine 2",
        raw_metadata=raw_metadata,
        parser_name="identity",
        min_lines=1,
        fail_on_truncation=True,
        metadata_extras={"function": "draft_parsed", "run_id": "RUN123"},
    )

    assert isinstance(result, ParsedGenerationResult)
    parsed_path = tmp_path / "gens" / "draft" / "D1" / "parsed.txt"
    assert parsed_path.read_text(encoding="utf-8") == "Line 1\nLine 2"
    parsed_meta_path = tmp_path / "gens" / "draft" / "D1" / "parsed_metadata.json"
    parsed_meta = json.loads(parsed_meta_path.read_text(encoding="utf-8"))
    assert parsed_meta["success"] is True
    assert parsed_meta["function"] == "draft_parsed"


def test_perform_parsed_generation_truncation_failure(tmp_path: Path):
    raw_metadata = {"truncated": True}
    try:
        perform_parsed_generation(
            stage="draft",
            gen_id="D1",
            data_root=tmp_path,
            raw_text="Line",
            raw_metadata=raw_metadata,
            parser_name="identity",
            min_lines=1,
            fail_on_truncation=True,
        )
    except ValueError as exc:
        assert "truncated" in str(exc)
        parsed_path = tmp_path / "gens" / "draft" / "D1" / "parsed.txt"
        parsed_meta_path = tmp_path / "gens" / "draft" / "D1" / "parsed_metadata.json"
        assert not parsed_path.exists()
        assert not parsed_meta_path.exists()
    else:
        raise AssertionError("Expected truncation ValueError")
