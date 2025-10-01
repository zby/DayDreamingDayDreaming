from __future__ import annotations

from pathlib import Path

from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.utils.generation import (
    load_generation,
    write_gen_metadata,
    write_gen_parsed,
    write_gen_prompt,
    write_gen_raw,
)


class TestGenerationShim:
    def test_write_helpers_delegate_to_data_layer(self, tmp_path: Path) -> None:
        gens_root = tmp_path / "gens"

        base = write_gen_raw(gens_root, "draft", "gen123", "raw content")
        write_gen_parsed(gens_root, "draft", "gen123", "parsed content")
        write_gen_prompt(gens_root, "draft", "gen123", "prompt content")
        write_gen_metadata(
            gens_root,
            "draft",
            "gen123",
            {"task_id": "task1", "function": "draft_response"},
        )

        expected_dir = tmp_path / "gens" / "draft" / "gen123"
        assert base == expected_dir
        assert (expected_dir / "raw.txt").read_text(encoding="utf-8") == "raw content"
        assert (expected_dir / "parsed.txt").read_text(encoding="utf-8") == "parsed content"
        assert (expected_dir / "prompt.txt").read_text(encoding="utf-8") == "prompt content"

        layer = GensDataLayer.from_root(tmp_path)
        metadata = layer.read_main_metadata("draft", "gen123")
        assert metadata["task_id"] == "task1"
        assert metadata["function"] == "draft_response"

    def test_load_generation_collects_available_artifacts(self, tmp_path: Path) -> None:
        gens_root = tmp_path / "gens"

        write_gen_raw(gens_root, "essay", "ess999", "essay raw")
        write_gen_metadata(
            gens_root,
            "essay",
            "ess999",
            {
                "parent_gen_id": "draft-1",
                "template_id": "essay-template",
                "mode": "llm",
            },
        )

        record = load_generation(gens_root, "essay", "ess999")

        assert record["stage"] == "essay"
        assert record["gen_id"] == "ess999"
        assert record["raw_text"] == "essay raw"
        assert record["parsed_text"] is None
        assert record["prompt_text"] is None
        assert record["metadata"]["template_id"] == "essay-template"
        assert record["parent_gen_id"] == "draft-1"

    def test_load_generation_handles_missing_metadata(self, tmp_path: Path) -> None:
        gens_root = tmp_path / "gens"

        write_gen_prompt(gens_root, "evaluation", "eval-002", "prompt")

        record = load_generation(gens_root, "evaluation", "eval-002")

        assert record["metadata"] is None
        assert record["raw_text"] == ""
        assert record["prompt_text"] == "prompt"
        assert record["parsed_text"] is None
