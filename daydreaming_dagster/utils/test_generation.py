from __future__ import annotations

from pathlib import Path
from daydreaming_dagster.utils.generation import write_gen_raw, write_gen_parsed, write_gen_prompt, write_gen_metadata


class TestGeneration:
    def test_write_and_to_index_row(self, tmp_path: Path):
        docs_root = tmp_path / "gens"

        # Write a simple draft document
        target = write_gen_raw(docs_root, "draft", "gen123", "raw content")
        write_gen_parsed(docs_root, "draft", "gen123", "parsed content")
        write_gen_prompt(docs_root, "draft", "gen123", "prompt content")
        write_gen_metadata(docs_root, "draft", "gen123", {"task_id": "task1", "function": "draft_response"})
        assert (target / "raw.txt").read_text(encoding="utf-8") == "raw content"
        assert (target / "parsed.txt").read_text(encoding="utf-8") == "parsed content"
        assert (target / "prompt.txt").read_text(encoding="utf-8") == "prompt content"
        assert (target / "metadata.json").exists()

    # Index integration removed; filesystem write verified above
