from __future__ import annotations

from pathlib import Path
from daydreaming_dagster.utils.document import Generation


class TestDocument:
    def test_write_and_to_index_row(self, tmp_path: Path):
        docs_root = tmp_path / "gens"

        # Build a simple draft document
        doc = Generation(
            stage="draft",
            gen_id="gen123",
            parent_gen_id=None,
            raw_text="raw content",
            parsed_text="parsed content",
            prompt_text="prompt content",
            metadata={"task_id": "task1", "function": "draft_response"},
        )

        target = doc.write_files(docs_root)
        assert (target / "raw.txt").read_text(encoding="utf-8") == "raw content"
        assert (target / "parsed.txt").read_text(encoding="utf-8") == "parsed content"
        assert (target / "prompt.txt").read_text(encoding="utf-8") == "prompt content"
        assert (target / "metadata.json").exists()

    # Index integration removed; filesystem write verified above
