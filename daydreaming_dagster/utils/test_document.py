from __future__ import annotations

from pathlib import Path
from daydreaming_dagster.utils.document import Document
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex


class TestDocument:
    def test_write_and_to_index_row(self, tmp_path: Path):
        docs_root = tmp_path / "docs"
        db_path = tmp_path / "db.sqlite"
        idx = SQLiteDocumentsIndex(db_path, docs_root)
        idx.init_maybe_create_tables()

        # Build a simple draft document
        doc = Document(
            stage="draft",
            logical_key_id="comboA__tplX__modelY",
            doc_id="doc123",
            parent_doc_id=None,
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

        row = doc.to_index_row(
            docs_root,
            task_id="task1",
            template_id="tplX",
            model_id="modelY",
            run_id="run",
        )
        # insert to ensure the row is acceptable
        idx.insert_document(row)
        got = idx.get_by_doc_id("doc123")
        assert got is not None
        assert got["stage"] == "draft"
        assert got["task_id"] == "task1"
