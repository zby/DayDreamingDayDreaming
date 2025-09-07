from __future__ import annotations

from pathlib import Path
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex, DocumentRow


class TestDocumentsIndex:
    def test_insert_and_latest_select(self, tmp_path: Path):
        db = tmp_path / "db.sqlite"
        docs_root = tmp_path / "docs"
        idx = SQLiteDocumentsIndex(db, docs_root)
        idx.init_maybe_create_tables()

        # Insert two attempts for same logical/task; second should be selected as latest
        r1 = DocumentRow(
            doc_id="docA",
            logical_key_id="logical1",
            stage="draft",
            task_id="task1",
            doc_dir=str(Path("draft") / "docA"),
            status="ok",
        )
        idx.insert_document(r1)

        r2 = DocumentRow(
            doc_id="docB",
            logical_key_id="logical1",
            stage="draft",
            task_id="task1",
            doc_dir=str(Path("draft") / "docB"),
            status="ok",
        )
        idx.insert_document(r2)

        got_task = idx.get_latest_by_task("draft", "task1")
        assert got_task is not None
        assert got_task["doc_id"] == "docB"

        got_logical = idx.get_latest_by_logical("logical1")
        assert got_logical is not None
        assert got_logical["doc_id"] == "docB"
