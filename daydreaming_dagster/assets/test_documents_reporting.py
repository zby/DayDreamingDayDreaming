from __future__ import annotations

from pathlib import Path
import os
from dagster import materialize, DagsterInstance
from unittest.mock import patch

from daydreaming_dagster.assets.documents_reporting import documents_latest_report, documents_consistency_report
from daydreaming_dagster.resources.io_managers import CSVIOManager
from daydreaming_dagster.resources.documents_index import DocumentsIndexResource
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex, DocumentRow


def test_documents_latest_report_smoke(tmp_path: Path):
    # Prepare temp data-root and docs/db
    data_root = tmp_path
    db_path = data_root / "db" / "documents.sqlite"
    docs_root = data_root / "docs"
    idx = SQLiteDocumentsIndex(db_path, docs_root)
    idx.init_maybe_create_tables()
    # Insert a minimal draft row
    docs_root.mkdir(parents=True, exist_ok=True)
    (docs_root / "draft" / "docX").mkdir(parents=True)
    idx.insert_document(
        DocumentRow(
            doc_id="docX",
            logical_key_id="logicalX",
            stage="draft",
            task_id="taskX",
            doc_dir=str(Path("draft") / "docX"),
            status="ok",
        )
    )

    # Resources
    resources = {
        "data_root": str(data_root),
        "error_log_io_manager": CSVIOManager(base_path=data_root / "7_reporting"),
        "documents_index": DocumentsIndexResource(db_path=str(db_path), docs_root=str(docs_root)),
    }

    with patch.dict(os.environ, {"DD_DOCS_INDEX_ENABLED": "1"}):
        with DagsterInstance.ephemeral() as instance:
            result = materialize([documents_latest_report, documents_consistency_report], resources=resources, instance=instance)
            assert result.success
    # Verify CSV written and non-empty
    out_csv = data_root / "7_reporting" / "documents_latest_report.csv"
    assert out_csv.exists(), "documents_latest_report.csv not created"
    assert out_csv.stat().st_size > 0, "documents_latest_report.csv is empty"
    out_csv2 = data_root / "7_reporting" / "documents_consistency_report.csv"
    assert out_csv2.exists(), "documents_consistency_report.csv not created"
