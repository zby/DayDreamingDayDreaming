from __future__ import annotations

from pathlib import Path
import os
from dagster import materialize, DagsterInstance
from unittest.mock import patch

from daydreaming_dagster.assets.documents_reporting import documents_latest_report, documents_consistency_report
from daydreaming_dagster.resources.io_managers import CSVIOManager


def test_documents_latest_report_smoke(tmp_path: Path):
    # Prepare temp data-root and gens store
    data_root = tmp_path
    gens_root = data_root / "gens" / "draft" / "genX"
    gens_root.mkdir(parents=True, exist_ok=True)
    (gens_root / "parsed.txt").write_text("Hello", encoding="utf-8")
    (gens_root / "metadata.json").write_text('{"task_id":"taskX","created_at":"2024-01-01T00:00:00Z"}', encoding="utf-8")

    # Resources
    resources = {
        "data_root": str(data_root),
        "error_log_io_manager": CSVIOManager(base_path=data_root / "7_reporting"),
    }

    with DagsterInstance.ephemeral() as instance:
        result = materialize([documents_latest_report, documents_consistency_report], resources=resources, instance=instance)
        assert result.success
    # Verify CSV written and non-empty
    out_csv = data_root / "7_reporting" / "documents_latest_report.csv"
    assert out_csv.exists(), "documents_latest_report.csv not created"
    assert out_csv.stat().st_size > 0, "documents_latest_report.csv is empty"
    out_csv2 = data_root / "7_reporting" / "documents_consistency_report.csv"
    assert out_csv2.exists(), "documents_consistency_report.csv not created"
