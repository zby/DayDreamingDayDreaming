from pathlib import Path
import json

from daydreaming_dagster.utils.filesystem_rows import get_row_by_doc_id, read_metadata


def test_read_metadata_present(tmp_path: Path):
    # Arrange: create docs_root/stage/doc_id/ with metadata.json
    docs_root = tmp_path / "docs"
    stage = "draft"
    doc_id = "abc123"
    base = docs_root / stage / doc_id
    base.mkdir(parents=True)
    meta = {"task_id": "t1", "template_id": "tplX", "model_id": "mA"}
    (base / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")

    # Act
    row = get_row_by_doc_id(docs_root, stage, doc_id)
    assert row is not None
    out = read_metadata(row)

    # Assert
    assert isinstance(out, dict)
    assert out.get("task_id") == "t1"
    assert out.get("template_id") == "tplX"
    assert out.get("model_id") == "mA"


def test_read_metadata_missing_returns_empty(tmp_path: Path):
    docs_root = tmp_path / "docs"
    stage = "essay"
    doc_id = "missingmeta"
    base = docs_root / stage / doc_id
    base.mkdir(parents=True)
    row = get_row_by_doc_id(docs_root, stage, doc_id)
    assert row is not None
    out = read_metadata(row)
    assert out == {}


def test_read_metadata_invalid_json_non_strict(tmp_path: Path):
    docs_root = tmp_path / "docs"
    stage = "evaluation"
    doc_id = "badjson"
    base = docs_root / stage / doc_id
    base.mkdir(parents=True)
    (base / "metadata.json").write_text("{not-json}", encoding="utf-8")
    row = get_row_by_doc_id(docs_root, stage, doc_id)
    assert row is not None
    out = read_metadata(row)  # non-strict returns {}
    assert out == {}


def test_read_metadata_invalid_json_strict_raises(tmp_path: Path):
    docs_root = tmp_path / "docs"
    stage = "evaluation"
    doc_id = "badjson2"
    base = docs_root / stage / doc_id
    base.mkdir(parents=True)
    (base / "metadata.json").write_text("{not-json}", encoding="utf-8")
    row = get_row_by_doc_id(docs_root, stage, doc_id)
    assert row is not None
    raised = False
    try:
        read_metadata(row, strict=True)
    except Exception:
        raised = True
    assert raised is True
