from __future__ import annotations

from pathlib import Path
from typing import Optional
import json


def _doc_dir(docs_root: Path, stage: str, doc_id: str) -> Path:
    # Flat layout: stage/doc_id
    return Path(docs_root) / stage / doc_id


def get_row_by_doc_id(docs_root: Path, stage: str, doc_id: str) -> Optional[dict]:
    """Return a minimal row dict for a given stage/doc_id if files exist.

    The row contains keys: doc_id, stage, doc_dir (absolute path as string).
    Returns None if the directory doesn't exist.
    """
    base = _doc_dir(Path(docs_root), str(stage), str(doc_id))
    if not base.exists():
        return None
    return {"doc_id": str(doc_id), "stage": str(stage), "doc_dir": str(base)}


def _read_text(base: Path, name: str) -> str:
    fp = base / name
    return fp.read_text(encoding="utf-8")


def read_raw(row: dict) -> str:
    base = Path(row["doc_dir"])
    return _read_text(base, "raw.txt")


def read_parsed(row: dict) -> str:
    base = Path(row["doc_dir"])
    return _read_text(base, "parsed.txt")


def read_prompt(row: dict) -> str:
    base = Path(row["doc_dir"])
    return _read_text(base, "prompt.txt")


def read_metadata(row: dict, *, strict: bool = False) -> dict:
    """Load metadata.json for a given docs-store row.

    Returns an empty dict when metadata.json is missing or invalid, unless strict=True
    in which case it raises the underlying exception.
    """
    base = Path(row["doc_dir"]) if isinstance(row, dict) else Path(str(row))
    fp = base / "metadata.json"
    try:
        if not fp.exists():
            return {}
        return json.loads(fp.read_text(encoding="utf-8")) or {}
    except Exception:
        if strict:
            raise
        return {}
