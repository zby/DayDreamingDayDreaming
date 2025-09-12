from __future__ import annotations

from pathlib import Path
from typing import Optional
import json
from ..constants import FILE_RAW, FILE_PARSED, FILE_PROMPT, FILE_METADATA


def _gen_dir(gens_root: Path, stage: str, gen_id: str) -> Path:
    # Flat layout: stage/gen_id
    return Path(gens_root) / stage / gen_id


def get_row_by_gen_id(gens_root: Path, stage: str, gen_id: str) -> Optional[dict]:
    """Return a minimal row dict for a given stage/gen_id if files exist.

    The row contains keys: gen_id, stage, gen_dir (absolute path as string).
    Returns None if the directory doesn't exist.
    """
    base = _gen_dir(Path(gens_root), str(stage), str(gen_id))
    if not base.exists():
        return None
    return {"gen_id": str(gen_id), "stage": str(stage), "gen_dir": str(base)}


def _read_text(base: Path, name: str) -> str:
    fp = base / name
    return fp.read_text(encoding="utf-8")


def read_raw(row: dict) -> str:
    base = Path(row.get("gen_dir") or row.get("doc_dir"))
    return _read_text(base, FILE_RAW)


def read_parsed(row: dict) -> str:
    base = Path(row.get("gen_dir") or row.get("doc_dir"))
    return _read_text(base, FILE_PARSED)


def read_prompt(row: dict) -> str:
    base = Path(row.get("gen_dir") or row.get("doc_dir"))
    return _read_text(base, FILE_PROMPT)


def read_metadata(row: dict, *, strict: bool = False) -> dict:
    """Load metadata.json for a given gens-store row.

    Returns an empty dict when metadata.json is missing or invalid, unless strict=True
    in which case it raises the underlying exception.
    """
    base = Path(row.get("gen_dir") or row.get("doc_dir")) if isinstance(row, dict) else Path(str(row))
    fp = base / FILE_METADATA
    try:
        if not fp.exists():
            return {}
        return json.loads(fp.read_text(encoding="utf-8")) or {}
    except Exception:
        if strict:
            raise
        return {}
