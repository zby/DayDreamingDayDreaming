from __future__ import annotations

from pathlib import Path
import json
from typing import Optional, Dict, Any

from .errors import DDError, Err
from .ids import gen_dir as build_gen_dir
from ..data_layer.paths import RAW_FILENAME, PARSED_FILENAME, PROMPT_FILENAME, METADATA_FILENAME


def _write_atomic(path: Path, data: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(data, encoding="utf-8")
        tmp.replace(path)
    except OSError as exc:  # pragma: no cover - filesystem errors rare in tests
        raise DDError(Err.IO_ERROR, ctx={"path": str(path)}) from exc

def _ensure_dir(gens_root: Path, stage: str, gen_id: str) -> Path:
    base = build_gen_dir(Path(gens_root), stage, gen_id)
    base.mkdir(parents=True, exist_ok=True)
    return base


def write_gen_raw(gens_root: Path, stage: str, gen_id: str, text: str) -> Path:
    """Write raw.txt for a generation, creating the directory if needed."""
    base = _ensure_dir(gens_root, stage, gen_id)
    _write_atomic(base / RAW_FILENAME, str(text or ""))
    return base


def write_gen_parsed(gens_root: Path, stage: str, gen_id: str, text: str) -> Path:
    """Write parsed.txt for a generation, creating the directory if needed."""
    base = _ensure_dir(gens_root, stage, gen_id)
    _write_atomic(base / PARSED_FILENAME, str(text))
    return base


def write_gen_prompt(gens_root: Path, stage: str, gen_id: str, text: str) -> Path:
    """Write prompt.txt for a generation, creating the directory if needed."""
    base = _ensure_dir(gens_root, stage, gen_id)
    _write_atomic(base / PROMPT_FILENAME, str(text))
    return base


def write_gen_metadata(gens_root: Path, stage: str, gen_id: str, metadata: Dict[str, Any]) -> Path:
    """Write metadata.json for a generation, creating the directory if needed."""
    base = _ensure_dir(gens_root, stage, gen_id)
    _write_atomic(base / METADATA_FILENAME, json.dumps(metadata, ensure_ascii=False, indent=2))
    return base


def load_generation(gens_root: Path, stage: str, gen_id: str) -> Dict[str, Any]:
    """Best-effort read of an existing generation from disk into a dict.

    Keys: stage, gen_id, parent_gen_id (from metadata), raw_text, parsed_text, prompt_text, metadata
    """
    base = build_gen_dir(Path(gens_root), stage, gen_id)
    def _read(name: str) -> Optional[str]:
        p = base / name
        if not p.exists():
            return None
        try:
            return p.read_text(encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(p)}) from exc
    md: Optional[Dict[str, Any]] = None
    mpath = base / METADATA_FILENAME
    if mpath.exists():
        try:
            md = json.loads(mpath.read_text(encoding="utf-8"))
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(mpath)}) from exc
        except json.JSONDecodeError as exc:
            raise DDError(Err.PARSER_FAILURE, ctx={"path": str(mpath)}) from exc
    return {
        "stage": str(stage),
        "gen_id": str(gen_id),
        "parent_gen_id": (md or {}).get("parent_gen_id"),
        "raw_text": _read(RAW_FILENAME) or "",
        "parsed_text": _read(PARSED_FILENAME),
        "prompt_text": _read(PROMPT_FILENAME),
        "metadata": md,
    }
