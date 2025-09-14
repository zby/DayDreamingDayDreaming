from __future__ import annotations

from pathlib import Path
import json
from typing import Optional, Dict, Any
from .ids import gen_dir as build_gen_dir
from ..constants import FILE_RAW, FILE_PARSED, FILE_PROMPT, FILE_METADATA


def _write_atomic(path: Path, data: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)

def write_generation_files(
    *,
    gens_root: Path,
    stage: str,
    gen_id: str,
    parent_gen_id: Optional[str],
    raw_text: str,
    parsed_text: Optional[str],
    prompt_text: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
    write_raw: bool = True,
    write_parsed: bool = True,
    write_prompt: bool = True,
    write_metadata: bool = True,
) -> Path:
    """Write generation files with atomic replace semantics.

    Returns the target directory path.
    """
    base = build_gen_dir(Path(gens_root), stage, gen_id)
    base.mkdir(parents=True, exist_ok=True)
    if write_raw:
        _write_atomic(base / FILE_RAW, str(raw_text or ""))
    if write_parsed and isinstance(parsed_text, str):
        _write_atomic(base / FILE_PARSED, parsed_text)
    if write_prompt and isinstance(prompt_text, str):
        _write_atomic(base / FILE_PROMPT, prompt_text)
    if write_metadata and isinstance(metadata, dict):
        _write_atomic(base / FILE_METADATA, json.dumps(metadata, ensure_ascii=False, indent=2))
    return base


def load_generation(gens_root: Path, stage: str, gen_id: str) -> Dict[str, Any]:
    """Best-effort read of an existing generation from disk into a dict.

    Keys: stage, gen_id, parent_gen_id (from metadata), raw_text, parsed_text, prompt_text, metadata
    """
    base = build_gen_dir(Path(gens_root), stage, gen_id)
    def _read(name: str) -> Optional[str]:
        p = base / name
        try:
            return p.read_text(encoding="utf-8") if p.exists() else None
        except Exception:
            return None
    md: Optional[Dict[str, Any]] = None
    mpath = base / FILE_METADATA
    if mpath.exists():
        try:
            md = json.loads(mpath.read_text(encoding="utf-8"))
        except Exception:
            md = None
    return {
        "stage": str(stage),
        "gen_id": str(gen_id),
        "parent_gen_id": (md or {}).get("parent_gen_id"),
        "raw_text": _read(FILE_RAW) or "",
        "parsed_text": _read(FILE_PARSED),
        "prompt_text": _read(FILE_PROMPT),
        "metadata": md,
    }
