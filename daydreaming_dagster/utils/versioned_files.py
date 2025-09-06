from __future__ import annotations

from pathlib import Path
import os
import re
from typing import Optional


_V_RE = re.compile(r"^(?P<stem>.+)_v(?P<ver>\d+)(?P<ext>\.[^.]+)$")


def _normalize_ext(ext: str) -> str:
    return ext if ext.startswith(".") else f".{ext}"


def latest_versioned_path(dir_path: Path, stem: str, ext: str = ".txt") -> Optional[Path]:
    """Return latest `{stem}_vN{ext}` under dir_path, or None if none exist.

    Does not raise if directory does not exist or is unreadable.
    """
    ext = _normalize_ext(ext)
    try:
        names = os.listdir(dir_path)
    except Exception:
        return None
    best_ver = -1
    best_name = None
    prefix = f"{stem}_v"
    for name in names:
        if not name.startswith(prefix) or not name.endswith(ext):
            continue
        m = _V_RE.match(name)
        if not m or m.group("stem") != stem or m.group("ext") != ext:
            continue
        try:
            ver = int(m.group("ver"))
        except Exception:
            continue
        if ver > best_ver:
            best_ver = ver
            best_name = name
    if best_name is None:
        return None
    p = dir_path / best_name
    return p if p.exists() else None


def next_versioned_path(dir_path: Path, stem: str, ext: str = ".txt") -> Path:
    """Return the next `{stem}_vN{ext}` path (does not create files).

    If directory is missing or no versions exist, returns v1.
    """
    ext = _normalize_ext(ext)
    latest = latest_versioned_path(dir_path, stem, ext)
    if latest is None:
        return dir_path / f"{stem}_v1{ext}"
    # Extract N from latest name
    m = _V_RE.match(latest.name)
    if not m:
        return dir_path / f"{stem}_v1{ext}"
    try:
        ver = int(m.group("ver"))
    except Exception:
        ver = 0
    return dir_path / f"{stem}_v{ver + 1}{ext}"


def save_versioned_text(dir_path: Path, stem: str, text: str, ext: str = ".txt", logger=None) -> Optional[str]:
    """Write text to the next versioned file; return path string or None on failure."""
    try:
        dir_path.mkdir(parents=True, exist_ok=True)
        target = next_versioned_path(dir_path, stem, ext)
        target.write_text(text, encoding="utf-8")
        return str(target)
    except Exception as e:
        if logger is not None:
            try:
                logger.info(f"Failed to save versioned text for {stem}: {e}")
            except Exception:
                pass
        return None

