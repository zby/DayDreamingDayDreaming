from __future__ import annotations

from pathlib import Path
from typing import Tuple


def find_document_path(document_id: str, data_root: Path) -> Tuple[Path | None, str]:
    """
    Locate a generation document by ID across current and legacy locations.

    Order of search (first hit wins):
    1) data/3_generation/essay_responses/{document_id}.txt
    2) data/3_generation/draft_responses/{document_id}.txt
    3) data/3_generation/links_responses/{document_id}.txt  (legacy)
    4) data/3_generation/generation_responses/{document_id}.txt  (legacy one-phase)
    5) data/3_generation/parsed_generation_responses/{document_id}.txt  (legacy)

    Returns (Path or None, label) where label is the directory name used.
    """
    base = Path(data_root)
    candidates = [
        (base / "3_generation" / "essay_responses" / f"{document_id}.txt", "essay_responses"),
        (base / "3_generation" / "draft_responses" / f"{document_id}.txt", "draft_responses"),
        (base / "3_generation" / "links_responses" / f"{document_id}.txt", "links_responses"),
        (base / "3_generation" / "generation_responses" / f"{document_id}.txt", "generation_responses"),
        (base / "3_generation" / "parsed_generation_responses" / f"{document_id}.txt", "parsed_generation_responses"),
    ]
    for path, label in candidates:
        try:
            if path.exists():
                return path, label
        except Exception:
            continue
    return None, ""

