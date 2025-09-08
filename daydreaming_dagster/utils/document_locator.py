from __future__ import annotations

from pathlib import Path
from typing import Tuple
from .versioned_files import latest_versioned_path


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
    # Prefer versioned file in essay/draft directories
    essay_dir = base / "3_generation" / "essay_responses"
    draft_dir = base / "3_generation" / "draft_responses"
    latest_essay = latest_versioned_path(essay_dir, document_id, ".txt")
    if latest_essay is not None:
        return latest_essay, "essay_responses"
    latest_draft = latest_versioned_path(draft_dir, document_id, ".txt")
    if latest_draft is not None:
        return latest_draft, "draft_responses"

    # BACKCOMPAT(PATHS): Fall back to unversioned files across known legacy locations.
    # Prefer maintaining canonical essay/draft_responses with versioned files; these
    # legacy paths remain for historical analyses and should not expand.
    candidates = [
        (essay_dir / f"{document_id}.txt", "essay_responses"),
        (draft_dir / f"{document_id}.txt", "draft_responses"),
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
