from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
from .ids import doc_dir as build_doc_dir


def _write_atomic(path: Path, data: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


@dataclass
class Document:
    """Domain object representing a generated document artifact.

    This class encapsulates filesystem writes for raw/parsed/prompt/metadata and
    conversion into a DocumentRow for the SQLite index. It does not depend on Dagster.
    """

    stage: str
    doc_id: str
    parent_doc_id: str | None
    raw_text: str
    parsed_text: str
    prompt_text: str | None = None
    metadata: dict | None = None

    def target_dir(self, docs_root: Path) -> Path:
        """Resolve the canonical doc directory from docs_root using repo helper."""
        return build_doc_dir(Path(docs_root), self.stage, self.doc_id)

    def write_files(self, docs_root: Path) -> Path:
        """Write raw.txt, parsed.txt, optional prompt.txt and metadata.json.

        Returns the target directory path.
        """
        base = self.target_dir(docs_root)
        base.mkdir(parents=True, exist_ok=True)
        _write_atomic(base / "raw.txt", self.raw_text)
        _write_atomic(base / "parsed.txt", self.parsed_text)
        if isinstance(self.prompt_text, str):
            _write_atomic(base / "prompt.txt", self.prompt_text)
        if isinstance(self.metadata, dict):
            _write_atomic(base / "metadata.json", json.dumps(self.metadata, ensure_ascii=False, indent=2))
        return base

    # Index row conversion removed in filesystem-only mode
