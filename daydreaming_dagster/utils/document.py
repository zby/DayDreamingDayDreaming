from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
from .documents_index import DocumentRow
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
    logical_key_id: str
    doc_id: str
    parent_doc_id: str | None
    raw_text: str
    parsed_text: str
    prompt_text: str | None = None
    metadata: dict | None = None

    def target_dir(self, docs_root: Path) -> Path:
        """Resolve the canonical doc directory from docs_root using repo helper."""
        return build_doc_dir(Path(docs_root), self.stage, self.logical_key_id, self.doc_id)

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

    def to_index_row(
        self,
        docs_root: Path,
        *,
        task_id: str,
        template_id: str | None,
        model_id: str | None,
        run_id: str | None,
        parser: str | None = None,
        status: str = "ok",
    ) -> DocumentRow:
        base = self.target_dir(docs_root)
        rel_dir = base.relative_to(docs_root)
        content_hash = hashlib.sha256((self.raw_text or "").encode("utf-8")).hexdigest()
        meta_small = {"function": self.metadata.get("function")} if isinstance(self.metadata, dict) else None
        return DocumentRow(
            doc_id=self.doc_id,
            logical_key_id=self.logical_key_id,
            stage=self.stage,
            task_id=task_id,
            parent_doc_id=self.parent_doc_id,
            template_id=str(template_id) if template_id is not None else None,
            model_id=str(model_id) if model_id is not None else None,
            run_id=str(run_id) if run_id is not None else None,
            parser=parser,
            status=status,
            usage_prompt_tokens=None,
            usage_completion_tokens=None,
            usage_max_tokens=None,
            doc_dir=str(rel_dir),
            raw_chars=len(self.raw_text or ""),
            parsed_chars=len(self.parsed_text or ""),
            content_hash=content_hash,
            meta_small=meta_small,
            lineage_prev_doc_id=None,
        )

