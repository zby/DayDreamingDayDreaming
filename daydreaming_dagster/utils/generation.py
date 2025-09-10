from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Optional
from .ids import gen_dir as build_gen_dir
from ..constants import FILE_RAW, FILE_PARSED, FILE_PROMPT, FILE_METADATA


def _write_atomic(path: Path, data: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


@dataclass
class Generation:
    """Domain object representing a generated artifact (a generation).

    Encapsulates filesystem writes for raw/parsed/prompt/metadata.
    """

    stage: str
    gen_id: str
    parent_gen_id: str | None
    raw_text: str
    parsed_text: str | None
    prompt_text: str | None = None
    metadata: dict | None = None

    def target_dir(self, gens_root: Path) -> Path:
        """Resolve the canonical generation directory from gens_root."""
        return build_gen_dir(Path(gens_root), self.stage, self.gen_id)

    def write_files(self, gens_root: Path) -> Path:
        """Write raw.txt, parsed.txt, optional prompt.txt and metadata.json.

        Returns the target directory path.
        """
        base = self.target_dir(gens_root)
        base.mkdir(parents=True, exist_ok=True)
        _write_atomic(base / FILE_RAW, self.raw_text)
        if isinstance(self.parsed_text, str):
            _write_atomic(base / FILE_PARSED, self.parsed_text)
        if isinstance(self.prompt_text, str):
            _write_atomic(base / FILE_PROMPT, self.prompt_text)
        if isinstance(self.metadata, dict):
            _write_atomic(base / FILE_METADATA, json.dumps(self.metadata, ensure_ascii=False, indent=2))
        return base

    # Index row conversion removed in filesystem-only mode

    @classmethod
    def load(cls, gens_root: Path, stage: str, gen_id: str) -> "Generation":
        """Best-effort read of an existing generation from disk.

        Missing text files become empty string/None. Metadata is parsed when available.
        """
        base = build_gen_dir(Path(gens_root), stage, gen_id)
        def _read(name: str) -> Optional[str]:
            p = base / name
            try:
                return p.read_text(encoding="utf-8") if p.exists() else None
            except Exception:
                return None
        md = None
        mpath = base / FILE_METADATA
        if mpath.exists():
            try:
                md = json.loads(mpath.read_text(encoding="utf-8"))
            except Exception:
                md = None
        return cls(
            stage=str(stage),
            gen_id=str(gen_id),
            parent_gen_id=(md or {}).get("parent_gen_id"),
            raw_text=_read(FILE_RAW) or "",
            parsed_text=_read(FILE_PARSED),
            prompt_text=_read(FILE_PROMPT),
            metadata=md,
        )
