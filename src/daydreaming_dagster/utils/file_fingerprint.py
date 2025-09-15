from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


@dataclass(frozen=True)
class FileEntry:
    rel_path: str
    size: int
    mtime_ns: int

    def fingerprint(self) -> str:
        h = hashlib.sha256()
        h.update(self.rel_path.encode("utf-8"))
        h.update(str(self.size).encode("ascii"))
        h.update(str(self.mtime_ns).encode("ascii"))
        return h.hexdigest()[:16]


def scan_files(base_dir: Path, files: Iterable[Path]) -> List[FileEntry]:
    entries: List[FileEntry] = []
    for fp in files:
        if not fp.exists():
            continue
        stat = fp.stat()
        rel = str(fp.relative_to(base_dir))
        entries.append(FileEntry(rel_path=rel, size=stat.st_size, mtime_ns=stat.st_mtime_ns))
    entries.sort(key=lambda e: e.rel_path)
    return entries


def combined_fingerprint(entries: Iterable[FileEntry]) -> str:
    h = hashlib.sha256()
    for e in entries:
        h.update(e.rel_path.encode("utf-8"))
        h.update(str(e.size).encode("ascii"))
        h.update(str(e.mtime_ns).encode("ascii"))
    return h.hexdigest()[:16]


# Removed unused diff_by_rel_path helper to keep surface minimal.
