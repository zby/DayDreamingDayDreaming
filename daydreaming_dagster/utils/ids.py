from __future__ import annotations

import hashlib
from typing import Iterable


def _hash_bytes(parts: Iterable[str]) -> bytes:
    h = hashlib.sha256()
    for p in parts:
        if not isinstance(p, str):
            p = str(p)
        h.update(p.encode("utf-8"))
        h.update(b"|")
    return h.digest()


_ALPHABET_36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def _to_base36(num: int) -> str:
    if num == 0:
        return "0"
    chars = []
    n = num
    while n > 0:
        n, r = divmod(n, 36)
        chars.append(_ALPHABET_36[r])
    return "".join(reversed(chars))


"""ID helpers for doc-id-first execution (no logical keys)."""


def doc_dir(root: str | "os.PathLike[str]", stage: str, doc_id: str):
    from pathlib import Path

    # Flat layout by stage and doc_id only
    return Path(root) / stage / doc_id


def reserve_doc_id(stage: str, task_id: str, *, run_id: str | None = None, salt: str | None = None, length: int = 16) -> str:
    """Reserve a deterministic 16-char base36 document id for a task row.

    Inputs are combined and hashed; no logical key is computed or stored.
    If run_id/salt are omitted, the id remains stable for the same (stage, task_id).
    """
    parts: list[str] = [stage, task_id]
    if run_id:
        parts.append(str(run_id))
    if salt:
        parts.append(str(salt))
    digest = _hash_bytes(parts)
    val = int.from_bytes(digest[:12], "big")
    b36 = _to_base36(val)
    if len(b36) < length:
        b36 = ("0" * (length - len(b36))) + b36
    return b36[:length]
