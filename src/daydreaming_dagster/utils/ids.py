from __future__ import annotations

import hashlib
from hashlib import blake2b
import json
import logging
from pathlib import Path
from typing import Iterable, Tuple, Dict

from .errors import DDError, Err
from ..types import Stage


DETERMINISTIC_GEN_IDS_ENABLED = True  # BACKCOMPAT: deterministic IDs are always enabled post-migration.


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


"""ID helpers for gen-id-first execution (no logical keys)."""


def gen_dir(root: str | "os.PathLike[str]", stage: str, gen_id: str):
    from pathlib import Path

    # Flat layout by stage and gen_id only
    return Path(root) / stage / gen_id


def reserve_gen_id(stage: str, task_id: str, *, run_id: str | None = None, salt: str | None = None, length: int = 16) -> str:
    """Reserve a deterministic 16-char base36 generation id for a task row.

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


# ---------------- Deterministic generation ids ---------------- #

_PREFIX_BY_STAGE: Dict[Stage, str] = {
    "draft": "d_",
    "essay": "e_",
    "evaluation": "v_",
}


def _normalize_component(value, *, field: str) -> str:
    if value is None:
        raise DDError(Err.INVALID_CONFIG, ctx={"field": field, "reason": "missing"})
    text = str(value).strip()
    if not text:
        raise DDError(Err.INVALID_CONFIG, ctx={"field": field, "reason": "empty"})
    return text.lower()


def _coerce_replicate(value, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise DDError(Err.INVALID_CONFIG, ctx={"field": field, "value": value})


def draft_signature(combo_id: str, draft_template_id: str, generation_model_id: str, replicate_index: int) -> tuple[str, str, str, int]:
    return (
        _normalize_component(combo_id, field="combo_id"),
        _normalize_component(draft_template_id, field="draft_template_id"),
        _normalize_component(generation_model_id, field="llm_model_id"),
        _coerce_replicate(replicate_index, field="replicate"),
    )


def essay_signature(draft_gen_id: str, essay_template_id: str, replicate_index: int) -> tuple[str, str, int]:
    return (
        _normalize_component(draft_gen_id, field="draft_gen_id"),
        _normalize_component(essay_template_id, field="essay_template_id"),
        _coerce_replicate(replicate_index, field="replicate"),
    )


def evaluation_signature(essay_gen_id: str, evaluation_template_id: str, evaluation_model_id: str, replicate_index: int) -> tuple[str, str, str, int]:
    return (
        _normalize_component(essay_gen_id, field="essay_gen_id"),
        _normalize_component(evaluation_template_id, field="evaluation_template_id"),
        _normalize_component(evaluation_model_id, field="llm_model_id"),
        _coerce_replicate(replicate_index, field="replicate"),
    )


def compute_deterministic_gen_id(stage: Stage, signature: Tuple, *, collision_index: int = 0) -> str:
    stage_norm = str(stage).lower()
    try:
        prefix = _PREFIX_BY_STAGE[stage_norm]
    except KeyError as exc:
        raise DDError(Err.INVALID_CONFIG, ctx={"stage": stage}) from exc
    if str(stage) != stage_norm:
        raise DDError(Err.INVALID_CONFIG, ctx={"stage": stage})
    canonical = "|".join(str(part) for part in signature)
    if collision_index:
        canonical = f"{canonical}|collision:{collision_index}"
    digest = blake2b(canonical.encode("utf-8"), digest_size=10).digest()
    value = int.from_bytes(digest, "big")
    b36 = _to_base36(value)
    base_id = prefix + b36
    if collision_index:
        return f"{base_id}-{collision_index}"
    return base_id


def compute_collision_resolved_gen_id(stage: Stage, signature: Tuple, gens_root: Path, *, max_attempts: int = 5) -> str:
    """Return a deterministic id, appending suffixes if collisions are detected on disk."""

    stage_norm = str(stage).lower()
    root = Path(gens_root) / stage_norm
    root.mkdir(parents=True, exist_ok=True)

    for attempt in range(max_attempts):
        candidate = compute_deterministic_gen_id(stage_norm, signature, collision_index=attempt)
        meta_path = root / candidate / "metadata.json"
        if not meta_path.exists():
            return candidate
        try:
            existing = json.loads(meta_path.read_text(encoding="utf-8"))
            existing_sig = signature_from_metadata(stage_norm, existing)
        except Exception:
            existing_sig = None

        if existing_sig == signature:
            return candidate

        logging.getLogger(__name__).warning(
            "deterministic id collision detected for %s/%s; attempting suffix %s",
            stage_norm,
            candidate,
            attempt + 1,
        )

    raise DDError(
        Err.INVALID_CONFIG,
        ctx={"stage": stage_norm, "reason": "deterministic_id_collision", "signature": signature},
    )


def signature_from_metadata(stage: Stage, metadata: dict) -> Tuple:
    rep = metadata.get("replicate")
    if rep is None:
        rep = 1
    elif isinstance(rep, str) and rep.strip().isdigit():
        rep = int(rep.strip())
    elif isinstance(rep, (int, float)):
        rep = int(rep)
    else:
        raise DDError(Err.INVALID_CONFIG, ctx={"field": "replicate", "value": rep})

    if stage == "draft":
        return draft_signature(
            metadata.get("combo_id"),
            metadata.get("template_id"),
            metadata.get("llm_model_id"),
            rep,
        )
    if stage == "essay":
        return essay_signature(
            metadata.get("parent_gen_id"),
            metadata.get("template_id") or metadata.get("essay_template"),
            rep,
        )
    if stage == "evaluation":
        return evaluation_signature(
            metadata.get("parent_gen_id"),
            metadata.get("template_id") or metadata.get("evaluation_template"),
            metadata.get("llm_model_id"),
            rep,
        )
    raise DDError(Err.INVALID_CONFIG, ctx={"stage": stage})
