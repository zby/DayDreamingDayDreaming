from __future__ import annotations

from pathlib import Path
from os import PathLike
from typing import Optional, Any, Dict

from dagster import MetadataValue
from dagster._core.errors import DagsterInvalidPropertyError

"""Helper utilities shared across asset implementations.

Note: prefer using Paths.from_context(context) directly in new code.
"""


def get_run_id(context) -> Optional[str]:
    """Best-effort extraction of the Dagster run id from an asset context."""
    try:
        run = getattr(context, "run", None)
    except DagsterInvalidPropertyError:
        run = None
    except AttributeError:
        run = None
    if run is not None:
        rid = getattr(run, "run_id", None)
        if rid:
            return str(rid)
    rid = getattr(context, "run_id", None)
    return str(rid) if rid else None


def emit_standard_output_metadata(
    context,
    *,
    function: str,
    gen_id: str,
    result: "ExecutionResultLike",
    extras: Optional[dict] = None,
) -> None:
    """Attach standardized metadata for legacy response-style generation outputs."""

    # Accept both our dataclass and a mapping-like result
    def _get(obj: Any, key: str, default=None):
        if hasattr(obj, key):
            return getattr(obj, key)
        if isinstance(obj, dict):
            return obj.get(key, default)
        return default

    info: Dict[str, Any] = _get(result, "info", {}) or {}

    md: Dict[str, MetadataValue] = {
        "function": MetadataValue.text(str(function)),
        "gen_id": MetadataValue.text(str(gen_id)),
        "finish_reason": MetadataValue.text(
            str(info.get("finish_reason"))
            if isinstance(info.get("finish_reason"), (str, int, float))
            else ""
        ),
        "truncated": MetadataValue.bool(bool(info.get("truncated"))),
    }

    meta = _get(result, "metadata", {}) or {}

    def _try_add_numeric(key: str, value, *, as_float: bool = False) -> None:
        try:
            if value is None:
                return
            md[key] = MetadataValue.float(float(value)) if as_float else MetadataValue.int(int(value))
        except Exception:
            pass

    try:
        if isinstance(meta, dict) and meta.get("duration_s") is not None:
            md["duration_s"] = MetadataValue.float(float(meta.get("duration_s")))
            if meta.get("duration_ms") is not None:
                md["duration_ms"] = MetadataValue.int(int(meta.get("duration_ms")))
    except Exception:
        pass

    try:
        if isinstance(meta, dict) and meta.get("max_tokens") is not None:
            md["max_tokens"] = MetadataValue.int(int(meta.get("max_tokens")))
    except Exception:
        pass

    if isinstance(meta, dict):
        _try_add_numeric("prompt_chars", meta.get("prompt_chars"))
        _try_add_numeric("raw_chars", meta.get("raw_chars"))
        _try_add_numeric("parsed_chars", meta.get("parsed_chars"))
        _try_add_numeric("prompt_lines", meta.get("prompt_lines"))
        _try_add_numeric("raw_lines", meta.get("raw_lines"))
        _try_add_numeric("parsed_lines", meta.get("parsed_lines"))

    def _total_tokens_from(u: Any) -> int | None:
        if isinstance(u, dict):
            t = u.get("total_tokens")
            if t is None:
                t = u.get("totalTokens") or u.get("total")
            try:
                return int(t) if isinstance(t, (int, float)) else None
            except Exception:
                return None
        return None

    if isinstance(meta, dict) and isinstance(meta.get("usage"), dict):
        tt = _total_tokens_from(meta.get("usage"))
        if tt is not None:
            md["total_tokens"] = MetadataValue.int(tt)
    else:
        u = info.get("usage") if isinstance(info, dict) else None
        tt = _total_tokens_from(u)
        if tt is not None:
            md["total_tokens"] = MetadataValue.int(tt)

    if extras:
        for k, v in extras.items():
            if k not in md:
                if isinstance(v, bool):
                    md[k] = MetadataValue.bool(v)
                elif isinstance(v, int):
                    md[k] = MetadataValue.int(v)
                elif isinstance(v, float):
                    md[k] = MetadataValue.float(v)
                elif isinstance(v, (list, dict)):
                    md[k] = MetadataValue.json(v)
                else:
                    md[k] = MetadataValue.text(str(v))

    context.add_output_metadata(md)


ExecutionResultLike = Any

__all__ = [
    "get_run_id",
    "build_stage_artifact_metadata",
    "emit_standard_output_metadata",
]


def _assign_metadata_value(md: Dict[str, MetadataValue], key: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, (str, Path, PathLike)) and key.endswith("_path"):
        md[key] = MetadataValue.path(str(value))
        return
    if isinstance(value, bool):
        md[key] = MetadataValue.bool(value)
    elif isinstance(value, int):
        md[key] = MetadataValue.int(value)
    elif isinstance(value, float):
        md[key] = MetadataValue.float(value)
    elif isinstance(value, (list, dict)):
        md[key] = MetadataValue.json(value)
    else:
        md[key] = MetadataValue.text(str(value))


def build_stage_artifact_metadata(
    *,
    function: str,
    artifact_label: str,
    metadata: Dict[str, Any],
    text: Optional[str] = None,
) -> Dict[str, MetadataValue]:
    """Standardized metadata payload for stage artifacts (input/raw/parsed)."""

    label = str(artifact_label or "artifact").lower()
    gen_id = metadata["gen_id"]
    stage = metadata["stage"]

    data = dict(metadata or {})

    for drop_key in ("stage", "gen_id", "function"):
        data.pop(drop_key, None)

    md: Dict[str, MetadataValue] = {
        "function": MetadataValue.text(str(function)),
        "gen_id": MetadataValue.text(str(gen_id)),
        "stage": MetadataValue.text(str(stage)),
    }

    for key in ("template_id", "mode", "parent_gen_id"):
        value = data.pop(key, None)
        if value:
            md[key] = MetadataValue.text(str(value))

    length_key = f"{label}_length"
    lines_key = f"{label}_lines"
    if text is not None:
        data.setdefault(length_key, len(text))
        data.setdefault(lines_key, sum(1 for _ in str(text).splitlines()))

    for metric_key in (length_key, lines_key):
        value = data.pop(metric_key, None)
        if value is not None:
            _assign_metadata_value(md, metric_key, value)

    path_key = f"{label}_path"
    meta_path_key = f"{label}_metadata_path"
    for key in (path_key, meta_path_key):
        value = data.pop(key, None)
        if value is not None:
            _assign_metadata_value(md, key, value)

    md[f"{label}_metadata"] = MetadataValue.json(metadata)

    for key, value in data.items():
        _assign_metadata_value(md, key, value)

    return md
