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


__all__ = [
    "get_run_id",
    "build_stage_artifact_metadata",
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
