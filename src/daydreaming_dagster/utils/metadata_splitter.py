"""Utilities for migrating legacy generation metadata into the new split layout."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
import json
import math

from daydreaming_dagster.data_layer.paths import Paths


@dataclass
class MetadataSplit:
    """Container for the three metadata files emitted after splitting."""

    main: Dict[str, Any]
    raw: Dict[str, Any]
    parsed: Dict[str, Any]


RAW_FUNCTION_BY_STAGE = {
    "draft": "draft_raw",
    "essay": "essay_raw",
    "evaluation": "evaluation_raw",
}

PARSED_FUNCTION_BY_STAGE = {
    "draft": "draft_parsed",
    "essay": "essay_parsed",
    "evaluation": "evaluation_parsed",
}


def _clean_optional(value: Any) -> Any:
    """Return ``None`` for legacy empty markers such as ``nan``."""

    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        lowered = trimmed.lower()
        if lowered in {"nan", "none", "null"}:
            return None
        return trimmed
    return value


def _maybe_int(value: Any) -> Optional[int]:
    value = _clean_optional(value)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _maybe_float(value: Any) -> Optional[float]:
    value = _clean_optional(value)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_bool(value: Any) -> Optional[bool]:
    value = _clean_optional(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "yes", "y", "1"}:
            return True
        if lowered in {"false", "f", "no", "n", "0"}:
            return False
    return None


def _first_not_none(values: Iterable[Any]) -> Any:
    for item in values:
        cleaned = _clean_optional(item)
        if cleaned is not None:
            return cleaned
    return None


def _infer_mode(legacy: Dict[str, Any]) -> str:
    value = _clean_optional(legacy.get("mode"))
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"llm", "copy"}:
            return lowered

    legacy_function = _clean_optional(legacy.get("function"))
    if isinstance(legacy_function, str) and "copy" in legacy_function:
        return "copy"

    if _clean_optional(legacy.get("usage")) is not None:
        return "llm"

    if _clean_optional(legacy.get("source_file")) or _clean_optional(legacy.get("created_from")):
        return "copy"

    return "llm"


def split_legacy_metadata(
    *,
    stage: str,
    gen_id: str,
    legacy: Dict[str, Any],
    paths: Paths,
) -> MetadataSplit:
    """Transform a legacy ``metadata.json`` payload into the new split structure."""

    stage = str(stage)
    gen_id = str(gen_id)
    raw_path = paths.raw_path(stage, gen_id)
    parsed_path = paths.parsed_path(stage, gen_id)

    template_id = _first_not_none(
        [
            legacy.get("template_id"),
            legacy.get("draft_template"),
            legacy.get("essay_template"),
            legacy.get("evaluation_template"),
        ]
    )
    llm_model_id = _first_not_none([legacy.get("llm_model_id"), legacy.get("model_id")])
    parent_gen_id = _clean_optional(legacy.get("parent_gen_id"))
    parser_name = _clean_optional(legacy.get("parser_name"))
    combo_id = _clean_optional(legacy.get("combo_id"))
    cohort_id = _clean_optional(legacy.get("cohort_id"))
    task_id = _clean_optional(legacy.get("task_id"))
    parent_task_id = _clean_optional(legacy.get("parent_task_id"))
    source_file = _clean_optional(legacy.get("source_file"))
    created_from = _clean_optional(legacy.get("created_from"))
    synthesized_from = _clean_optional(legacy.get("synthesized_from"))
    legacy_function = _clean_optional(legacy.get("function"))
    run_id = _clean_optional(legacy.get("run_id"))
    replicate = _maybe_int(legacy.get("replicate"))

    mode = _infer_mode(legacy)

    main_metadata: Dict[str, Any] = {
        "stage": stage,
        "gen_id": gen_id,
        "mode": mode,
    }

    if template_id is not None:
        main_metadata["template_id"] = template_id
    if llm_model_id is not None:
        main_metadata["llm_model_id"] = llm_model_id
    if parent_gen_id is not None:
        main_metadata["parent_gen_id"] = parent_gen_id
    if parser_name is not None:
        main_metadata["parser_name"] = parser_name
    if combo_id is not None:
        main_metadata["combo_id"] = combo_id
    if cohort_id is not None:
        main_metadata["cohort_id"] = cohort_id
    if task_id is not None:
        main_metadata["task_id"] = task_id
    if parent_task_id is not None:
        main_metadata["parent_task_id"] = parent_task_id
    if source_file is not None:
        main_metadata["source_file"] = source_file
    if created_from is not None:
        main_metadata["created_from"] = created_from
    if synthesized_from is not None:
        main_metadata["synthesized_from"] = synthesized_from
    if run_id is not None:
        main_metadata["run_id"] = run_id

    # Preserve stage-specific template aliases when present.
    for alias_key in ("draft_template", "essay_template", "evaluation_template"):
        alias_value = _clean_optional(legacy.get(alias_key))
        if alias_value is not None:
            main_metadata[alias_key] = alias_value

    files = legacy.get("files")
    if isinstance(files, dict):
        filtered: Dict[str, Any] = {}
        for key, value in files.items():
            cleaned = _clean_optional(value)
            if cleaned is not None:
                filtered[key] = cleaned
        if filtered:
            main_metadata["files"] = filtered

    raw_metadata: Dict[str, Any] = {
        "stage": stage,
        "gen_id": gen_id,
        "mode": mode,
    }
    if llm_model_id is not None:
        raw_metadata["llm_model_id"] = llm_model_id
    if run_id is not None:
        raw_metadata["run_id"] = run_id
    if combo_id is not None:
        raw_metadata["combo_id"] = combo_id
    if cohort_id is not None:
        raw_metadata["cohort_id"] = cohort_id
    if replicate is not None:
        raw_metadata["replicate"] = replicate

    finish_reason = _clean_optional(legacy.get("finish_reason"))
    if finish_reason is not None:
        raw_metadata["finish_reason"] = finish_reason
    truncated = _maybe_bool(legacy.get("truncated"))
    if truncated is not None:
        raw_metadata["truncated"] = truncated
    usage = legacy.get("usage")
    if usage is not None:
        raw_metadata["usage"] = usage

    for key in ("duration_s", "duration_ms"):
        val = _maybe_float(legacy.get(key))
        if val is not None:
            raw_metadata[key] = val

    for key in ("prompt_chars", "raw_chars", "total_tokens", "max_tokens"):
        val = _maybe_int(legacy.get(key))
        if val is not None:
            raw_metadata[key] = val

    if legacy_function is not None:
        raw_metadata["legacy_function"] = legacy_function
    raw_function = RAW_FUNCTION_BY_STAGE.get(stage)
    if raw_function:
        raw_metadata["function"] = raw_function

    raw_metadata["input_mode"] = "copy" if mode == "copy" else "prompt"

    copied_from = _clean_optional(legacy.get("copied_from"))
    if copied_from is None and mode == "copy":
        copied_from = source_file or created_from or synthesized_from
    if copied_from is not None:
        raw_metadata["copied_from"] = copied_from

    files_dict = main_metadata.get("files") if isinstance(main_metadata.get("files"), dict) else {}
    raw_file = _clean_optional(files_dict.get("raw")) if isinstance(files_dict, dict) else None
    if raw_file is not None:
        raw_metadata["raw_path"] = raw_file
    elif raw_path.exists():
        raw_metadata["raw_path"] = str(raw_path.resolve())

    parsed_metadata: Dict[str, Any] = {
        "stage": stage,
        "gen_id": gen_id,
        "parser_name": parser_name or "legacy",
        "success": parsed_path.exists(),
        "error": None,
    }

    parsed_chars = _maybe_int(legacy.get("parsed_chars"))
    if parsed_chars is not None:
        parsed_metadata["parsed_chars"] = parsed_chars
    if combo_id is not None:
        parsed_metadata["combo_id"] = combo_id
    if cohort_id is not None:
        parsed_metadata["cohort_id"] = cohort_id
    if replicate is not None:
        parsed_metadata["replicate"] = replicate
    parsed_metadata["input_mode"] = raw_metadata.get("input_mode")
    if copied_from is not None:
        parsed_metadata["copied_from"] = copied_from
    if run_id is not None:
        parsed_metadata["run_id"] = run_id

    parsed_file = _clean_optional(files_dict.get("parsed")) if isinstance(files_dict, dict) else None
    if parsed_file is not None:
        parsed_metadata["parsed_path"] = parsed_file
    elif parsed_path.exists():
        parsed_metadata["parsed_path"] = str(parsed_path.resolve())

    parsed_function = PARSED_FUNCTION_BY_STAGE.get(stage)
    if parsed_function:
        parsed_metadata["function"] = parsed_function

    legacy_keys = {
        key for key in legacy.keys() if key not in {"files", "mode", "function"}
    }
    # Track keys that could not be classified cleanly for manual review.
    consumed = set(main_metadata.keys()) | set(raw_metadata.keys()) | set(parsed_metadata.keys())
    consumed.update({"stage", "gen_id"})
    unused = [key for key in legacy_keys if key not in consumed]
    if unused:
        main_metadata.setdefault("legacy_fields", {})
        for key in unused:
            if key not in main_metadata["legacy_fields"]:
                main_metadata["legacy_fields"][key] = legacy.get(key)

    return MetadataSplit(main=main_metadata, raw=raw_metadata, parsed=parsed_metadata)


def split_metadata_file(metadata_path: Path, *, paths: Paths) -> MetadataSplit:
    """Read a legacy ``metadata.json`` file and return the split representation."""

    stage = metadata_path.parent.parent.name
    gen_id = metadata_path.parent.name
    legacy = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(legacy, dict):
        raise ValueError(f"Legacy metadata is not a JSON object: {metadata_path}")
    return split_legacy_metadata(stage=stage, gen_id=gen_id, legacy=legacy, paths=paths)


def migrate_generation_metadata(
    data_root: Path,
    *,
    dry_run: bool = True,
    overwrite: bool = False,
) -> Dict[str, int]:
    """Split all legacy generation metadata files under ``data_root``.

    Returns a simple counter dictionary describing the migration outcome.
    """

    paths = Paths.from_str(data_root)
    stats = {
        "evaluated": 0,
        "converted": 0,
        "skipped_existing": 0,
        "errors": 0,
    }

    gens_root = paths.gens_root
    if not gens_root.exists():
        return stats

    for stage_dir in sorted(p for p in gens_root.iterdir() if p.is_dir()):
        stage = stage_dir.name
        for gen_dir in sorted(p for p in stage_dir.iterdir() if p.is_dir()):
            metadata_path = gen_dir / "metadata.json"
            if not metadata_path.exists():
                continue
            stats["evaluated"] += 1
            raw_meta_path = gen_dir / "raw_metadata.json"
            parsed_meta_path = gen_dir / "parsed_metadata.json"
            if raw_meta_path.exists() and parsed_meta_path.exists() and not overwrite:
                stats["skipped_existing"] += 1
                continue
            try:
                split = split_metadata_file(metadata_path, paths=paths)
            except Exception:
                stats["errors"] += 1
                continue

            if dry_run:
                stats["converted"] += 1
                continue

            gen_dir.mkdir(parents=True, exist_ok=True)
            metadata_path.write_text(json.dumps(split.main, ensure_ascii=False, indent=2), encoding="utf-8")
            raw_meta_path.write_text(json.dumps(split.raw, ensure_ascii=False, indent=2), encoding="utf-8")
            parsed_meta_path.write_text(json.dumps(split.parsed, ensure_ascii=False, indent=2), encoding="utf-8")
            stats["converted"] += 1

    return stats


__all__ = [
    "MetadataSplit",
    "migrate_generation_metadata",
    "split_legacy_metadata",
    "split_metadata_file",
]
