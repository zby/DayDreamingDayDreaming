from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time

from daydreaming_dagster.config.paths import Paths
from daydreaming_dagster.utils.generation import write_gen_raw
from .stage_core import generate_llm


@dataclass
class RawGenerationResult:
    """Structured output for raw generation assets."""

    raw_text: str
    raw_path: Path
    main_metadata_path: Path
    raw_metadata: Dict[str, Any]


def _write_raw_metadata(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_raw_metadata(
    *,
    stage: str,
    gen_id: str,
    llm_model_id: Optional[str],
    mode: str,
    duration_s: Optional[float],
    finish_reason: Optional[str],
    truncated: Optional[bool],
    usage: Optional[Any],
    extras: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "stage": stage,
        "gen_id": gen_id,
        "mode": mode,
    }
    if llm_model_id is not None:
        payload["llm_model_id"] = llm_model_id
    if duration_s is not None:
        payload["duration_s"] = duration_s
    if finish_reason is not None:
        payload["finish_reason"] = finish_reason
    if truncated is not None:
        payload["truncated"] = bool(truncated)
    if usage is not None:
        payload["usage"] = usage
    if extras:
        for key, value in extras.items():
            if key not in payload:
                payload[key] = value
    return payload


def perform_llm_raw_generation(
    *,
    stage: str,
    llm_client,
    data_root: Path,
    gen_id: str,
    template_id: str,
    prompt_text: str,
    llm_model_id: Optional[str],
    max_tokens: Optional[int],
    metadata_extras: Optional[Dict[str, Any]] = None,
) -> RawGenerationResult:
    """Invoke an LLM and persist raw artifacts for a generation stage."""

    paths = Paths.from_str(data_root)
    raw_path = paths.raw_path(stage, gen_id)
    main_metadata_path = paths.metadata_path(stage, gen_id)
    raw_metadata_path = paths.raw_metadata_path(stage, gen_id)

    start = time.time()
    raw_text, info = generate_llm(llm_client, prompt_text, model=str(llm_model_id or ""), max_tokens=max_tokens)
    duration = round(time.time() - start, 3)

    write_gen_raw(paths.gens_root, stage, gen_id, raw_text)

    finish_reason = info.get("finish_reason") or info.get("finishReason")
    truncated = info.get("truncated")
    usage = info.get("usage")

    raw_metadata = _build_raw_metadata(
        stage=stage,
        gen_id=gen_id,
        llm_model_id=llm_model_id,
        mode="llm",
        duration_s=duration,
        finish_reason=finish_reason,
        truncated=truncated,
        usage=usage,
        extras=metadata_extras,
    )
    _write_raw_metadata(raw_metadata_path, raw_metadata)

    return RawGenerationResult(
        raw_text=raw_text,
        raw_path=raw_path,
        main_metadata_path=main_metadata_path,
        raw_metadata=raw_metadata,
    )


def perform_copy_raw_generation(
    *,
    stage: str,
    data_root: Path,
    gen_id: str,
    source_stage: str,
    source_gen_id: str,
    metadata_extras: Optional[Dict[str, Any]] = None,
) -> RawGenerationResult:
    """Copy upstream parsed text into the raw slot for copy-mode stages."""

    paths = Paths.from_str(data_root)
    raw_path = paths.raw_path(stage, gen_id)
    main_metadata_path = paths.metadata_path(stage, gen_id)
    raw_metadata_path = paths.raw_metadata_path(stage, gen_id)

    source_path = paths.parsed_path(source_stage, source_gen_id)
    raw_text = source_path.read_text(encoding="utf-8") if source_path.exists() else ""

    write_gen_raw(paths.gens_root, stage, gen_id, raw_text)

    raw_metadata = _build_raw_metadata(
        stage=stage,
        gen_id=gen_id,
        llm_model_id=None,
        mode="copy",
        duration_s=None,
        finish_reason="copy",
        truncated=False,
        usage=None,
        extras={"copied_from": str(source_path), **(metadata_extras or {})},
    )
    _write_raw_metadata(raw_metadata_path, raw_metadata)

    return RawGenerationResult(
        raw_text=raw_text,
        raw_path=raw_path,
        main_metadata_path=main_metadata_path,
        raw_metadata=raw_metadata,
    )
