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

    stage: str
    gen_id: str
    data_root: Path
    raw_text: str
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


def _persist_raw_generation(
    *,
    paths: Paths,
    stage: str,
    gen_id: str,
    raw_text: str,
    raw_metadata: Dict[str, Any],
) -> RawGenerationResult:
    write_gen_raw(paths.gens_root, stage, gen_id, raw_text)
    raw_metadata_path = paths.raw_metadata_path(stage, gen_id)
    _write_raw_metadata(raw_metadata_path, raw_metadata)
    main_metadata_path = paths.metadata_path(stage, gen_id)
    return RawGenerationResult(
        stage=stage,
        gen_id=gen_id,
        data_root=paths.data_root,
        raw_text=raw_text,
        main_metadata_path=main_metadata_path,
        raw_metadata=raw_metadata,
    )


def perform_raw_generation(
    *,
    stage: str,
    mode: str,
    data_root: Path,
    gen_id: str,
    input_text: str,
    metadata_extras: Optional[Dict[str, Any]] = None,
    llm_client=None,
    llm_model_id: Optional[str] = None,
    max_tokens: Optional[int] = None,
) -> RawGenerationResult:
    """Generate or copy raw content based on mode, persisting artifacts."""

    paths = Paths.from_str(data_root)

    extras = metadata_extras or {}
    if mode == "llm":
        if llm_client is None:
            raise ValueError("llm_client must be provided when mode is 'llm'")
        start = time.time()
        raw_text, info = generate_llm(llm_client, input_text, model=str(llm_model_id or ""), max_tokens=max_tokens)
        duration = round(time.time() - start, 3)
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
            extras=extras,
        )
    elif mode == "copy":
        raw_text = input_text
        raw_metadata = _build_raw_metadata(
            stage=stage,
            gen_id=gen_id,
            llm_model_id=None,
            mode="copy",
            duration_s=None,
            finish_reason="copy",
            truncated=False,
            usage=None,
            extras=extras,
        )
    else:
        raise ValueError(f"Unsupported mode '{mode}' for raw generation")

    return _persist_raw_generation(
        paths=paths,
        stage=stage,
        gen_id=gen_id,
        raw_text=raw_text,
        raw_metadata=raw_metadata,
    )
