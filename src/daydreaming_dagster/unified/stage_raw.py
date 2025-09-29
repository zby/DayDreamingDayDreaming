from __future__ import annotations

import time
from typing import Any, Dict, Optional, Tuple

from daydreaming_dagster.assets._helpers import get_run_id, build_stage_artifact_metadata
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer, resolve_generation_metadata
from .stage_core import Stage, generate_llm


def _coerce_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _stage_raw_asset(
    *,
    data_layer: GensDataLayer,
    stage: Stage,
    gen_id: str,
    prompt_text: str,
    llm_client,
    stage_settings,
    run_id: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    data_layer.reserve_generation(stage, gen_id, create=True)

    metadata = resolve_generation_metadata(data_layer, stage, gen_id)
    main_metadata = data_layer.read_main_metadata(stage, gen_id)
    mode = (metadata.mode or "").strip().lower() or "llm"

    max_tokens = stage_settings.generation_max_tokens if stage_settings else None

    raw_metadata: Dict[str, Any] = {
        "function": f"{stage}_raw",
        "mode": mode,
        "stage": str(stage),
        "gen_id": str(gen_id),
    }
    if run_id:
        raw_metadata["run_id"] = run_id

    raw_text: str

    if mode == "llm":
        llm_model_id = _coerce_optional_str(main_metadata.get("llm_model_id"))
        if llm_model_id:
            raw_metadata["llm_model_id"] = llm_model_id
        if llm_client is None:
            raise ValueError("openrouter_client resource required for llm mode")
        start = time.time()
        raw_text, info = generate_llm(
            llm_client,
            str(prompt_text or ""),
            model=str(llm_model_id or ""),
            max_tokens=max_tokens,
        )
        duration_s = round(time.time() - start, 3)
        raw_metadata["duration_s"] = duration_s
        info = info or {}
        finish_reason = info.get("finish_reason") or info.get("finishReason")
        if finish_reason is not None:
            raw_metadata["finish_reason"] = finish_reason
        truncated = info.get("truncated")
        if truncated is not None:
            raw_metadata["truncated"] = bool(truncated)
        usage = info.get("usage")
        if usage is not None:
            raw_metadata["usage"] = usage
    elif mode == "copy":
        if prompt_text is None:
            raise ValueError("copy mode requires prompt_text")
        raw_text = prompt_text
        raw_metadata["finish_reason"] = "copy"
    else:
        raise ValueError(f"Unsupported mode '{mode}' for stage raw")

    raw_metadata["input_mode"] = "copy" if mode == "copy" else "prompt"

    raw_metadata["raw_length"] = len(raw_text)
    raw_metadata["raw_lines"] = sum(1 for _ in str(raw_text).splitlines())

    raw_path = data_layer.write_raw(stage, gen_id, raw_text)
    raw_metadata_path = data_layer.paths.raw_metadata_path(stage, gen_id)
    raw_metadata["raw_path"] = str(raw_path)
    raw_metadata["raw_metadata_path"] = str(raw_metadata_path)
    data_layer.write_raw_metadata(stage, gen_id, raw_metadata)

    return raw_text, raw_metadata


def stage_raw_asset(context, stage: Stage, *, prompt_text: str) -> str:
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    llm_client = getattr(context.resources, "openrouter_client", None)
    experiment_config = getattr(context.resources, "experiment_config", None)
    run_id = get_run_id(context)

    stage_settings = experiment_config.stage_config.get(stage) if experiment_config else None

    raw_text, raw_metadata = _stage_raw_asset(
        data_layer=data_layer,
        stage=stage,
        gen_id=gen_id,
        prompt_text=prompt_text,
        llm_client=llm_client,
        stage_settings=stage_settings,
        run_id=run_id,
    )

    output_md = build_stage_artifact_metadata(
        function=f"{stage}_raw",
        artifact_label="raw",
        metadata=raw_metadata,
        text=raw_text,
    )

    context.add_output_metadata(output_md)

    return raw_text


__all__ = ["stage_raw_asset"]
