from __future__ import annotations

from typing import Any, Dict, Optional

from dagster import MetadataValue

from daydreaming_dagster.assets._helpers import get_run_id, build_stage_artifact_metadata
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer, resolve_generation_metadata
from daydreaming_dagster.utils.errors import DDError, Err
from .stage_core import Stage, parse_text, resolve_parser_name


def _count_non_empty_lines(text: str) -> int:
    return sum(1 for line in text.splitlines() if line.strip())


def _stage_parsed_asset(
    *,
    data_layer: GensDataLayer,
    stage: Stage,
    gen_id: str,
    raw_text: str,
    parser_name: str,
    raw_metadata: Dict[str, Any],
    stage_settings,
    min_lines_override: Optional[int],
    fail_on_truncation: bool,
) -> tuple[str, Dict[str, Any]]:
    data_layer.reserve_generation(stage, gen_id, create=True)

    # Check if we should skip regeneration
    force = stage_settings.force if stage_settings else False
    if not force and data_layer.parsed_exists(stage, gen_id):
        # Reuse existing artifact
        parsed_text = data_layer.read_parsed(stage, gen_id)

        # Try to read existing metadata, but don't fail if missing
        try:
            parsed_metadata = data_layer.read_parsed_metadata(stage, gen_id)
        except DDError as err:
            if err.code is Err.DATA_MISSING:
                # Warn about missing metadata but proceed with reuse
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Reusing parsed.txt for {stage}/{gen_id} but parsed_metadata.json is missing"
                )
                parsed_metadata = {
                    "function": f"{stage}_parsed",
                    "stage": str(stage),
                    "gen_id": str(gen_id),
                }
            else:
                raise

        # Mark as reused
        parsed_metadata["reused"] = True

        return parsed_text, parsed_metadata

    metadata = resolve_generation_metadata(data_layer, stage, gen_id)
    raw_metadata = raw_metadata or {}

    if fail_on_truncation and bool(raw_metadata.get("truncated")):
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "stage": stage,
                "gen_id": gen_id,
                "reason": "truncated_raw",
            },
        )

    effective_min_lines = (
        min_lines_override
        if min_lines_override is not None
        else (stage_settings.min_lines if stage_settings else None)
    )

    if isinstance(effective_min_lines, int) and effective_min_lines > 0:
        non_empty = _count_non_empty_lines(raw_text)
        if non_empty < effective_min_lines:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "reason": "min_lines_not_met",
                    "observed": non_empty,
                    "required": effective_min_lines,
                },
            )

    if not parser_name:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "stage": stage,
                "gen_id": gen_id,
                "reason": "missing_parser_name",
            },
        )

    parsed_text = parse_text(stage, raw_text, parser_name)
    if not isinstance(parsed_text, str):
        raw_path = data_layer.paths.raw_path(stage, gen_id)
        tail_lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()][-5:]
        preview = " | ".join(tail_lines)
        hint = (
            "This usually means the raw evaluation response did not contain a parseable "
            "0-9 score."
        )
        message = (
            f"Parser '{parser_name}' could not extract a valid result for stage '{stage}' "
            f"(generation '{gen_id}'). {hint} Inspect the raw output at {raw_path}."
        )
        if preview:
            message += f" Last non-empty lines: {preview}"
        raise DDError(
            Err.PARSER_FAILURE,
            ctx={
                "stage": stage,
                "gen_id": gen_id,
                "parser_name": parser_name,
                "reason": "parse_failed",
                "preview": preview or None,
            },
        )

    parsed_metadata: Dict[str, Any] = {
        "stage": str(stage),
        "gen_id": str(gen_id),
        "function": f"{stage}_parsed",
        "parser_name": parser_name,
        "success": True,
        "error": None,
        "reused": False,
    }

    for key in ("input_mode", "copied_from"):
        if key in raw_metadata:
            parsed_metadata[key] = raw_metadata[key]

    parsed_path = data_layer.write_parsed(stage, gen_id, parsed_text)
    parsed_metadata_path = data_layer.paths.parsed_metadata_path(stage, gen_id)
    parsed_metadata["parsed_path"] = str(parsed_path)
    parsed_metadata["parsed_metadata_path"] = str(parsed_metadata_path)
    data_layer.write_parsed_metadata(stage, gen_id, parsed_metadata)

    return parsed_text, parsed_metadata


def stage_parsed_asset(
    context,
    stage: Stage,
    *,
    raw_text: str,
    parser_name: Optional[str] = None,
    raw_metadata: Optional[Dict[str, Any]] = None,
    fail_on_truncation: bool = True,
    min_lines: Optional[int] = None,
) -> str:
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    metadata = resolve_generation_metadata(data_layer, stage, gen_id)

    if raw_metadata is None:
        try:
            raw_metadata = data_layer.read_raw_metadata(stage, gen_id)
        except DDError as err:
            if err.code is Err.DATA_MISSING:
                raw_metadata = {}
            else:
                raise

    if parser_name is None:
        parser_name = resolve_parser_name(data_layer.data_root, stage, metadata.template_id, None)

    experiment_config = getattr(context.resources, "experiment_config", None)
    if experiment_config is not None:
        settings = experiment_config.stage_config.get(stage)
        if min_lines is None and settings:
            min_lines = settings.min_lines

    run_id = get_run_id(context)

    stage_settings = experiment_config.stage_config.get(stage) if experiment_config else None

    parsed_text, parsed_metadata = _stage_parsed_asset(
        data_layer=data_layer,
        stage=stage,
        gen_id=gen_id,
        raw_text=raw_text,
        parser_name=parser_name,
        raw_metadata=raw_metadata or {},
        stage_settings=stage_settings,
        min_lines_override=min_lines,
        fail_on_truncation=fail_on_truncation,
    )

    if run_id:
        parsed_metadata["run_id"] = run_id
        data_layer.write_parsed_metadata(stage, gen_id, parsed_metadata)

    context.add_output_metadata(
        build_stage_artifact_metadata(
            function=f"{stage}_parsed",
            artifact_label="parsed",
            metadata=parsed_metadata,
            text=parsed_text,
        )
    )

    return parsed_text


__all__ = ["stage_parsed_asset"]
