from __future__ import annotations

from typing import Any, Iterable

from daydreaming_dagster.assets._helpers import build_stage_artifact_metadata
from daydreaming_dagster.data_layer.gens_data_layer import (
    GensDataLayer,
    resolve_generation_metadata,
)
from daydreaming_dagster.utils.errors import DDError, Err

from .stage_core import Stage, effective_parent_stage, render_template


def _count_non_empty_lines(text: str) -> int:
    return sum(1 for line in text.splitlines() if line.strip())


def _find_content_combination(
    content_combinations: Iterable[Any], combo_id: str
) -> Any | None:
    for combo in content_combinations:
        candidate = getattr(combo, "combo_id", None)
        if candidate is None and isinstance(combo, dict):
            candidate = combo.get("combo_id")
        if str(candidate or "").strip() == combo_id:
            return combo
    return None


def _base_input_info(stage: Stage, gen_id: str, metadata) -> dict[str, Any]:
    info: dict[str, Any] = {
        "stage": stage,
        "gen_id": gen_id,
        "template_id": metadata.template_id,
        "mode": metadata.mode,
    }
    if metadata.combo_id:
        info["combo_id"] = metadata.combo_id
    if metadata.origin_cohort_id:
        info["origin_cohort_id"] = metadata.origin_cohort_id
    if metadata.parent_gen_id:
        info["parent_gen_id"] = metadata.parent_gen_id
    return info


def _finalize_input_info(
    info: dict[str, Any],
    *,
    input_text: str,
    input_path,
    extras: dict[str, Any],
    mode: str,
    stage: Stage,
) -> tuple[str, dict[str, Any]]:
    details = dict(info)
    details.update(extras)
    details["input_path"] = str(input_path)
    details["input_lines"] = _count_non_empty_lines(input_text)
    details["input_length"] = len(input_text)

    if mode == "copy" and stage == "draft":
        details.pop("parent_gen_id", None)

    return input_text, details


def _stage_input_asset(
    *,
    data_layer: GensDataLayer,
    stage: Stage,
    gen_id: str,
    content_combinations=None,
    reuse_existing: bool = False,
) -> tuple[str, dict[str, Any]]:
    data_layer.reserve_generation(stage, gen_id, create=True)

    metadata = resolve_generation_metadata(data_layer, stage, gen_id)
    info = _base_input_info(stage, gen_id, metadata)
    input_path = data_layer.paths.input_path(stage, gen_id)

    reused = bool(reuse_existing)
    input_text: str | None = None

    if reused:
        try:
            input_text = data_layer.read_input(stage, gen_id)
        except DDError as err:
            if err.code is Err.DATA_MISSING:
                ctx = dict(err.ctx or {})
                ctx.update(
                    {
                        "stage": stage,
                        "gen_id": gen_id,
                        "artifact": "input",
                        "reason": "input_missing_for_reuse",
                    }
                )
                raise DDError(Err.DATA_MISSING, ctx=ctx, cause=err)
            raise

    if metadata.mode == "copy":
        parent_stage = effective_parent_stage(stage)
        if not metadata.parent_gen_id:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "mode": metadata.mode,
                    "reason": "missing_parent",
                },
            )
        parent_text = data_layer.read_parsed(parent_stage, metadata.parent_gen_id)

        if not reused:
            data_layer.delete_downstream_artifacts(stage, gen_id, from_stage="input")
            input_text = parent_text
            input_path = data_layer.write_input(stage, gen_id, input_text)
        elif input_text is None:
            input_text = parent_text

        extras = {
            "input_mode": "copy",
            "copied_from": str(
                data_layer.parsed_path(parent_stage, metadata.parent_gen_id).resolve()
            ),
        }
        if stage == "draft" and metadata.combo_id:
            extras["combo_id"] = metadata.combo_id

        return _finalize_input_info(
            info,
            input_text=input_text,
            input_path=input_path,
            extras=extras,
            mode=metadata.mode,
            stage=stage,
        )

    extras: dict[str, Any] = {"input_mode": "prompt"}

    if stage == "draft":
        if metadata.combo_id:
            extras["combo_id"] = metadata.combo_id
        if not reused:
            if content_combinations is None:
                raise DDError(
                    Err.INVALID_CONFIG,
                    ctx={
                        "stage": stage,
                        "gen_id": gen_id,
                        "reason": "missing_content_combinations",
                    },
                )
            combo_id = (metadata.combo_id or "").strip()
            match = _find_content_combination(content_combinations, combo_id)
            if match is None:
                raise DDError(
                    Err.DATA_MISSING,
                    ctx={
                        "stage": stage,
                        "gen_id": gen_id,
                        "combo_id": combo_id,
                        "reason": "combo_not_found",
                    },
                )
            concepts = getattr(match, "contents", None)
            if concepts is None and isinstance(match, dict):
                concepts = match.get("contents")
            if concepts is None:
                raise DDError(
                    Err.INVALID_CONFIG,
                    ctx={
                        "stage": stage,
                        "gen_id": gen_id,
                        "combo_id": combo_id,
                        "reason": "missing_contents",
                    },
                )
            template_vars = {"concepts": concepts}
        else:
            template_vars = None
    elif stage in {"essay", "evaluation"}:
        parent_stage = effective_parent_stage(stage)
        if not metadata.parent_gen_id:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "reason": "missing_parent",
                },
            )
        parent_text = data_layer.read_parsed(parent_stage, metadata.parent_gen_id)
        info["parent_gen_id"] = metadata.parent_gen_id
        if stage == "essay":
            extras["draft_line_count"] = _count_non_empty_lines(parent_text)
            template_vars = {
                "draft_block": parent_text,
                "links_block": parent_text,
            }
        else:
            template_vars = {"response": parent_text}
    else:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"stage": stage, "reason": "unsupported_stage"},
        )

    if reused and input_text is None:
        input_text = data_layer.read_input(stage, gen_id)

    if not reused:
        if template_vars is None:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "reason": "missing_template_vars",
                },
            )
        input_text = render_template(
            stage,
            metadata.template_id,
            template_vars,
            paths=data_layer.paths,
        )
        data_layer.delete_downstream_artifacts(stage, gen_id, from_stage="input")
        input_path = data_layer.write_input(stage, gen_id, input_text)

    return _finalize_input_info(
        info,
        input_text=input_text,
        input_path=input_path,
        extras=extras,
        mode=metadata.mode,
        stage=stage,
    )


def stage_input_asset(context, stage: Stage, *, content_combinations=None) -> str:
    """Produce the canonical stage input text (prompt or copy) for the next phase."""

    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    experiment_config = getattr(context.resources, "experiment_config", None)
    stage_settings = experiment_config.stage_config.get(stage) if experiment_config else None
    force = stage_settings.force if stage_settings else False
    reuse_existing = data_layer.input_exists(stage, gen_id, force=force)

    input_text, info = _stage_input_asset(
        data_layer=data_layer,
        stage=stage,
        gen_id=gen_id,
        content_combinations=content_combinations,
        reuse_existing=reuse_existing,
    )

    context.add_output_metadata(
        build_stage_artifact_metadata(
            function=f"{stage}_stage_input",
            artifact_label="input",
            metadata=info,
            text=input_text,
        )
    )
    return input_text


__all__ = ["stage_input_asset"]
