from __future__ import annotations

from typing import Any, Iterable

from dagster import Failure, MetadataValue

from daydreaming_dagster.assets._helpers import build_stage_artifact_metadata
from daydreaming_dagster.data_layer.gens_data_layer import (
    GensDataLayer,
    resolve_generation_metadata,
)
from .stage_core import Stage, render_template, effective_parent_stage


def _count_non_empty_lines(text: str) -> int:
    return sum(1 for line in text.splitlines() if line.strip())


def _find_content_combination(content_combinations: Iterable[Any], combo_id: str) -> Any | None:
    for combo in content_combinations:
        candidate = getattr(combo, "combo_id", None)
        if candidate is None and isinstance(combo, dict):
            candidate = combo.get("combo_id")
        if str(candidate or "").strip() == combo_id:
            return combo
    return None


def _stage_input_asset(
    *,
    data_layer: GensDataLayer,
    stage: Stage,
    gen_id: str,
    content_combinations=None,
) -> tuple[str, dict[str, Any]]:
    data_layer.reserve_generation(stage, gen_id, create=True)

    metadata = resolve_generation_metadata(data_layer, stage, gen_id)

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

    extras: dict[str, Any] = {}

    if metadata.mode == "copy":
        parent_stage = effective_parent_stage(stage)
        if not metadata.parent_gen_id:
            raise ValueError("copy mode requires parent_gen_id")
        input_text = data_layer.read_parsed(parent_stage, metadata.parent_gen_id)
        input_path = data_layer.write_input(stage, gen_id, input_text)

        extras.update(
            {
                "input_mode": "copy",
                "copied_from": str(
                    data_layer.parsed_path(parent_stage, metadata.parent_gen_id).resolve()
                ),
            }
        )
        if stage == "draft" and metadata.combo_id:
            extras["combo_id"] = metadata.combo_id
    else:
        extras["input_mode"] = "prompt"
        if stage == "draft":
            if content_combinations is None:
                raise ValueError("draft stage_input_asset requires content_combinations")
            combo_id = (metadata.combo_id or "").strip()
            match = _find_content_combination(content_combinations, combo_id)
            if match is None:
                raise ValueError(f"content combination '{combo_id}' missing for draft/{gen_id}")
            concepts = getattr(match, "contents", None)
            if concepts is None and isinstance(match, dict):
                concepts = match.get("contents")
            if concepts is None:
                raise ValueError("content combination missing 'contents'")
            template_vars = {"concepts": concepts}
            if metadata.combo_id:
                extras["combo_id"] = metadata.combo_id
        elif stage == "essay":
            parent_stage = effective_parent_stage(stage)
            if not metadata.parent_gen_id:
                raise ValueError("essay stage_input_asset requires parent_gen_id")
            parent_text = data_layer.read_parsed(parent_stage, metadata.parent_gen_id)
            template_vars = {
                "draft_block": parent_text,
                "links_block": parent_text,
            }
            extras["draft_line_count"] = _count_non_empty_lines(parent_text)
            info["parent_gen_id"] = metadata.parent_gen_id
        elif stage == "evaluation":
            parent_stage = effective_parent_stage(stage)
            if not metadata.parent_gen_id:
                raise ValueError("evaluation stage_input_asset requires parent_gen_id")
            parent_text = data_layer.read_parsed(parent_stage, metadata.parent_gen_id)
            template_vars = {"response": parent_text}
            info["parent_gen_id"] = metadata.parent_gen_id
        else:
            raise ValueError(f"Unsupported stage: {stage}")

        input_text = render_template(stage, metadata.template_id, template_vars)
        input_path = data_layer.write_input(stage, gen_id, input_text)

    info["input_path"] = str(input_path)
    info["input_lines"] = _count_non_empty_lines(input_text)
    info["input_length"] = len(input_text)
    for key, value in extras.items():
        info[key] = value

    if metadata.mode == "copy" and stage == "draft":
        info.pop("parent_gen_id", None)

    return input_text, info


def stage_input_asset(context, stage: Stage, *, content_combinations=None) -> str:
    """Produce the canonical stage input text (prompt or copy) for the next phase."""

    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    input_text, info = _stage_input_asset(
        data_layer=data_layer,
        stage=stage,
        gen_id=gen_id,
        content_combinations=content_combinations,
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
