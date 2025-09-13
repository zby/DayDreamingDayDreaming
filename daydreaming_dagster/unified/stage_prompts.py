from __future__ import annotations

from pathlib import Path
from typing import Optional

from dagster import Failure, MetadataValue

from .stage_core import Stage, render_template
from .stage_policy import (
    PROMPT_REQUIRED_BY_STAGE,
    read_membership_fields,
)


def prompt_asset(context, stage: Stage, *, content_combinations=None) -> str:
    gen_id = context.partition_key
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        resolve_generator_mode,
        load_parent_parsed_text,
        build_prompt_metadata,
        get_data_root,
    )

    data_root = get_data_root(context)
    if stage not in PROMPT_REQUIRED_BY_STAGE:
        raise Failure(description=f"Unsupported stage for prompt: {stage}")

    row, _cohort = require_membership_row(context, stage, str(gen_id), require_columns=PROMPT_REQUIRED_BY_STAGE[stage])
    mf = read_membership_fields(row)
    mode = resolve_generator_mode(kind=stage, data_root=data_root, template_id=mf.template_id)

    if mode == "copy":
        extras = {}
        if stage in ("essay", "evaluation"):
            extras["parent_gen_id"] = str(mf.parent_gen_id or "")
        if stage == "draft":
            extras["combo_id"] = str(mf.combo_id or "")
        context.add_output_metadata(
            build_prompt_metadata(
                context,
                stage=stage,
                gen_id=str(gen_id),
                template_id=mf.template_id,
                mode=mode,
                parent_gen_id=str(mf.parent_gen_id or "") if stage != "draft" else None,
                prompt_text=None,
                extras=extras,
            )
        )
        return "COPY_MODE: no prompt needed"

    values = {}
    extras = {}
    parent_gen_id: Optional[str] = None
    if stage == "draft":
        if content_combinations is None:
            raise Failure(
                description="content_combinations is required for draft prompts",
                metadata={
                    "function": MetadataValue.text("draft_prompt"),
                    "gen_id": MetadataValue.text(str(gen_id)),
                },
            )
        combo_id = str(mf.combo_id or "")
        content_combination = next((c for c in content_combinations if getattr(c, "combo_id", None) == combo_id), None)
        if content_combination is None:
            raise Failure(
                description=f"Content combination '{combo_id}' not found in combinations database",
                metadata={
                    "function": MetadataValue.text("draft_prompt"),
                    "combo_id": MetadataValue.text(combo_id),
                    "total_combinations": MetadataValue.int(len(content_combinations) if content_combinations else 0),
                },
            )
        values = {"concepts": content_combination.contents}
        extras["combo_id"] = combo_id
    elif stage == "essay":
        parent_gen_id, parent_text = load_parent_parsed_text(context, stage, str(gen_id), failure_fn_name="essay_prompt")
        values = {"draft_block": parent_text, "links_block": parent_text}
        extras["draft_line_count"] = sum(1 for ln in parent_text.splitlines() if ln.strip())
    else:
        parent_gen_id, parent_text = load_parent_parsed_text(context, stage, str(gen_id), failure_fn_name="evaluation_prompt")
        values = {"response": parent_text}

    prompt = render_template(stage, mf.template_id, values)
    context.add_output_metadata(
        build_prompt_metadata(
            context,
            stage=stage,
            gen_id=str(gen_id),
            template_id=mf.template_id,
            mode="llm",
            parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
            prompt_text=prompt,
            extras=extras,
        )
    )
    return prompt


__all__ = ["prompt_asset"]

