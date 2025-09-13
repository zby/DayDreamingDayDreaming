from __future__ import annotations

from typing import Optional

from .stage_core import Stage, render_template
from .stage_policy import (
    get_stage_spec,
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
    spec = get_stage_spec(stage)
    row, _cohort = require_membership_row(context, stage, str(gen_id), require_columns=spec.prompt_fields)
    mf = read_membership_fields(row)
    mode = resolve_generator_mode(kind=stage, data_root=data_root, template_id=mf.template_id)

    if mode == "copy":
        extras = {}
        if spec.parent_stage is not None:
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

    values, extras, parent_gen_id = spec.build_prompt_values(context, str(gen_id), mf, content_combinations)

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
