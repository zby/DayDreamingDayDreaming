from __future__ import annotations

from typing import Optional

from dagster import Failure, MetadataValue

from .stage_core import Stage, render_template
from daydreaming_dagster.config.paths import Paths
from .stage_policy import (
    get_stage_spec,
    read_membership_fields,
    resolve_generator_mode,
)


def prompt_asset(context, stage: Stage, *, content_combinations=None) -> str:
    gen_id = context.partition_key
    from daydreaming_dagster.assets._helpers import (
        load_parent_parsed_text,
        build_prompt_metadata,
    )
    paths = Paths.from_context(context)
    data_root = paths.data_root
    spec = get_stage_spec(stage)
    # DI-first: use injected membership service when available; otherwise create one on the fly
    svc = getattr(getattr(context, "resources", object()), "membership_service", None)
    if not (svc and hasattr(svc, "require_row")):
        from daydreaming_dagster.resources.membership_service import MembershipServiceResource
        svc = MembershipServiceResource()
    row, _cohort = svc.require_row(data_root, stage, str(gen_id), require_columns=spec.prompt_fields)
    mf = read_membership_fields(row)
    mode = resolve_generator_mode(kind=stage, data_root=data_root, template_id=mf.template_id)

    if mode == "copy":
        parent_stage = spec.parent_stage
        parent_gen = str(mf.parent_gen_id or "").strip()
        if not parent_stage or not parent_gen:
            raise Failure(
                description="COPY mode requires a valid parent_gen_id",
                metadata={
                    "stage": MetadataValue.text(str(stage)),
                    "gen_id": MetadataValue.text(str(gen_id)),
                },
            )
        resolved_parent_gen, parent_text = load_parent_parsed_text(
            context,
            stage,
            str(gen_id),
            failure_fn_name="prompt_asset.copy_mode",
        )
        parent_gen = resolved_parent_gen or parent_gen
        extras = {
            "parent_gen_id": parent_gen,
            "input_mode": "copy",
            "copied_from": str(Paths.from_context(context).parsed_path(parent_stage, parent_gen).resolve()),
        }
        if stage == "draft":
            extras["combo_id"] = str(mf.combo_id or "")
        context.add_output_metadata(
            build_prompt_metadata(
                context,
                stage=stage,
                gen_id=str(gen_id),
                template_id=mf.template_id,
                mode=mode,
                parent_gen_id=parent_gen if stage != "draft" else None,
                prompt_text=parent_text,
                extras=extras,
            )
        )
        return parent_text

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
            extras={"input_mode": "prompt", **extras},
        )
    )
    return prompt


__all__ = ["prompt_asset"]
