from __future__ import annotations

from pathlib import Path

from .stage_core import Stage, execute_llm, execute_copy
from daydreaming_dagster.config.paths import Paths
from .stage_policy import get_stage_spec, read_membership_fields, resolve_generator_mode
from .envelopes import GenerationEnvelope


def response_asset(context, prompt_text, stage: Stage) -> str:
    gen_id = context.partition_key
    if not isinstance(prompt_text, str) or not prompt_text.strip():
        raise ValueError(f"Upstream {stage}_prompt is missing or empty")

    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        emit_standard_output_metadata,
        get_run_id,
        get_data_root,
    )

    data_root = get_data_root(context)
    spec = get_stage_spec(stage)
    row, cohort = require_membership_row(context, stage, str(gen_id), require_columns=spec.response_fields)
    mf = read_membership_fields(row)
    mode = resolve_generator_mode(kind=stage, data_root=data_root, template_id=mf.template_id)
    envelope = GenerationEnvelope(stage=stage, gen_id=str(gen_id), template_id=mf.template_id, parent_gen_id=mf.parent_gen_id, llm_model_id=mf.llm_model_id, mode=mode)
    envelope.validate(spec)

    if envelope.mode == "copy":
        result = execute_copy(
            out_dir=data_root / "gens",
            stage=stage,
            gen_id=str(gen_id),
            template_id=envelope.template_id,
            parent_gen_id=str(envelope.parent_gen_id or ""),
            pass_through_from=Paths.from_str(str(data_root)).parsed_path(
                (spec.parent_stage or ""), str(envelope.parent_gen_id or "")
            ),
            metadata_extra={
                "function": f"{stage}_response",
                "run_id": get_run_id(context),
                **({"cohort_id": str(cohort)} if isinstance(cohort, str) and cohort else {}),
            },
        )
        emit_standard_output_metadata(
            context,
            function=f"{stage}_response",
            gen_id=str(gen_id),
            result=result,
            extras={"mode": "copy", "parent_gen_id": str(envelope.parent_gen_id or "")},
        )
        return result.parsed_text or ""

    max_tokens, min_lines = spec.tokens_and_min_lines(context)
    result = execute_llm(
        stage=stage,
        llm=context.resources.openrouter_client,
        root_dir=data_root,
        gen_id=str(gen_id),
        template_id=envelope.template_id,
        prompt_text=str(prompt_text),
        model=str(envelope.llm_model_id or ""),
        max_tokens=max_tokens,
        min_lines=min_lines,
        parent_gen_id=str(envelope.parent_gen_id) if envelope.parent_gen_id else None,
        metadata_extra={
            "function": f"{stage}_response",
            "run_id": get_run_id(context),
            **({"cohort_id": str(cohort)} if isinstance(cohort, str) and cohort else {}),
        },
    )
    emit_standard_output_metadata(context, function=f"{stage}_response", gen_id=str(gen_id), result=result)
    context.log.info(f"Generated {stage} response for gen {gen_id}")
    return result.raw_text or ""


def essay_response_asset(context, essay_prompt) -> str:
    return response_asset(context, essay_prompt, "essay")


def evaluation_response_asset(context, evaluation_prompt) -> str:
    return response_asset(context, evaluation_prompt, "evaluation")


def draft_response_asset(context, draft_prompt) -> str:
    return response_asset(context, draft_prompt, "draft")


__all__ = [
    "response_asset",
    "essay_response_asset",
    "evaluation_response_asset",
    "draft_response_asset",
]
