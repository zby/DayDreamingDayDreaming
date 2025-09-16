"""
Experimental raw generation assets for draft, essay, and evaluation stages.

These assets persist raw.txt and raw_metadata.json while leaving parsed output to
future parsed-stage assets. They currently coexist with the legacy response
assets and are not yet wired into the main Definitions bundle.
"""

from __future__ import annotations

from typing import Dict, Optional

from dagster import Failure, MetadataValue

from ._decorators import asset_with_boundary
from .partitions import draft_gens_partitions, essay_gens_partitions, evaluation_gens_partitions
from ..config.paths import Paths
from ..unified.envelopes import GenerationEnvelope
from ..unified.stage_policy import (
    get_stage_spec,
    read_membership_fields,
    resolve_generator_mode,
)
from ..unified.raw_generation import (
    RawGenerationResult,
    perform_llm_raw_generation,
    perform_copy_raw_generation,
)
from ..assets._helpers import get_run_id


def _membership_service(context):
    svc = getattr(getattr(context, "resources", object()), "membership_service", None)
    if not (svc and hasattr(svc, "require_row")):
        from daydreaming_dagster.resources.membership_service import MembershipServiceResource
        svc = MembershipServiceResource()
    return svc


def _raw_metadata_extras(*, cohort: Optional[str], replicate: Optional[int], combo_id: Optional[str]) -> Dict[str, Optional[str]]:
    extras: Dict[str, Optional[str]] = {}
    if cohort:
        extras["cohort_id"] = str(cohort)
    if replicate is not None:
        extras["replicate"] = int(replicate)
    if combo_id:
        extras["combo_id"] = combo_id
    return extras


@asset_with_boundary(
    stage="draft_raw",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="draft_raw_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def draft_raw(context, draft_prompt) -> RawGenerationResult:
    paths = Paths.from_context(context)
    data_root = paths.data_root
    svc = _membership_service(context)
    spec = get_stage_spec("draft")
    row, cohort = svc.require_row(data_root, "draft", str(context.partition_key), require_columns=spec.response_fields)
    mf = read_membership_fields(row)

    replicate_val = None
    try:
        replicate_val = int(row.get("replicate")) if hasattr(row, "index") and ("replicate" in row.index) else None
    except Exception:
        replicate_val = None

    mode = resolve_generator_mode(kind="draft", data_root=data_root, template_id=mf.template_id)
    envelope = GenerationEnvelope(stage="draft", gen_id=str(context.partition_key), template_id=mf.template_id, parent_gen_id=mf.parent_gen_id, llm_model_id=mf.llm_model_id, mode=mode)
    envelope.validate(spec)

    base_extras = {"function": "draft_raw", "run_id": get_run_id(context)}
    base_extras.update(_raw_metadata_extras(cohort=cohort, replicate=replicate_val, combo_id=mf.combo_id))

    max_tokens, _ = spec.tokens_and_min_lines(context)

    if envelope.mode == "copy":
        parent_stage = spec.parent_stage
        if not parent_stage or not envelope.parent_gen_id:
            raise Failure(description="draft copy-mode requires a valid parent_gen_id", metadata={"gen_id": MetadataValue.text(str(context.partition_key))})
        copy_extras = dict(base_extras)
        copy_extras["input_mode"] = "copy"
        copy_extras["copied_from"] = str(paths.parsed_path(parent_stage, envelope.parent_gen_id).resolve())
        result = perform_copy_raw_generation(
            stage="draft",
            data_root=data_root,
            gen_id=str(context.partition_key),
            copy_text=str(draft_prompt or ""),
            metadata_extras=copy_extras,
        )
    else:
        llm_extras = dict(base_extras)
        llm_extras["input_mode"] = "prompt"
        result = perform_llm_raw_generation(
            stage="draft",
            llm_client=context.resources.openrouter_client,
            data_root=data_root,
            gen_id=str(context.partition_key),
            template_id=envelope.template_id,
            prompt_text=str(draft_prompt),
            llm_model_id=envelope.llm_model_id,
            max_tokens=max_tokens,
            metadata_extras=llm_extras,
        )

    raw_path = paths.raw_path("draft", str(context.partition_key))
    context.add_output_metadata(
        {
            "function": MetadataValue.text("draft_raw"),
            "gen_id": MetadataValue.text(str(context.partition_key)),
            "raw_path": MetadataValue.path(str(raw_path)),
            "raw_metadata": MetadataValue.json(result.raw_metadata),
        }
    )
    return result


@asset_with_boundary(
    stage="essay_raw",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_raw_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def essay_raw(context, essay_prompt) -> RawGenerationResult:
    paths = Paths.from_context(context)
    data_root = paths.data_root
    svc = _membership_service(context)
    spec = get_stage_spec("essay")
    row, cohort = svc.require_row(data_root, "essay", str(context.partition_key), require_columns=spec.response_fields)
    mf = read_membership_fields(row)

    replicate_val = None
    try:
        replicate_val = int(row.get("replicate")) if hasattr(row, "index") and ("replicate" in row.index) else None
    except Exception:
        replicate_val = None

    mode = resolve_generator_mode(kind="essay", data_root=data_root, template_id=mf.template_id)
    envelope = GenerationEnvelope(stage="essay", gen_id=str(context.partition_key), template_id=mf.template_id, parent_gen_id=mf.parent_gen_id, llm_model_id=mf.llm_model_id, mode=mode)
    envelope.validate(spec)

    base_extras = {"function": "essay_raw", "run_id": get_run_id(context)}
    base_extras.update(_raw_metadata_extras(cohort=cohort, replicate=replicate_val, combo_id=mf.combo_id))

    max_tokens, _ = spec.tokens_and_min_lines(context)

    if envelope.mode == "copy":
        parent_stage = spec.parent_stage
        if not parent_stage or not envelope.parent_gen_id:
            raise Failure(description="essay copy-mode requires a valid parent_gen_id", metadata={"gen_id": MetadataValue.text(str(context.partition_key))})
        copy_extras = dict(base_extras)
        copy_extras["input_mode"] = "copy"
        copy_extras["copied_from"] = str(paths.parsed_path(parent_stage, envelope.parent_gen_id).resolve())
        result = perform_copy_raw_generation(
            stage="essay",
            data_root=data_root,
            gen_id=str(context.partition_key),
            copy_text=str(essay_prompt or ""),
            metadata_extras=copy_extras,
        )
    else:
        llm_extras = dict(base_extras)
        llm_extras["input_mode"] = "prompt"
        result = perform_llm_raw_generation(
            stage="essay",
            llm_client=context.resources.openrouter_client,
            data_root=data_root,
            gen_id=str(context.partition_key),
            template_id=envelope.template_id,
            prompt_text=str(essay_prompt),
            llm_model_id=envelope.llm_model_id,
            max_tokens=max_tokens,
            metadata_extras=llm_extras,
        )

    raw_path = paths.raw_path("essay", str(context.partition_key))
    context.add_output_metadata(
        {
            "function": MetadataValue.text("essay_raw"),
            "gen_id": MetadataValue.text(str(context.partition_key)),
            "raw_path": MetadataValue.path(str(raw_path)),
            "raw_metadata": MetadataValue.json(result.raw_metadata),
        }
    )
    return result


@asset_with_boundary(
    stage="evaluation_raw",
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_raw_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def evaluation_raw(context, evaluation_prompt) -> RawGenerationResult:
    paths = Paths.from_context(context)
    data_root = paths.data_root
    svc = _membership_service(context)
    spec = get_stage_spec("evaluation")
    row, cohort = svc.require_row(data_root, "evaluation", str(context.partition_key), require_columns=spec.response_fields)
    mf = read_membership_fields(row)

    replicate_val = None
    try:
        replicate_val = int(row.get("replicate")) if hasattr(row, "index") and ("replicate" in row.index) else None
    except Exception:
        replicate_val = None

    mode = resolve_generator_mode(kind="evaluation", data_root=data_root, template_id=mf.template_id)
    envelope = GenerationEnvelope(stage="evaluation", gen_id=str(context.partition_key), template_id=mf.template_id, parent_gen_id=mf.parent_gen_id, llm_model_id=mf.llm_model_id, mode=mode)
    envelope.validate(spec)

    base_extras = {"function": "evaluation_raw", "run_id": get_run_id(context)}
    base_extras.update(_raw_metadata_extras(cohort=cohort, replicate=replicate_val, combo_id=mf.combo_id))

    max_tokens, _ = spec.tokens_and_min_lines(context)

    if envelope.mode == "copy":
        parent_stage = spec.parent_stage
        if not parent_stage or not envelope.parent_gen_id:
            raise Failure(description="evaluation copy-mode requires a valid parent_gen_id", metadata={"gen_id": MetadataValue.text(str(context.partition_key))})
        copy_extras = dict(base_extras)
        copy_extras["input_mode"] = "copy"
        copy_extras["copied_from"] = str(paths.parsed_path(parent_stage, envelope.parent_gen_id).resolve())
        result = perform_copy_raw_generation(
            stage="evaluation",
            data_root=data_root,
            gen_id=str(context.partition_key),
            copy_text=str(evaluation_prompt or ""),
            metadata_extras=copy_extras,
        )
    else:
        llm_extras = dict(base_extras)
        llm_extras["input_mode"] = "prompt"
        result = perform_llm_raw_generation(
            stage="evaluation",
            llm_client=context.resources.openrouter_client,
            data_root=data_root,
            gen_id=str(context.partition_key),
            template_id=envelope.template_id,
            prompt_text=str(evaluation_prompt),
            llm_model_id=envelope.llm_model_id,
            max_tokens=max_tokens,
            metadata_extras=llm_extras,
        )

    raw_path = paths.raw_path("evaluation", str(context.partition_key))
    context.add_output_metadata(
        {
            "function": MetadataValue.text("evaluation_raw"),
            "gen_id": MetadataValue.text(str(context.partition_key)),
            "raw_path": MetadataValue.path(str(raw_path)),
            "raw_metadata": MetadataValue.json(result.raw_metadata),
        }
    )
    return result


__all__ = [
    "draft_raw",
    "essay_raw",
    "evaluation_raw",
]
