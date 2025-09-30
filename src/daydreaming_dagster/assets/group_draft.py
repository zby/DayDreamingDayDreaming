"""
Group: generation_draft

Dagster assets for the draft stage built on the unified stage helpers.
These assets convert Dagster context into the `_stage_*` primitives so
all IO and metadata handling stay in the shared unified layer.
"""

from __future__ import annotations

from dagster import AssetKey

from ._decorators import asset_with_boundary
from .partitions import draft_gens_partitions
from ._helpers import build_stage_artifact_metadata, get_run_id
from ..data_layer.gens_data_layer import GensDataLayer, resolve_generation_metadata
from ..unified.stage_core import Stage, resolve_parser_name
from ..unified.stage_inputs import _stage_input_asset
from ..unified.stage_raw import _stage_raw_asset
from ..unified.stage_parsed import _stage_parsed_asset
from ..utils.errors import DDError, Err


DRAFT_STAGE: Stage = "draft"


@asset_with_boundary(
    stage="draft",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="draft_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
)
def draft_prompt(context, content_combinations) -> str:
    """Produce the canonical draft prompt text for the generation stage."""
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    input_text, info = _stage_input_asset(
        data_layer=data_layer,
        stage=DRAFT_STAGE,
        gen_id=gen_id,
        content_combinations=content_combinations,
    )

    context.add_output_metadata(
        build_stage_artifact_metadata(
            function="draft_stage_input",
            artifact_label="input",
            metadata=info,
            text=input_text,
        )
    )
    return input_text


@asset_with_boundary(
    stage="draft_raw",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config", "openrouter_client"},
)
def draft_raw(context, draft_prompt: str) -> str:
    """Execute the draft raw generation and persist raw outputs via the data layer."""
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    experiment_config = getattr(context.resources, "experiment_config", None)
    stage_settings = experiment_config.stage_config.get(DRAFT_STAGE) if experiment_config else None
    llm_client = getattr(context.resources, "openrouter_client", None)
    run_id = get_run_id(context)

    raw_text, raw_metadata = _stage_raw_asset(
        data_layer=data_layer,
        stage=DRAFT_STAGE,
        gen_id=gen_id,
        prompt_text=draft_prompt,
        llm_client=llm_client,
        stage_settings=stage_settings,
        run_id=run_id,
    )

    context.add_output_metadata(
        build_stage_artifact_metadata(
            function="draft_raw",
            artifact_label="raw",
            metadata=raw_metadata,
            text=raw_text,
        )
    )
    return raw_text


@asset_with_boundary(
    stage="draft_parsed",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
    deps={AssetKey("draft_raw")},
)
def draft_parsed(context) -> str:
    """Parse the draft raw text into its persisted parsed representation."""
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    # Re-read the persisted raw output to support multi-process execution.
    raw_text = data_layer.read_raw(DRAFT_STAGE, gen_id)

    metadata = resolve_generation_metadata(data_layer, DRAFT_STAGE, gen_id)
    try:
        raw_metadata = data_layer.read_raw_metadata(DRAFT_STAGE, gen_id)
    except DDError as err:
        if err.code is Err.DATA_MISSING:
            raw_metadata = {}
        else:
            raise

    parser_name = resolve_parser_name(
        data_layer.data_root,
        DRAFT_STAGE,
        metadata.template_id,
        None,
    )

    experiment_config = getattr(context.resources, "experiment_config", None)
    stage_settings = experiment_config.stage_config.get(DRAFT_STAGE) if experiment_config else None
    min_lines_override = stage_settings.min_lines if stage_settings else None

    parsed_text, parsed_metadata = _stage_parsed_asset(
        data_layer=data_layer,
        stage=DRAFT_STAGE,
        gen_id=gen_id,
        raw_text=raw_text,
        parser_name=parser_name,
        raw_metadata=raw_metadata,
        stage_settings=stage_settings,
        min_lines_override=min_lines_override,
        fail_on_truncation=True,
    )

    run_id = get_run_id(context)
    if run_id:
        parsed_metadata["run_id"] = run_id
        data_layer.write_parsed_metadata(DRAFT_STAGE, gen_id, parsed_metadata)

    context.add_output_metadata(
        build_stage_artifact_metadata(
            function="draft_parsed",
            artifact_label="parsed",
            metadata=parsed_metadata,
            text=parsed_text,
        )
    )
    return parsed_text


__all__ = [
    "draft_prompt",
    "draft_raw",
    "draft_parsed",
]
