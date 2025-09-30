"""
Group: generation_essays

Dagster assets for the essay stage built on the unified stage helpers.
The assets translate Dagster context into the lower-level `_stage_*` helpers,
leaving IO and metadata persistence to the shared unified implementation.
"""

from __future__ import annotations

from dagster import AssetKey

from ._decorators import asset_with_boundary
from .partitions import essay_gens_partitions
from ._helpers import build_stage_artifact_metadata, get_run_id
from ..data_layer.gens_data_layer import GensDataLayer, resolve_generation_metadata
from ..unified.stage_core import Stage, resolve_parser_name
from ..unified.stage_inputs import _stage_input_asset
from ..unified.stage_raw import _stage_raw_asset
from ..unified.stage_parsed import _stage_parsed_asset
from ..utils.errors import DDError, Err


ESSAY_STAGE: Stage = "essay"


@asset_with_boundary(
    stage="essay",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
)
def essay_prompt(context) -> str:
    """Produce the canonical essay prompt/input text via the gens data layer."""
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    input_text, info = _stage_input_asset(
        data_layer=data_layer,
        stage=ESSAY_STAGE,
        gen_id=gen_id,
    )

    context.add_output_metadata(
        build_stage_artifact_metadata(
            function="essay_stage_input",
            artifact_label="input",
            metadata=info,
            text=input_text,
        )
    )
    return input_text


@asset_with_boundary(
    stage="essay_raw",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config", "openrouter_client"},
)
def essay_raw(context, essay_prompt: str) -> str:
    """Execute the essay raw generation and persist raw artifacts via the data layer."""
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    experiment_config = getattr(context.resources, "experiment_config", None)
    stage_settings = experiment_config.stage_config.get(ESSAY_STAGE) if experiment_config else None
    llm_client = getattr(context.resources, "openrouter_client", None)
    run_id = get_run_id(context)

    raw_text, raw_metadata = _stage_raw_asset(
        data_layer=data_layer,
        stage=ESSAY_STAGE,
        gen_id=gen_id,
        prompt_text=essay_prompt,
        llm_client=llm_client,
        stage_settings=stage_settings,
        run_id=run_id,
    )

    context.add_output_metadata(
        build_stage_artifact_metadata(
            function="essay_raw",
            artifact_label="raw",
            metadata=raw_metadata,
            text=raw_text,
        )
    )
    return raw_text


@asset_with_boundary(
    stage="essay_parsed",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
    deps={AssetKey("essay_raw")},
)
def essay_parsed(context) -> str:
    """Parse the essay raw text into its persisted parsed representation."""
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    # Reload persisted raw output instead of relying on in-memory handoff.
    raw_text = data_layer.read_raw(ESSAY_STAGE, gen_id)

    metadata = resolve_generation_metadata(data_layer, ESSAY_STAGE, gen_id)
    try:
        raw_metadata = data_layer.read_raw_metadata(ESSAY_STAGE, gen_id)
    except DDError as err:
        if err.code is Err.DATA_MISSING:
            raw_metadata = {}
        else:
            raise

    parser_name = resolve_parser_name(
        data_layer.data_root,
        ESSAY_STAGE,
        metadata.template_id,
        None,
    )

    experiment_config = getattr(context.resources, "experiment_config", None)
    stage_settings = experiment_config.stage_config.get(ESSAY_STAGE) if experiment_config else None
    min_lines_override = stage_settings.min_lines if stage_settings else None

    parsed_text, parsed_metadata = _stage_parsed_asset(
        data_layer=data_layer,
        stage=ESSAY_STAGE,
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
        data_layer.write_parsed_metadata(ESSAY_STAGE, gen_id, parsed_metadata)

    context.add_output_metadata(
        build_stage_artifact_metadata(
            function="essay_parsed",
            artifact_label="parsed",
            metadata=parsed_metadata,
            text=parsed_text,
        )
    )
    return parsed_text


__all__ = [
    "essay_prompt",
    "essay_raw",
    "essay_parsed",
]
