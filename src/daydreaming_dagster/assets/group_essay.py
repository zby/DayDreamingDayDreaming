"""
Group: generation_essays

Dagster assets for the essay stage built on the unified stage helpers.
The assets translate Dagster context into the lower-level `_stage_*` helpers,
leaving IO and metadata persistence to the shared unified implementation.
"""

from __future__ import annotations

from dagster import AssetKey

from .partitions import essay_gens_partitions
from .stage_asset_helpers import (
    build_parsed_asset,
    build_prompt_asset,
    build_raw_asset,
)
from ..unified.stage_core import Stage


ESSAY_STAGE: Stage = "essay"


essay_prompt = build_prompt_asset(
    stage=ESSAY_STAGE,
    name="essay_prompt",
    docstring="Produce the canonical essay prompt/input text via the gens data layer.",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
)


essay_raw = build_raw_asset(
    stage=ESSAY_STAGE,
    name="essay_raw",
    docstring="Execute the essay raw generation and persist raw artifacts via the data layer.",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config", "openrouter_client"},
    prompt_input_asset_key="essay_prompt",
    prompt_input_param="essay_prompt",
)


essay_parsed = build_parsed_asset(
    stage=ESSAY_STAGE,
    name="essay_parsed",
    docstring="Parse the essay raw text into its persisted parsed representation.",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
    deps=[AssetKey("essay_raw")],
)


__all__ = [
    "essay_prompt",
    "essay_raw",
    "essay_parsed",
]
