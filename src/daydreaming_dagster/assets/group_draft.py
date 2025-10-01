"""
Group: generation_draft

Dagster assets for the draft stage built on the unified stage helpers.
These assets convert Dagster context into the `_stage_*` primitives so
all IO and metadata handling stay in the shared unified layer.
"""

from __future__ import annotations

from dagster import AssetKey

from .partitions import draft_gens_partitions
from .stage_asset_helpers import (
    build_parsed_asset,
    build_prompt_asset,
    build_raw_asset,
)
from ..unified.stage_core import Stage


DRAFT_STAGE: Stage = "draft"


draft_prompt = build_prompt_asset(
    stage=DRAFT_STAGE,
    name="draft_prompt",
    docstring="Produce the canonical draft prompt text for the generation stage.",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="draft_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
    needs_content_combinations=True,
)


draft_raw = build_raw_asset(
    stage=DRAFT_STAGE,
    name="draft_raw",
    docstring="Execute the draft raw generation and persist raw outputs via the data layer.",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config", "openrouter_client"},
    prompt_input_asset_key="draft_prompt",
    prompt_input_param="draft_prompt",
)


draft_parsed = build_parsed_asset(
    stage=DRAFT_STAGE,
    name="draft_parsed",
    docstring="Parse the draft raw text into its persisted parsed representation.",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
    deps=[AssetKey("draft_raw")],
)


__all__ = [
    "draft_prompt",
    "draft_raw",
    "draft_parsed",
]
