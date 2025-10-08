"""
Group: evaluation

Dagster assets for the evaluation stage that delegate to the unified stage
helpers. These keep Dagster wiring focused on translating context to the
shared `_stage_*` primitives while the unified layer manages IO.
"""

from __future__ import annotations

from dagster import AssetKey

from .partitions import evaluation_gens_partitions
from .raw_data import EVALUATION_TEMPLATES_KEY
from .stage_asset_helpers import (
    build_parsed_asset,
    build_prompt_asset,
    build_raw_asset,
)
from ..unified.stage_core import Stage


EVALUATION_STAGE: Stage = "evaluation"


evaluation_prompt = build_prompt_asset(
    stage=EVALUATION_STAGE,
    name="evaluation_prompt",
    docstring="Produce the evaluation prompt text via the gens data layer.",
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
    deps=[EVALUATION_TEMPLATES_KEY],
)


evaluation_raw = build_raw_asset(
    stage=EVALUATION_STAGE,
    name="evaluation_raw",
    docstring="Execute evaluation raw generation and persist raw assets via the data layer.",
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"openrouter_client", "data_root", "experiment_config"},
    prompt_input_asset_key="evaluation_prompt",
    prompt_input_param="evaluation_prompt",
)


evaluation_parsed = build_parsed_asset(
    stage=EVALUATION_STAGE,
    name="evaluation_parsed",
    docstring="Parse evaluation raw text into the persisted parsed artifact.",
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
    deps=[AssetKey("evaluation_raw")],
)


__all__ = [
    "evaluation_prompt",
    "evaluation_raw",
    "evaluation_parsed",
]
