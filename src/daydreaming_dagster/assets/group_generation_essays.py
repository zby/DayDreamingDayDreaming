"""
Group: generation_essays

Asset registrations for the essay (Phase‑2) generation stage.
Logic is implemented in unified.stage_services asset‑style entrypoints.
"""

from ._decorators import asset_with_boundary
from .partitions import essay_gens_partitions
from ..unified.stage_services import prompt_asset, essay_response_asset


@asset_with_boundary(
    stage="essay",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
)
def essay_prompt(context) -> str:
    """Dagster asset wrapper for essay prompt.

    Delegates to unified.stage_services.prompt_asset(stage="essay").
    """
    return prompt_asset(context, "essay")


@asset_with_boundary(
    stage="essay",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def essay_response(context, essay_prompt) -> str:
    """Dagster asset wrapper for essay response.

    Delegates to unified.stage_services.essay_response_asset.
    """
    return essay_response_asset(context, essay_prompt)
