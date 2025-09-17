"""
Group: evaluation

Asset registrations for the evaluation stage. Logic is implemented in
unified.stage_services asset‑style entrypoints.
"""

from ._decorators import asset_with_boundary
from .partitions import evaluation_gens_partitions
from .raw_data import EVALUATION_TEMPLATES_KEY
from ..unified.stage_services import stage_input_asset, evaluation_response_asset


@asset_with_boundary(
    stage="evaluation",
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"data_root"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def evaluation_prompt(context) -> str:
    """Dagster asset wrapper for evaluation prompt.

    Delegates to unified.stage_services.stage_input_asset(stage="evaluation").
    """
    return stage_input_asset(context, "evaluation")


@asset_with_boundary(
    stage="evaluation",
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_response_io_manager",
    required_resource_keys={"openrouter_client", "data_root", "experiment_config"},
    deps=["evaluation_prompt"],
)
def evaluation_response(context, evaluation_prompt) -> str:
    """Dagster asset wrapper for evaluation response.

    Delegates to unified.stage_services.evaluation_response_asset.
    """
    return evaluation_response_asset(context, evaluation_prompt)
