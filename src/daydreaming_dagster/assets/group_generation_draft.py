"""
Group: generation_draft

Asset registrations for the draft (Phase‑1) generation stage.
Logic is implemented in unified.stage_services asset‑style entrypoints.
"""

from ._decorators import asset_with_boundary
from .partitions import draft_gens_partitions
from ..unified.stage_services import prompt_asset, draft_response_asset


@asset_with_boundary(
    stage="draft",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="draft_prompt_io_manager",
    required_resource_keys={"membership_service"},
)
def draft_prompt(context, content_combinations) -> str:
    """Dagster asset wrapper for draft prompt.

    Delegates to unified.stage_services.prompt_asset(stage="draft").
    """
    return prompt_asset(context, "draft", content_combinations=content_combinations)


@asset_with_boundary(
    stage="draft",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="draft_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root", "membership_service"},
)
def draft_response(context, draft_prompt) -> str:
    """Dagster asset wrapper for draft response.

    Delegates to unified.stage_services.draft_response_asset.
    """
    return draft_response_asset(context, draft_prompt)
