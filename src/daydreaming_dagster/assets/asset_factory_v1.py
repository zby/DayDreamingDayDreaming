from __future__ import annotations

"""
Narrow asset factory that wraps existing stage_services.execute_* functions.

This keeps the original callable's signature visible to Dagster via
functools.wraps, adds the centralized error boundary, and registers the
asset with the provided name/group and any additional @asset kwargs.

Stage 1 only provides this for future adoption; existing assets do not
switch to the factory yet to avoid churn.
"""

from functools import wraps
import dagster as dg

from ._error_boundary import with_asset_error_boundary


def make_asset_from(
    *,
    execute_fn,
    name: str,
    group_name: str,
    stage: str,
    **asset_kwargs,
):
    """
    Build a Dagster asset that delegates to an existing execute_* function.

    - Preserves the original signature for Dagster by using functools.wraps
    - Returns exactly whatever the execute_* function returns
    - Adds a centralized error boundary for consistent failure surfacing

    Example:

        from daydreaming_dagster.unified.stage_services import execute_draft_llm
        draft_generation = make_asset_from(
            execute_fn=execute_draft_llm,
            name="draft_generation",
            group_name="generation_draft",
            stage="draft",
            partitions_def=draft_gens_partitions,
            io_manager_key="draft_response_io_manager",
            required_resource_keys={"openrouter_client", "data_root", "experiment_config"},
        )
    """

    @wraps(execute_fn)
    def _asset(*args, **kwargs):
        return execute_fn(*args, **kwargs)

    # Apply boundary first so @asset can see the original signature via __wrapped__
    wrapped = with_asset_error_boundary(stage=stage)(_asset)
    return dg.asset(name=name, group_name=group_name, **asset_kwargs)(wrapped)


__all__ = ["make_asset_from"]

