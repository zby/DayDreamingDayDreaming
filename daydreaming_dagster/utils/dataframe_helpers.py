"""DataFrame helper utilities for Dagster assets."""

import pandas as pd
from dagster import Failure, MetadataValue


def get_task_row(df: pd.DataFrame, task_id_col: str, task_id: str, context, asset_name: str) -> pd.Series:
    """Get single task row with standardized error handling.
    
    Args:
        df: DataFrame to search in
        task_id_col: Name of the column containing task IDs
        task_id: The task ID to find
        context: Dagster context for logging
        asset_name: Name of the asset/table for error messages
        
    Returns:
        Single row as pandas Series
        
    Raises:
        Failure: If task_id not found in DataFrame
    """
    matching_tasks = df[df[task_id_col] == task_id]
    if matching_tasks.empty:
        available_tasks = df[task_id_col].tolist()[:5]  # Show first 5
        context.log.error(f"Task ID '{task_id}' not found in {asset_name} DataFrame")
        raise Failure(
            description=f"Task '{task_id}' not found in {asset_name} database",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "available_tasks_sample": MetadataValue.text(str(available_tasks)),
                "total_tasks": MetadataValue.int(len(df)),
                "resolution_1": MetadataValue.text(f"Check if {asset_name} asset was materialized recently"),
                "resolution_2": MetadataValue.text(f"Run: dagster asset materialize --select {asset_name}"),
            }
        )
    
    return matching_tasks.iloc[0]


def resolve_llm_model_id(row: pd.Series, stage: str) -> str:
    """Resolve the LLM model id from a task row for a given stage.

    Preferred columns:
    - draft: `draft_llm_model` or `generation_model`
    - essay: `essay_llm_model` or `generation_model`
    - evaluation: `evaluation_llm_model` or `evaluation_model`
    Falls back to empty string when not present.
    """
    try:
        s = stage.lower().strip() if isinstance(stage, str) else ""
        if s == "draft":
            return str(row.get("draft_llm_model") or row.get("generation_model") or "").strip()
        if s == "essay":
            return str(row.get("essay_llm_model") or row.get("generation_model") or "").strip()
        if s == "evaluation":
            return str(row.get("evaluation_llm_model") or row.get("evaluation_model") or "").strip()
    except Exception:
        return ""
    return ""
