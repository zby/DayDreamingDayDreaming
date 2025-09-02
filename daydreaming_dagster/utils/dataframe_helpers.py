"""DataFrame helper utilities for Dagster assets."""

import pandas as pd
from dagster import Failure, MetadataValue


def is_valid_evaluation_task_id(task_id: str) -> bool:
    """Validate evaluation_task_id format: {document_id}__{evaluation_template}__{evaluation_model_id}.

    Rules:
    - Exactly two double-underscore separators ('__'), yielding three non-empty parts
    - No whitespace characters
    - Parts are non-empty strings
    """
    if not isinstance(task_id, str):
        return False
    if any(ch.isspace() for ch in task_id):
        return False
    parts = task_id.split("__")
    if len(parts) != 3:
        return False
    if any(len(p) == 0 for p in parts):
        return False
    return True


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
