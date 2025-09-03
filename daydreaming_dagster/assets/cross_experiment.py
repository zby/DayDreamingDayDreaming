"""Cross-experiment analysis assets for template version comparison and results tracking."""

from dagster import asset, AssetIn, MetadataValue, Config, AutomationCondition
from pathlib import Path
import pandas as pd
from typing import Dict, Any
from datetime import datetime
import fcntl
import time

from ..utils.evaluation_processing import parse_evaluation_files_cross_experiment, calculate_evaluation_metadata
from .partitions import (
    draft_tasks_partitions,
    essay_tasks_partitions,
    evaluation_tasks_partitions,
)


def append_to_results_csv(file_path: str, new_row: dict):
    """Thread-safe CSV appending with proper headers."""
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert to DataFrame
    df = pd.DataFrame([new_row])
    
    # Thread-safe append with file locking
    with open(file_path, 'a+') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # Exclusive lock
        
        # Check if file is empty (needs header)
        f.seek(0)
        is_empty = f.read(1) == ''
        
        # Append the row
        df.to_csv(f, mode='a', header=is_empty, index=False)
        
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)  # Release lock


def should_include_evaluation(evaluation_metadata: Dict[str, Any]) -> bool:
    """Filter function that determines whether to include an evaluation file.
    
    Args:
        evaluation_metadata: Dictionary containing all metadata extracted from filename
            - evaluation_template: str
            - evaluation_model: str
            - combo_id: str (if available)
            - generation_template: str (if available)
            - generation_model: str (if available)
            - Any other metadata available from filename parsing
    
    Returns:
        True to include this evaluation, False to exclude
    """
    # TODO: Add filtering logic here later
    # For now, always include everything
    return True


class FilteredEvaluationResultsConfig(Config):
    """Configuration for filtered evaluation results."""
    # For now, no config needed - always uses default filter
    # TODO: Add filtering configuration options later


@asset(
    group_name="cross_experiment",
    io_manager_key="cross_experiment_io_manager",
    description="Parse evaluation files across all experiments using configurable filtering",
    compute_kind="pandas"
)
def filtered_evaluation_results(context, config: FilteredEvaluationResultsConfig) -> pd.DataFrame:
    """Parse evaluation files across all experiments with configurable filtering.
    
    Currently uses the default filter that accepts all files.
    TODO: Add configuration options for custom filtering.
    """
    
    # Use the new cross-experiment parsing utility with filtering
    base_path = Path("data/4_evaluation/evaluation_responses")
    
    # Parse all files (filtering can be added later if needed)
    results_df = parse_evaluation_files_cross_experiment(
        base_path, 
        context
    )
    
    # Add metadata
    metadata = calculate_evaluation_metadata(results_df)
    metadata.update({
        "total_files_parsed": MetadataValue.int(len(results_df)),
        "base_path": MetadataValue.path(base_path),
        "filter_function": MetadataValue.text(should_include_evaluation.__name__)
    })
    
    context.add_output_metadata(metadata)
    return results_df


class TemplateComparisonConfig(Config):
    """Configuration for template version comparison."""
    template_versions: list = None  # If None, uses all available templates


@asset(
    group_name="cross_experiment",
    io_manager_key="cross_experiment_io_manager",
    ins={"filtered_evaluation_results": AssetIn()},
    description="Create pivot table comparing template versions from filtered results",
    compute_kind="pandas"
)
def template_version_comparison_pivot(
    context, 
    filtered_evaluation_results: pd.DataFrame,
    config: TemplateComparisonConfig
) -> pd.DataFrame:
    """Create pivot table for template version comparison from filtered results."""
    
    df = filtered_evaluation_results.copy()
    
    # If no specific template versions specified, use all available in filtered results
    template_versions = config.template_versions
    if template_versions is None:
        template_versions = df['evaluation_template'].unique().tolist()
    
    # Filter to specified template versions
    filtered_df = df[df['evaluation_template'].isin(template_versions)]
    
    # Create pivot table for comparison
    pivot_df = filtered_df.pivot_table(
        index=['combo_id', 'generation_template', 'generation_model'],
        columns='evaluation_template',
        values='score',
        aggfunc='first'
    ).reset_index()
    
    # Add metadata
    context.add_output_metadata({
        "template_versions_compared": MetadataValue.json(template_versions),
        "pivot_table_rows": MetadataValue.int(len(pivot_df)),
        "pivot_table_columns": MetadataValue.int(len(pivot_df.columns)),
        "source_filtered_results": MetadataValue.int(len(filtered_df))
    })
    
    return pivot_df


# ============================================================================
# AUTO-MATERIALIZING RESULTS TRACKING ASSETS
# ============================================================================

@asset(
    partitions_def=draft_tasks_partitions,
    deps=["draft_response"],
    automation_condition=AutomationCondition.eager(),
    group_name="cross_experiment_tracking",
    description="Automatically appends new row when draft_response completes"
)
def draft_generation_results_append(context, draft_generation_tasks):
    """Automatically appends new row when draft_response completes."""
    task_id = context.partition_key
    
    task_rows = draft_generation_tasks[draft_generation_tasks["draft_task_id"] == task_id]
    if task_rows.empty:
        context.log.warning(f"No task found for draft response {task_id}")
        return "no_task_found"
    task_row = task_rows.iloc[0]
    
    response_file = Path(f"data/3_generation/draft_responses/{task_id}.txt")
    response_exists = response_file.exists()
    
    new_row = {
        "draft_task_id": task_id,
        "combo_id": task_row["combo_id"],
        "draft_template_id": task_row["draft_template"],
        "generation_model": task_row["generation_model_name"],
        "generation_status": "success" if response_exists else "failed",
        "generation_timestamp": datetime.now().isoformat(),
        "response_file": f"draft_responses/{task_id}.txt"
    }
    if response_exists:
        new_row["response_size_bytes"] = response_file.stat().st_size
    
    try:
        append_to_results_csv("data/7_cross_experiment/draft_generation_results.csv", new_row)
        context.add_output_metadata({
            "task_id": MetadataValue.text(task_id),
            "combo_id": MetadataValue.text(task_row["combo_id"]),
            "draft_template_id": MetadataValue.text(task_row["draft_template"]),
            "generation_model": MetadataValue.text(task_row["generation_model_name"]),
            "response_exists": MetadataValue.bool(response_exists),
            "table_file": MetadataValue.path("data/7_cross_experiment/draft_generation_results.csv"),
        })
        return "appended"
    except Exception as e:
        context.log.error(f"Failed to append draft generation result for {task_id}: {e}")
        raise


@asset(
    partitions_def=essay_tasks_partitions,
    deps=["essay_response"],
    automation_condition=AutomationCondition.eager(),
    group_name="cross_experiment_tracking",
    description="Automatically appends new row when essay_response completes"
)
def essay_generation_results_append(context, essay_generation_tasks):
    """Automatically appends new row when essay_response completes."""
    task_id = context.partition_key
    
    task_rows = essay_generation_tasks[essay_generation_tasks["essay_task_id"] == task_id]
    if task_rows.empty:
        context.log.warning(f"No task found for essay response {task_id}")
        return "no_task_found"
    task_row = task_rows.iloc[0]
    
    response_file = Path(f"data/3_generation/essay_responses/{task_id}.txt")
    response_exists = response_file.exists()
    
    new_row = {
        "essay_task_id": task_id,
        "combo_id": task_row["combo_id"],
        "essay_template_id": task_row["essay_template"],
        "generation_model": task_row["generation_model_name"],
        "generation_status": "success" if response_exists else "failed",
        "generation_timestamp": datetime.now().isoformat(),
        "response_file": f"essay_responses/{task_id}.txt"
    }
    if response_exists:
        new_row["response_size_bytes"] = response_file.stat().st_size
    
    try:
        append_to_results_csv("data/7_cross_experiment/essay_generation_results.csv", new_row)
        context.add_output_metadata({
            "task_id": MetadataValue.text(task_id),
            "combo_id": MetadataValue.text(task_row["combo_id"]),
            "essay_template_id": MetadataValue.text(task_row["essay_template"]),
            "generation_model": MetadataValue.text(task_row["generation_model_name"]),
            "response_exists": MetadataValue.bool(response_exists),
            "table_file": MetadataValue.path("data/7_cross_experiment/essay_generation_results.csv"),
        })
        return "appended"
    except Exception as e:
        context.log.error(f"Failed to append essay generation result for {task_id}: {e}")
        raise


@asset(
    partitions_def=evaluation_tasks_partitions, 
    deps=["evaluation_response"],
    automation_condition=AutomationCondition.eager(),
    group_name="cross_experiment_tracking",
    description="Automatically appends new row when evaluation_response completes"
)
def evaluation_results_append(context, evaluation_tasks, essay_generation_tasks):
    """Automatically appends new row when evaluation_response completes."""
    task_id = context.partition_key
    
    # Get the specific evaluation task for this partition
    eval_task_rows = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id]
    if eval_task_rows.empty:
        context.log.warning(f"No evaluation task found for response {task_id}")
        return "no_eval_task_found"
        
    eval_task_row = eval_task_rows.iloc[0]
    
    # Get essay metadata via foreign key
    essay_task_id = eval_task_row["essay_task_id"]
    essay_rows = essay_generation_tasks[essay_generation_tasks["essay_task_id"] == essay_task_id]
    if essay_rows.empty:
        context.log.warning(
            f"No essay task found for evaluation {task_id} (essay_task_id: {essay_task_id})"
        )
        return "no_essay_task_found"
    essay_row = essay_rows.iloc[0]
    
    # Check if evaluation response file exists
    eval_response_file = Path(f"data/4_evaluation/evaluation_responses/{task_id}.txt")
    eval_response_exists = eval_response_file.exists()
    
    # Create row data
    new_row = {
        "evaluation_task_id": task_id,
        "essay_task_id": essay_task_id,
        "combo_id": essay_row["combo_id"],
        "link_template": essay_row.get("link_template", "unknown"),
        "essay_template": essay_row["essay_template"],
        "generation_model": essay_row["generation_model_name"],
        "evaluation_template": eval_task_row["evaluation_template"],
        "evaluation_model": eval_task_row["evaluation_model_name"],
        "evaluation_status": "success" if eval_response_exists else "failed",
        "evaluation_timestamp": datetime.now().isoformat(),
        "eval_response_file": f"evaluation_responses/{task_id}.txt"
    }
    
    # Add file size if file exists
    if eval_response_exists:
        new_row["eval_response_size_bytes"] = eval_response_file.stat().st_size
    
    # Append to results table
    try:
        append_to_results_csv("data/7_cross_experiment/evaluation_results.csv", new_row)
        context.log.info(f"Auto-appended evaluation result for {task_id}")
        
        # Add output metadata
        context.add_output_metadata({
            "evaluation_task_id": MetadataValue.text(task_id),
            "essay_task_id": MetadataValue.text(essay_task_id),
            "combo_id": MetadataValue.text(essay_row["combo_id"]),
            "evaluation_template": MetadataValue.text(eval_task_row["evaluation_template"]),
            "evaluation_model": MetadataValue.text(eval_task_row["evaluation_model_name"]),
            "link_template": MetadataValue.text(essay_row.get("link_template", "unknown")),
            "essay_template": MetadataValue.text(essay_row["essay_template"]),
            "generation_model": MetadataValue.text(essay_row["generation_model_name"]),
            "response_exists": MetadataValue.bool(eval_response_exists),
            "table_file": MetadataValue.path("data/7_cross_experiment/evaluation_results.csv"),
            "append_timestamp": MetadataValue.timestamp(time.time())
        })
        
        return "appended"
        
    except Exception as e:
        context.log.error(f"Failed to append evaluation result for {task_id}: {e}")
        raise
