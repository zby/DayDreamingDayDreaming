from dagster import asset, MetadataValue, AssetIn, Failure
from pathlib import Path
import pandas as pd

from ..utils.evaluation_processing import parse_evaluation_files, enrich_evaluation_data, calculate_evaluation_metadata, add_evaluation_file_paths


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"evaluation_response_io_manager"},
    ins={
        "evaluation_tasks": AssetIn(),
        "essay_generation_tasks": AssetIn(),
    },
    description="Parse evaluation responses and enrich with task metadata",
    compute_kind="pandas"
)
def parsed_scores(context, evaluation_tasks: pd.DataFrame, essay_generation_tasks: pd.DataFrame) -> pd.DataFrame:
    """Parse evaluation responses to extract scores and enrich with metadata."""
    # Validate inputs
    if evaluation_tasks is None or essay_generation_tasks is None:
        raise Failure("Both evaluation_tasks and essay_generation_tasks are required")
    
    # Get base path and parse responses using evaluation processing utility
    base_path = Path(context.resources.evaluation_response_io_manager.base_path)
    # Use default parsing from utils - no custom parse function needed
    parsed_df = parse_evaluation_files(evaluation_tasks, base_path, context=context)

    # Join with evaluation task metadata (now references essay_task_id)
    enriched_df = parsed_df.merge(
        evaluation_tasks[[
            "evaluation_task_id",
            "essay_task_id",
            "evaluation_template",
            "evaluation_model",
        ]],
        on="evaluation_task_id",
        how="left",
    )

    # Join with essay generation metadata to recover combo/model/template
    if not essay_generation_tasks.empty:
        enriched_df = enriched_df.merge(
            essay_generation_tasks[[
                "essay_task_id",
                "combo_id",
                "link_template",
                "essay_template",
                "generation_model",
                "generation_model_name",
            ]],
            on="essay_task_id",
            how="left",
        )
    else:
        enriched_df["combo_id"] = "unknown"
        enriched_df["essay_template"] = "unknown"
        enriched_df["generation_model"] = "unknown"

    # Construct evaluation response path directly from task id
    enriched_df["evaluation_response_path"] = enriched_df["evaluation_task_id"].apply(
        lambda tid: str(Path("data/4_evaluation/evaluation_responses") / f"{tid}.txt")
    )

    # Map essay_template into legacy generation_template column for compatibility
    enriched_df["generation_template"] = enriched_df["essay_template"]

    # Select final columns (include IDs useful for downstream path reconstruction)
    result_df = enriched_df[[
        "essay_task_id",
        "combo_id",
        "link_template",
        "generation_template",
        "generation_model",
        "evaluation_template",
        "evaluation_model",
        "score",
        "error",
        "evaluation_response_path",
    ]]
    
    # Calculate metadata using evaluation processing utility
    metadata = calculate_evaluation_metadata(result_df)
    metadata.update({
        "evaluation_tasks_processed": MetadataValue.int(len(evaluation_tasks)),
        "essay_tasks_available": MetadataValue.int(len(essay_generation_tasks)),
        "response_file_path": MetadataValue.path(base_path),
    })
    
    context.add_output_metadata(metadata)
    return result_df
