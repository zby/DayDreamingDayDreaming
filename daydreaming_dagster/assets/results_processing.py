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
    },
    description="Parse evaluation responses and enrich with task metadata",
    compute_kind="pandas"
)
def parsed_scores(context, evaluation_tasks: pd.DataFrame) -> pd.DataFrame:
    """Parse evaluation responses to extract scores and enrich with metadata."""
    # Validate inputs
    if evaluation_tasks is None:
        raise Failure("evaluation_tasks are required")
    
    # Get base path and parse responses using evaluation processing utility
    base_path = Path(context.resources.evaluation_response_io_manager.base_path)
    # Use default parsing from utils - no custom parse function needed
    parsed_df = parse_evaluation_files(evaluation_tasks, base_path, context=context)

    # Join with evaluation task metadata (denormalized)
    enriched_df = parsed_df.merge(
        evaluation_tasks[[
            "evaluation_task_id",
            "document_id",
            "stage",
            "origin",
            "file_path",
            "combo_id",
            "link_template",
            "essay_template",
            "generation_model",
            "generation_model_name",
            "evaluation_template",
            "evaluation_model",
            "source_asset",
            "source_dir",
        ]],
        on="evaluation_task_id",
        how="left",
    )

    # Construct evaluation response path directly from task id
    enriched_df["evaluation_response_path"] = enriched_df["evaluation_task_id"].apply(
        lambda tid: str(Path("data/4_evaluation/evaluation_responses") / f"{tid}.txt")
    )

    # Pass through document file path for generation reference
    enriched_df["generation_response_path"] = enriched_df["file_path"]

    # Derive generation_template for analysis: prefer essay_template, else draft_template (if present), else link_template
    if "draft_template" in enriched_df.columns:
        enriched_df["generation_template"] = np.where(
            enriched_df["essay_template"].notna() & (enriched_df["essay_template"] != ""),
            enriched_df["essay_template"],
            np.where(
                enriched_df["draft_template"].notna() & (enriched_df["draft_template"] != ""),
                enriched_df["draft_template"],
                enriched_df.get("link_template"),
            ),
        )
    else:
        enriched_df["generation_template"] = enriched_df["essay_template"].where(
            enriched_df["essay_template"].notna() & (enriched_df["essay_template"] != ""),
            enriched_df["link_template"],
        )

    # Select final columns (include IDs useful for downstream path reconstruction)
    result_df = enriched_df[[
        "document_id",
        "stage",
        "origin",
        "combo_id",
        "link_template",
        "essay_template",
        "generation_template",
        "generation_model",
        "generation_model_name",
        "evaluation_template",
        "evaluation_model",
        "source_asset",
        "source_dir",
        "score",
        "error",
        "generation_response_path",
        "evaluation_response_path",
    ]]
    
    # Calculate metadata using evaluation processing utility
    metadata = calculate_evaluation_metadata(result_df)
    metadata.update({
        "evaluation_tasks_processed": MetadataValue.int(len(evaluation_tasks)),
        "response_file_path": MetadataValue.path(base_path),
    })
    
    context.add_output_metadata(metadata)
    return result_df
