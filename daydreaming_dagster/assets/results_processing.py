from dagster import asset, MetadataValue, AssetIn, Failure
from pathlib import Path
import pandas as pd
import numpy as np

from ..utils.evaluation_processing import (
    parse_evaluation_files,
    enrich_evaluation_data,
    calculate_evaluation_metadata,
    add_evaluation_file_paths,
)
from .raw_data import EVALUATION_TEMPLATES_KEY


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"evaluation_response_io_manager"},
    ins={
        "evaluation_tasks": AssetIn(),
        "evaluation_templates": AssetIn(key=EVALUATION_TEMPLATES_KEY),
    },
    description="Parse evaluation responses and enrich with task metadata",
    compute_kind="pandas"
)
def parsed_scores(context, evaluation_tasks: pd.DataFrame, evaluation_templates: pd.DataFrame) -> pd.DataFrame:
    """Parse evaluation responses to extract scores and enrich with metadata."""
    # Validate inputs
    if evaluation_tasks is None:
        raise Failure("evaluation_tasks are required")
    
    # Get base path and parse responses using evaluation processing utility
    base_path = Path(context.resources.evaluation_response_io_manager.base_path)
    # Parser selection is resolved at parse time from evaluation_templates.csv
    parsed_df = parse_evaluation_files(
        evaluation_tasks,
        base_path,
        context=context,
        evaluation_templates=evaluation_templates,
    )

    # Join with evaluation task metadata (denormalized)
    base_cols = [
        "evaluation_task_id",
        "document_id",
        "stage",
        "origin",
        "file_path",
        "combo_id",
        "draft_template",
        "essay_template",
        "generation_model",
        "generation_model_name",
        "evaluation_template",
        "evaluation_model",
        "source_asset",
        "source_dir",
    ]
    select_cols = [c for c in base_cols if c in evaluation_tasks.columns]
    enriched_df = parsed_df.merge(
        evaluation_tasks[select_cols],
        on="evaluation_task_id",
        how="left",
    )

    # Evaluation response path: prefer actual used path if provided by parser; else construct
    if "used_response_path" in enriched_df.columns:
        enriched_df["evaluation_response_path"] = enriched_df["used_response_path"].astype(str)
    else:
        enriched_df["evaluation_response_path"] = enriched_df["evaluation_task_id"].apply(
            lambda tid: str(Path("data/4_evaluation/evaluation_responses") / f"{tid}.txt")
        )

    # Pass through document file path for generation reference
    enriched_df["generation_response_path"] = enriched_df["file_path"]

    # Derive generation_template for analysis: prefer essay_template, else draft_template
    enriched_df["generation_template"] = np.where(
        enriched_df["essay_template"].notna() & (enriched_df["essay_template"] != ""),
        enriched_df["essay_template"],
        enriched_df.get("draft_template"),
    )

    # Select final columns (include IDs useful for downstream path reconstruction)
    desired_cols = [
        "document_id",
        "stage",
        "origin",
        "combo_id",
        "draft_template",
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
    ]
    for col, default in ("stage", "unknown"), ("origin", "unknown"), ("source_asset", "unknown"), ("source_dir", "unknown"):
        if col not in enriched_df.columns:
            enriched_df[col] = default
    present_cols = [c for c in desired_cols if c in enriched_df.columns]
    result_df = enriched_df[present_cols]
    
    # Calculate metadata using evaluation processing utility
    metadata = calculate_evaluation_metadata(result_df)
    metadata.update({
        "evaluation_tasks_processed": MetadataValue.int(len(evaluation_tasks)),
        "response_file_path": MetadataValue.path(base_path),
    })
    
    context.add_output_metadata(metadata)
    return result_df
