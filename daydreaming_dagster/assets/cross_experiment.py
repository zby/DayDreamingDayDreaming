"""Cross-experiment analysis assets for template version comparison and results tracking."""

from dagster import asset, AssetIn, MetadataValue, Config
from pathlib import Path
import pandas as pd
from typing import Dict, Any
from datetime import datetime
import fcntl
import time

from ..utils.evaluation_processing import parse_evaluation_files_cross_experiment, calculate_evaluation_metadata

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
    # FALLBACK(OPS): Reads legacy single-phase responses under data/4_evaluation/evaluation_responses.
    # Keep enabled for cross-experiment analysis; consider gating with a config flag to opt-in.
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


# (Auto-materializing tracking assets removed. Derive cross-experiment views from the gens store and tasks.)
