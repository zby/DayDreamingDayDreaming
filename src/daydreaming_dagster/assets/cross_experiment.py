"""Cross-experiment analysis assets using gens-store as the source of truth.

Reads evaluation results from `data/gens/evaluation/<gen_id>/{parsed.txt,raw.txt,metadata.json}`
and enriches with generation metadata from parent essay/draft documents.
"""

from dagster import AssetIn, MetadataValue, Config
from ._decorators import asset_with_boundary
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import json

from ..utils.evaluation_processing import calculate_evaluation_metadata
from ..config.paths import Paths
from ..utils.evaluation_scores import aggregate_evaluation_scores_for_ids

class FilteredEvaluationResultsConfig(Config):
    """Configuration for filtered evaluation results."""
    # For now, no config needed - always uses default filter
    # TODO: Add filtering configuration options later


@asset_with_boundary(
    stage="cross_experiment",
    group_name="cross_experiment",
    io_manager_key="cross_experiment_io_manager",
    description="Scan gens-store evaluation outputs across experiments and parse scores",
    compute_kind="pandas",
    required_resource_keys={"data_root"},
)
def filtered_evaluation_results(context, config: FilteredEvaluationResultsConfig) -> pd.DataFrame:
    """Collect evaluation results from gens-store and enrich with generation metadata.

    Implementation is consolidated with utils.evaluation_scores.aggregate_evaluation_scores_for_ids
    to ensure consistent row shape and enrichment rules across assets.
    """
    paths = Paths.from_context(context)
    eval_root = paths.gens_root / "evaluation"
    # Let filesystem errors surface naturally if eval_root is missing
    gen_ids = [p.name for p in sorted(eval_root.iterdir()) if p.is_dir()]

    df = aggregate_evaluation_scores_for_ids(paths.data_root, gen_ids)

    # Add metadata
    metadata = calculate_evaluation_metadata(df)
    metadata.update({
        "total_responses": MetadataValue.int(len(df)),
        "base_path": MetadataValue.path(str(eval_root)),
    })
    context.add_output_metadata(metadata)
    return df


class TemplateComparisonConfig(Config):
    """Configuration for template version comparison."""
    template_versions: list = None  # If None, uses all available templates


@asset_with_boundary(
    stage="cross_experiment",
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


# (Auto-materializing tracking assets removed. Derive cross-experiment views from the gens store and cohort membership.)
