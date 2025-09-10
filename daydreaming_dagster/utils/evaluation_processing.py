"""Utilities for evaluation result enrichment and metadata calculations.

This module now focuses on gens-store based flows. Legacy flat-file parsing
helpers have been removed in favor of assets/cross_experiment.py.
"""

import pandas as pd
from typing import Dict, Any, List
from dagster import MetadataValue


def enrich_evaluation_data(parsed_df: pd.DataFrame, evaluation_tasks: pd.DataFrame,
                           essay_generation_tasks: pd.DataFrame, required_columns: List[str] = None) -> pd.DataFrame:
    """Enrich parsed evaluation data with task metadata via DataFrame joins.
    
    Args:
        parsed_df: DataFrame with parsed evaluation results
        evaluation_tasks: Evaluation task metadata
        essay_generation_tasks: Essay generation task metadata
        required_columns: List of columns to ensure exist (with 'unknown' defaults)
        
    Returns:
        Enriched DataFrame with all evaluation metadata
    """
    if parsed_df.empty:
        return parsed_df
        
    # Join with evaluation metadata
    enriched_df = parsed_df.merge(
        evaluation_tasks[['evaluation_task_id', 'essay_task_id', 'evaluation_template', 'evaluation_model']],
        on='evaluation_task_id',
        how='left'
    )
    
    # Join with essay generation metadata
    if not essay_generation_tasks.empty:
        enriched_df = enriched_df.merge(
            essay_generation_tasks[['essay_task_id', 'combo_id', 'essay_template', 'generation_model']],
            on='essay_task_id',
            how='left'
        )
    else:
        enriched_df['combo_id'] = 'unknown'
        enriched_df['essay_template'] = 'unknown'
        enriched_df['generation_model'] = 'unknown'
    
    # Ensure required columns exist
    if required_columns:
        for col in required_columns:
            if col not in enriched_df.columns:
                enriched_df[col] = 'unknown'
    
    return enriched_df


def calculate_evaluation_metadata(df: pd.DataFrame, score_column: str = 'score', error_column: str = 'error') -> Dict[str, Any]:
    """Calculate essential metadata for evaluation DataFrame assets.
    
    Args:
        df: DataFrame containing evaluation results
        score_column: Column name containing evaluation scores
        error_column: Column name containing error information
        
    Returns:
        Dictionary of evaluation metadata values
    """
    total_responses = len(df)
    successful_parses = len(df[df[error_column].isna()]) if error_column in df.columns else total_responses
    
    metadata = {
        "total_responses": MetadataValue.int(total_responses),
        "successful_parses": MetadataValue.int(successful_parses),
        "success_rate": MetadataValue.float(round((successful_parses / total_responses * 100), 2) if total_responses > 0 else 0.0),
    }
    
    # Add score statistics if available
    if score_column in df.columns and successful_parses > 0:
        valid_scores = df[df[error_column].isna()][score_column]
        if len(valid_scores) > 0:
            metadata.update({
                "avg_score": MetadataValue.float(round(float(valid_scores.mean()), 2)),
                "min_score": MetadataValue.float(round(float(valid_scores.min()), 2)),
                "max_score": MetadataValue.float(round(float(valid_scores.max()), 2)),
            })
    
    return metadata
