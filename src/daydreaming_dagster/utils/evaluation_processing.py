"""Utilities for evaluation result metadata calculations.

This module focuses on gens-store based flows. Cross-experiment parsing and
DataFrame enrichment are implemented in assets/cross_experiment.py.
"""

import pandas as pd
from typing import Dict, Any
from dagster import MetadataValue


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


def filter_valid_scores(df: pd.DataFrame, *, score_column: str = 'score', error_column: str = 'error') -> pd.DataFrame:
    """Return rows with a present score and no error.

    Centralizes the common filtering pattern used by pivot/analysis assets.
    """
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    if score_column not in df.columns or error_column not in df.columns:
        # Be conservative: if required columns are missing, return empty
        return df[df.index == -1]
    return df[df[error_column].isna() & df[score_column].notna()].copy()
