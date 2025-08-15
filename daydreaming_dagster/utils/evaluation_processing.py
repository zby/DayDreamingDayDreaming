"""Utilities for processing evaluation response files and enriching with metadata."""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from dagster import MetadataValue


def parse_evaluation_files(evaluation_tasks: pd.DataFrame, base_path: Path, parse_function, context=None) -> pd.DataFrame:
    """Parse evaluation response files and extract structured data.
    
    Args:
        evaluation_tasks: DataFrame with evaluation task definitions
        base_path: Base directory containing response files
        parse_function: Function to parse individual evaluation response content
        context: Optional Dagster context for logging
        
    Returns:
        DataFrame with parsed evaluation results
    """
    if evaluation_tasks.empty:
        return pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
    
    parsed_results = []
    total_tasks = len(evaluation_tasks)
    
    for i, (_, task_row) in enumerate(evaluation_tasks.iterrows()):
        if context and i > 0 and i % 100 == 0:
            context.log.info(f"Processed {i}/{total_tasks} evaluation tasks")
            
        evaluation_task_id = task_row['evaluation_task_id']
        response_file = base_path / f"{evaluation_task_id}.txt"
        
        try:
            if not response_file.exists():
                parsed_results.append({
                    "evaluation_task_id": evaluation_task_id,
                    "score": None,
                    "error": "File not found"
                })
                continue
                
            response_text = response_file.read_text()
            result = parse_function(response_text, task_row)
            
            parsed_results.append({
                "evaluation_task_id": evaluation_task_id,
                "score": result.get("score"),
                "error": result.get("error")
            })
            
        except Exception as e:
            parsed_results.append({
                "evaluation_task_id": evaluation_task_id,
                "score": None,
                "error": f"Parse error: {str(e)}"
            })
    
    return pd.DataFrame(parsed_results)


def enrich_evaluation_data(parsed_df: pd.DataFrame, evaluation_tasks: pd.DataFrame, 
                          generation_tasks: pd.DataFrame, required_columns: List[str] = None) -> pd.DataFrame:
    """Enrich parsed evaluation data with task metadata via DataFrame joins.
    
    Args:
        parsed_df: DataFrame with parsed evaluation results
        evaluation_tasks: Evaluation task metadata
        generation_tasks: Generation task metadata
        required_columns: List of columns to ensure exist (with 'unknown' defaults)
        
    Returns:
        Enriched DataFrame with all evaluation metadata
    """
    if parsed_df.empty:
        return parsed_df
        
    # Join with evaluation metadata
    enriched_df = parsed_df.merge(
        evaluation_tasks[['evaluation_task_id', 'generation_task_id', 'evaluation_template', 'evaluation_model']],
        on='evaluation_task_id',
        how='left'
    )
    
    # Join with generation metadata
    if not generation_tasks.empty:
        enriched_df = enriched_df.merge(
            generation_tasks[['generation_task_id', 'combo_id', 'generation_template', 'generation_model']],
            on='generation_task_id',
            how='left'
        )
    else:
        enriched_df['combo_id'] = 'unknown'
        enriched_df['generation_template'] = 'unknown'
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


def add_evaluation_file_paths(df: pd.DataFrame, base_path: str, filename_template: str) -> pd.DataFrame:
    """Add evaluation file path columns based on metadata in the DataFrame.
    
    Args:
        df: DataFrame to add paths to
        base_path: Base directory path for evaluation files
        filename_template: Template string for evaluation filename construction
        
    Returns:
        DataFrame with added file_path column
    """
    df_copy = df.copy()
    df_copy['file_path'] = df_copy.apply(
        lambda row: f"{base_path}/{filename_template.format(**row)}",
        axis=1
    )
    return df_copy
