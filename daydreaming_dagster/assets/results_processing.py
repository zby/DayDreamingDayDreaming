from dagster import asset, MetadataValue, AssetIn, Failure
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any


def _detect_parsing_strategy(evaluation_task_id: str) -> str:
    """Detect appropriate parsing strategy based on evaluation task ID."""
    legacy_templates = ['creativity-metrics', 'daydreaming-verification', 'iterative-loops', 'scientific-rigor']
    
    for template_name in legacy_templates:
        if template_name in evaluation_task_id:
            return 'complex'
    
    return 'in_last_line'  # Default for modern templates


def _parse_single_response(evaluation_task_id: str, response_file: Path, context) -> Dict[str, Any]:
    """Parse a single evaluation response file.
    
    Args:
        evaluation_task_id: Unique identifier for the evaluation task
        response_file: Path to the evaluation response file
        context: Dagster execution context for logging
        
    Returns:
        Dictionary with evaluation_task_id, score, and error fields
    """
    from ..utils.eval_response_parser import parse_llm_response
    
    try:
        if not response_file.exists():
            # Missing files are expected during development/partial runs
            context.log.debug(f"Response file not found (expected during partial runs): {response_file}")
            return None
            
        response_text = response_file.read_text()
        strategy = _detect_parsing_strategy(evaluation_task_id)
        result = parse_llm_response(response_text, strategy)
        
        return {
            "evaluation_task_id": evaluation_task_id,
            "score": result["score"],
            "error": result["error"]
        }
        
    except Exception as e:
        # Log parsing errors but continue processing other files
        context.log.warning(f"Failed to parse response for {evaluation_task_id}: {e}")
        return {
            "evaluation_task_id": evaluation_task_id,
            "score": None,
            "error": f"Parse error: {str(e)}"
        }


def _process_evaluation_responses(evaluation_tasks: pd.DataFrame, base_path: Path, context) -> List[Dict[str, Any]]:
    """Process all evaluation response files sequentially.
    
    This function implements the core aggregation logic for consolidating
    partitioned evaluation_response assets into a single dataset.
    
    Args:
        evaluation_tasks: DataFrame with evaluation task definitions
        base_path: Base directory containing response files
        context: Dagster execution context for logging and metadata
        
    Returns:
        List of parsed score dictionaries
    """
    parsed_scores = []
    processed_count = 0
    missing_count = 0
    error_count = 0
    
    total_tasks = len(evaluation_tasks)
    context.log.info(f"Processing {total_tasks} evaluation tasks sequentially")
    
    for i, (_, task_row) in enumerate(evaluation_tasks.iterrows()):
        evaluation_task_id = task_row['evaluation_task_id']
        response_file = base_path / f"{evaluation_task_id}.txt"
        
        # Log progress for long-running operations
        if i > 0 and i % 100 == 0:
            context.log.info(f"Processed {i}/{total_tasks} tasks ({(i/total_tasks)*100:.1f}%)")
        
        result = _parse_single_response(evaluation_task_id, response_file, context)
        if result:
            parsed_scores.append(result)
            if result.get('error') is None:
                processed_count += 1
            else:
                error_count += 1
        else:
            missing_count += 1
    
    context.log.info(
        f"Evaluation response processing complete: "
        f"{processed_count} successful, {error_count} errors, {missing_count} missing files"
    )
    
    return parsed_scores


def _enrich_with_metadata(parsed_df: pd.DataFrame, evaluation_tasks: pd.DataFrame, generation_tasks: pd.DataFrame) -> pd.DataFrame:
    """Enrich parsed scores with task metadata via DataFrame joins."""
    if parsed_df.empty or evaluation_tasks.empty:
        return _add_missing_columns(parsed_df)
    
    # Join with evaluation metadata
    enriched_df = parsed_df.merge(
        evaluation_tasks[['evaluation_task_id', 'generation_task_id', 'evaluation_template', 'evaluation_model']],
        on='evaluation_task_id',
        how='left'
    )
    
    # Join with generation metadata
    if not generation_tasks.empty:
        final_df = enriched_df.merge(
            generation_tasks[['generation_task_id', 'combo_id', 'generation_template', 'generation_model']],
            on='generation_task_id',
            how='left'
        )
    else:
        final_df = enriched_df.copy()
        for col in ['combo_id', 'generation_template', 'generation_model']:
            final_df[col] = 'unknown'
    
    return final_df


def _add_missing_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add missing columns with default values for empty datasets."""
    required_columns = [
        'combo_id', 'generation_template', 'generation_model', 
        'evaluation_template', 'evaluation_model', 'generation_task_id'
    ]
    
    df_copy = df.copy()
    for col in required_columns:
        if col not in df_copy.columns:
            df_copy[col] = 'unknown'
    
    return df_copy


def _calculate_metadata(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate comprehensive metadata for Dagster observability.
    
    This metadata helps track the asset's performance and provides
    visibility into the evaluation pipeline's health and completeness.
    """
    total_responses = len(df)
    successful_parses = len(df[df['error'].isna()]) if 'error' in df.columns else total_responses
    failed_parses = total_responses - successful_parses
    success_rate = (successful_parses / total_responses * 100) if total_responses > 0 else 0.0
    
    metadata = {
        "total_responses": MetadataValue.int(total_responses),
        "successful_parses": MetadataValue.int(successful_parses),
        "failed_parses": MetadataValue.int(failed_parses),
        "success_rate": MetadataValue.float(round(success_rate, 2)),
    }
    
    # Add pipeline-specific metadata if data exists
    if not df.empty and 'combo_id' in df.columns:
        metadata.update({
            "unique_combinations": MetadataValue.int(df['combo_id'].nunique()),
            "generation_templates": MetadataValue.int(df['generation_template'].nunique() if 'generation_template' in df.columns else 0),
            "evaluation_templates": MetadataValue.int(df['evaluation_template'].nunique() if 'evaluation_template' in df.columns else 0),
            "generation_models": MetadataValue.int(df['generation_model'].nunique() if 'generation_model' in df.columns else 0),
            "evaluation_models": MetadataValue.int(df['evaluation_model'].nunique() if 'evaluation_model' in df.columns else 0),
        })
        
        # Add score distribution metadata if scores exist
        if 'score' in df.columns and successful_parses > 0:
            valid_scores = df[df['error'].isna()]['score']
            if len(valid_scores) > 0:
                metadata.update({
                    "avg_score": MetadataValue.float(round(valid_scores.mean(), 2)),
                    "min_score": MetadataValue.float(round(valid_scores.min(), 2)),
                    "max_score": MetadataValue.float(round(valid_scores.max(), 2)),
                    "perfect_scores": MetadataValue.int(len(valid_scores[valid_scores == 10.0]))
                })
    
    return metadata


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"evaluation_response_io_manager"},
    ins={
        "evaluation_tasks": AssetIn(),
        "generation_tasks": AssetIn()
    },
    description="Aggregate and parse evaluation responses from partitioned evaluation_response assets. "
                "Reads evaluation response files sequentially to extract scores and enrich with task metadata. "
                "This asset consolidates results from the partitioned evaluation pipeline into a single dataset.",
    compute_kind="pandas"
)
def parsed_scores(context, evaluation_tasks: pd.DataFrame, generation_tasks: pd.DataFrame) -> pd.DataFrame:
    """Parse evaluation responses to extract scores and enrich with metadata.
    
    This asset serves as the aggregation point for partitioned evaluation results,
    reading individual evaluation response files and consolidating them into a 
    structured dataset with full task metadata lineage.
    
    Dependencies:
    - evaluation_tasks: Task definitions (determines which files to read)
    - generation_tasks: Generation metadata (for enrichment)
    - evaluation_response (partitioned): Individual response files (implicit dependency)
    
    Returns:
        DataFrame with columns: combo_id, generation_template, generation_model,
        evaluation_template, evaluation_model, score, error, evaluation_response_path
    """
    # Validate inputs - critical for asset dependency tracking
    if evaluation_tasks is None:
        raise Failure("evaluation_tasks input is required but was None")
    if generation_tasks is None:
        raise Failure("generation_tasks input is required but was None")
        
    # Get configured response file path from I/O manager
    evaluation_response_manager = context.resources.evaluation_response_io_manager
    base_path = Path(evaluation_response_manager.base_path)
    
    context.log.info(
        f"Processing evaluation responses from {base_path} "
        f"({len(evaluation_tasks)} evaluation tasks defined)"
    )
    
    # Process responses sequentially to avoid memory issues with large datasets
    if evaluation_tasks.empty:
        context.log.info("No evaluation tasks defined - returning empty results")
        parsed_scores_list = []
    else:
        parsed_scores_list = _process_evaluation_responses(evaluation_tasks, base_path, context)
    
    # Create base DataFrame
    if parsed_scores_list:
        parsed_df = pd.DataFrame(parsed_scores_list)
    else:
        parsed_df = pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
    
    # Enrich with task metadata
    enriched_df = _enrich_with_metadata(parsed_df, evaluation_tasks, generation_tasks)
    
    # Add compatibility column and reorder
    enriched_df['evaluation_response_path'] = 'managed_by_dagster'
    
    column_order = [
        'combo_id', 'generation_template', 'generation_model',
        'evaluation_template', 'evaluation_model', 'score', 'error',
        'evaluation_response_path'
    ]
    existing_columns = [col for col in column_order if col in enriched_df.columns]
    result_df = enriched_df[existing_columns]
    
    # Add comprehensive metadata for Dagster observability
    metadata = _calculate_metadata(result_df)
    metadata.update({
        "evaluation_tasks_processed": MetadataValue.int(len(evaluation_tasks)),
        "generation_tasks_available": MetadataValue.int(len(generation_tasks)),
        "response_file_path": MetadataValue.path(base_path),
        "output_columns": MetadataValue.text(", ".join(existing_columns))
    })
    
    context.add_output_metadata(metadata)
    context.log.info(
        f"Successfully parsed {len(result_df)} evaluation responses with metadata "
        f"(success rate: {metadata['success_rate'].value}%)"
    )
    
    return result_df