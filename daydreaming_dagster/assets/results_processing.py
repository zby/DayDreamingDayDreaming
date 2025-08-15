from dagster import asset, MetadataValue, AssetIn, Failure
from pathlib import Path
import pandas as pd

from ..utils.evaluation_processing import parse_evaluation_files, enrich_evaluation_data, calculate_evaluation_metadata, add_evaluation_file_paths
from ..utils.eval_response_parser import parse_llm_response


def _parse_evaluation_response(response_text: str, task_row) -> dict:
    """Parse an evaluation response using the appropriate strategy.
    
    Args:
        response_text: Raw response text from the file
        task_row: Row from evaluation_tasks DataFrame containing metadata
        
    Returns:
        Dictionary with score and error fields
    """
    evaluation_template = task_row['evaluation_template']
    
    # Determine parsing strategy based on template
    strategy = 'complex' if evaluation_template in [
        'creativity-metrics', 'daydreaming-verification', 
        'iterative-loops', 'scientific-rigor'
    ] else 'in_last_line'
    
    return parse_llm_response(response_text, strategy)


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"evaluation_response_io_manager"},
    ins={
        "evaluation_tasks": AssetIn(),
        "generation_tasks": AssetIn()
    },
    description="Parse evaluation responses and enrich with task metadata",
    compute_kind="pandas"
)
def parsed_scores(context, evaluation_tasks: pd.DataFrame, generation_tasks: pd.DataFrame) -> pd.DataFrame:
    """Parse evaluation responses to extract scores and enrich with metadata."""
    # Validate inputs
    if evaluation_tasks is None or generation_tasks is None:
        raise Failure("Both evaluation_tasks and generation_tasks are required")
    
    # Get base path and parse responses using evaluation processing utility
    base_path = Path(context.resources.evaluation_response_io_manager.base_path)
    parsed_df = parse_evaluation_files(evaluation_tasks, base_path, _parse_evaluation_response, context)
    
    # Enrich with metadata using evaluation processing utility
    enriched_df = enrich_evaluation_data(parsed_df, evaluation_tasks, generation_tasks)
    
    # Add evaluation response paths
    enriched_df = add_evaluation_file_paths(
        enriched_df, 
        "data/4_evaluation/evaluation_responses",
        "{combo_id}_{generation_template}_{generation_model}_{evaluation_template}_{evaluation_model}.txt"
    )
    
    # Select final columns
    result_df = enriched_df[['combo_id', 'generation_template', 'generation_model', 'evaluation_template', 'evaluation_model', 'score', 'error', 'file_path']].rename(columns={'file_path': 'evaluation_response_path'})
    
    # Calculate metadata using evaluation processing utility
    metadata = calculate_evaluation_metadata(result_df)
    metadata.update({
        "evaluation_tasks_processed": MetadataValue.int(len(evaluation_tasks)),
        "generation_tasks_available": MetadataValue.int(len(generation_tasks)),
        "response_file_path": MetadataValue.path(base_path)
    })
    
    context.add_output_metadata(metadata)
    return result_df

