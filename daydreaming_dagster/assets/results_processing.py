from dagster import asset, MetadataValue, AssetKey, Failure
from pathlib import Path
import pandas as pd
import numpy as np
import math


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"evaluation_response_io_manager"}
)
def parsed_scores(context, evaluation_tasks, generation_tasks) -> pd.DataFrame:
    """
    Parse evaluation responses to extract scores and metadata.
    Processes evaluation response files sequentially to avoid memory issues.
    """
    # Get the base path from configured evaluation_response_io_manager
    evaluation_response_manager = context.resources.evaluation_response_io_manager
    base_path = Path(evaluation_response_manager.base_path)
    
    context.log.info(f"Processing evaluation responses from {base_path}")
    
    # Process evaluation responses sequentially to avoid loading all into memory
    parsed_scores = []
    processed_count = 0
    
    if not evaluation_tasks.empty:
        # Import the parsing logic directly instead of using the filesystem-bypassing function
        from ..utils.eval_response_parser import parse_llm_response
        
        # Process each evaluation task sequentially to avoid memory issues
        for _, task_row in evaluation_tasks.iterrows():
            evaluation_task_id = task_row['evaluation_task_id']
            response_file = base_path / f"{evaluation_task_id}.txt"
            
            try:
                if response_file.exists():
                    # Read single file (not keeping in memory)
                    response_text = response_file.read_text()
                    processed_count += 1
                    
                    # Detect parsing strategy
                    strategy = 'in_last_line'  # Default strategy
                    old_template_names = ['creativity-metrics', 'daydreaming-verification', 'iterative-loops', 'scientific-rigor']
                    for old_template_name in old_template_names:
                        if old_template_name in evaluation_task_id:
                            strategy = 'complex'
                    if 'daydreaming-verification-v2' in evaluation_task_id:
                        strategy = 'in_last_line'
                    
                    # Parse the response
                    result = parse_llm_response(response_text, strategy)
                    score_data = {
                        "evaluation_task_id": evaluation_task_id,
                        "score": result["score"], 
                        "error": result["error"]
                    }
                    parsed_scores.append(score_data)
                else:
                    context.log.warning(f"Response file not found: {response_file}")
                    
            except Exception as e:
                context.log.error(f"Failed to parse response for {evaluation_task_id}: {e}")
                error_record = {
                    "evaluation_task_id": evaluation_task_id,
                    "score": None,
                    "error": str(e)
                }
                parsed_scores.append(error_record)
        
        context.log.info(f"Processed {processed_count} evaluation responses sequentially")
    
    # Create DataFrame from parsed results
    parsed_df = pd.DataFrame(parsed_scores) if parsed_scores else pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
    
    # CLEAN APPROACH: Use DataFrame joins instead of fragile string parsing
    # Join with evaluation_tasks to get clean evaluation metadata
    if not parsed_df.empty and not evaluation_tasks.empty:
        # Join with evaluation_tasks to get evaluation metadata and generation_task_id
        enriched_df = parsed_df.merge(
            evaluation_tasks[['evaluation_task_id', 'generation_task_id', 'evaluation_template', 'evaluation_model']],
            on='evaluation_task_id',
            how='left'
        )
        
        # Join with generation_tasks to get generation metadata
        if not generation_tasks.empty:
            final_df = enriched_df.merge(
                generation_tasks[['generation_task_id', 'combo_id', 'generation_template', 'generation_model']],
                on='generation_task_id',
                how='left'
            )
        else:
            final_df = enriched_df
            # Add missing generation columns
            final_df['combo_id'] = 'unknown'
            final_df['generation_template'] = 'unknown'
            final_df['generation_model'] = 'unknown'
    else:
        final_df = parsed_df.copy()
        # Add missing columns for empty case
        for col in ['combo_id', 'generation_template', 'generation_model', 'evaluation_template', 'evaluation_model', 'generation_task_id']:
            if col not in final_df.columns:
                final_df[col] = 'unknown'
    
    # No need for model provider extraction - we have clean model IDs from DataFrame joins
    
    # Use final_df instead of parsed_df for the rest of the function
    parsed_df = final_df
    
    # Add evaluation response path column for compatibility
    parsed_df['evaluation_response_path'] = 'managed_by_dagster'
    
    # Reorder columns for better readability
    column_order = [
        'combo_id',
        'generation_template', 
        'generation_model',
        'evaluation_template',
        'evaluation_model',
        'score',
        'error',
        'evaluation_response_path'
    ]
    
    # Only keep columns that exist in the dataframe
    existing_columns = [col for col in column_order if col in parsed_df.columns]
    
    context.log.info(f"Parsed {len(parsed_df)} evaluation responses with extracted metadata")
    
    # Add output metadata
    total_responses = len(parsed_df)
    successful_parses = len(parsed_df[parsed_df['error'].isna()]) if 'error' in parsed_df.columns else total_responses
    failed_parses = total_responses - successful_parses
    success_rate = (successful_parses / total_responses * 100) if total_responses > 0 else 0.0
    
    # Handle potential NaN values and ensure float type
    if math.isnan(success_rate):
        success_rate = 0.0
    else:
        success_rate = float(success_rate)  # Ensure it's a float type
    
    context.add_output_metadata({
        "total_responses": MetadataValue.int(total_responses),
        "successful_parses": MetadataValue.int(successful_parses),
        "failed_parses": MetadataValue.int(failed_parses),
        "success_rate": MetadataValue.float(round(success_rate, 2)),
        "unique_combinations": MetadataValue.int(parsed_df['combo_id'].nunique() if 'combo_id' in parsed_df.columns else 0),
        "columns_extracted": MetadataValue.text(", ".join(existing_columns))
    })
    
    return parsed_df[existing_columns]