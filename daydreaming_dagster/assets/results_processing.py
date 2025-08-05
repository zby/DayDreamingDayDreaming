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
    Aggregates all evaluation responses and parses them into structured data.
    """
    # PRAGMATIC APPROACH: Load evaluation responses directly from files
    # The asset value loader requires partitions to be registered in Dagster's dynamic partitions,
    # but we need to load all existing evaluation responses regardless of partition status.
    # This approach respects the same file structure as the PartitionedTextIOManager.
    evaluation_responses = {}
    evaluation_response_paths = {}  # Track file paths for debugging
    
    if not evaluation_tasks.empty:
        try:
            from pathlib import Path
            
            # Get the base path from the configured evaluation_response_io_manager
            evaluation_response_manager = context.resources.evaluation_response_io_manager
            base_path = Path(evaluation_response_manager.base_path)
            
            for _, task_row in evaluation_tasks.iterrows():
                evaluation_task_id = task_row['evaluation_task_id']
                response_file = base_path / f"{evaluation_task_id}.txt"
                
                try:
                    if response_file.exists():
                        response_text = response_file.read_text()
                        evaluation_responses[evaluation_task_id] = response_text
                        evaluation_response_paths[evaluation_task_id] = str(response_file)
                    else:
                        context.log.warning(f"Could not load evaluation response for {evaluation_task_id}: Response file not found: {response_file}")
                        continue
                        
                except Exception as e:
                    context.log.warning(f"Could not load evaluation response for {evaluation_task_id}: {e}")
                    continue
                    
        except Exception as e:
            context.log.error(f"Could not load evaluation responses: {e}")
            raise Failure(f"Failed to load evaluation responses: {e}")
    
    context.log.info(f"Collected {len(evaluation_responses)} evaluation responses from existing files")
    
    # Parse evaluation responses directly without bypassing Dagster I/O system
    if evaluation_responses:
        # Import the parsing logic directly instead of using the filesystem-bypassing function
        from ..utils.eval_response_parser import parse_llm_response
        
        parsed_scores = []
        for evaluation_task_id, response_text in evaluation_responses.items():
            try:
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
                
            except Exception as e:
                context.log.error(f"Failed to parse response for {evaluation_task_id}: {e}")
                error_record = {
                    "evaluation_task_id": evaluation_task_id,
                    "score": None,
                    "error": str(e)
                }
                parsed_scores.append(error_record)
        
        # Create DataFrame directly
        parsed_df = pd.DataFrame(parsed_scores) if parsed_scores else pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
    else:
        # Create empty DataFrame if no responses found
        parsed_df = pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
    
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
    
    # Add evaluation response file paths for debugging
    if evaluation_response_paths:
        # Create a DataFrame with paths for merging
        paths_df = pd.DataFrame([
            {'evaluation_task_id': task_id, 'evaluation_response_path': path}
            for task_id, path in evaluation_response_paths.items()
        ])
        
        # Merge paths into the main DataFrame
        parsed_df = parsed_df.merge(paths_df, on='evaluation_task_id', how='left')
        
        # Fill missing paths with 'not_found' for tasks that had no response file
        parsed_df['evaluation_response_path'] = parsed_df['evaluation_response_path'].fillna('not_found')
    else:
        # Add empty path column if no responses were loaded
        parsed_df['evaluation_response_path'] = 'not_found'
    
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