from dagster import asset, MetadataValue, AssetKey, Failure
from pathlib import Path
import pandas as pd
from ..utils.nodes_standalone import parse_scores
import numpy as np
import math


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager"
)
def parsed_scores(context, evaluation_tasks, generation_tasks) -> pd.DataFrame:
    """
    Parse evaluation responses to extract scores and metadata.
    Aggregates all evaluation responses and parses them into structured data.
    """
    # CLEAN APPROACH: Use Dagster's asset value loader to collect all evaluation responses
    # This is the proper way to load partitioned asset data in Dagster
    evaluation_responses = {}
    
    if not evaluation_tasks.empty:
        # Get the definitions to access the asset value loader
        # Note: We need access to the definitions object for this approach
        try:
            # TODO: Remove this ugly import when Dagster implements asset value loader in context
            # Currently waiting on: https://github.com/dagster-io/dagster/issues/15452
            # "Allow accessing the asset value loader from OpExecutionContext"
            # This would let us do: context.load_asset_value() instead of defs.get_asset_value_loader()
            from ..definitions import defs
            
            # Use Dagster's built-in method for loading partitioned asset values
            with defs.get_asset_value_loader() as loader:
                for _, task_row in evaluation_tasks.iterrows():
                    evaluation_task_id = task_row['evaluation_task_id']
                    
                    try:
                        # Load the evaluation response for this partition
                        response_text = loader.load_asset_value(
                            AssetKey("evaluation_response"), 
                            partition_key=evaluation_task_id,
                            instance=context.instance  # Required for dynamic partitions
                        )
                        evaluation_responses[evaluation_task_id] = response_text
                        
                    except Exception as e:
                        context.log.warning(f"Could not load evaluation response for {evaluation_task_id}: {e}")
                        continue
                        
        except Exception as e:
            context.log.error(f"Could not use asset value loader: {e}")
            raise Failure(f"Failed to load evaluation responses using asset value loader: {e}")
    
    context.log.info(f"Collected {len(evaluation_responses)} evaluation responses using Dagster asset value loader")
    
    # Use existing parse_scores function if we have responses
    if evaluation_responses:
        parsed_csv_path = parse_scores(evaluation_responses)
    else:
        # Create empty CSV if no responses found
        import tempfile
        empty_df = pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        empty_df.to_csv(temp_file.name, index=False)
        parsed_csv_path = temp_file.name
    
    # Load the basic parsed scores
    parsed_df = pd.read_csv(parsed_csv_path)
    
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
    
    # Add model provider columns using simple mapping logic
    def get_model_provider(model_id):
        """Extract provider from model ID like 'deepseek_r1_f' or 'qwq_32b_f'."""
        if pd.isna(model_id):
            return 'unknown'
        
        model_str = str(model_id).lower()
        if 'deepseek' in model_str:
            return 'deepseek'
        elif 'qwq' in model_str or 'qwen' in model_str:
            return 'qwen'
        elif 'gemma' in model_str or 'google' in model_str:
            return 'google'
        else:
            return 'unknown'
    
    # Apply provider mapping - only if DataFrame is not empty
    if not final_df.empty:
        final_df['generation_model_provider'] = final_df['generation_model'].apply(get_model_provider)
        final_df['evaluation_model_provider'] = final_df['evaluation_model'].apply(get_model_provider)
    else:
        # Add columns for empty DataFrame
        final_df['generation_model_provider'] = pd.Series(dtype='object')
        final_df['evaluation_model_provider'] = pd.Series(dtype='object')
    
    # Use final_df instead of parsed_df for the rest of the function
    parsed_df = final_df
    
    # Reorder columns for better readability
    column_order = [
        'combo_id',
        'generation_template', 
        'generation_model_provider',
        'evaluation_template',
        'evaluation_model_provider',
        # 'run_number',  # COMMENTED OUT
        'score',
        'error'
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