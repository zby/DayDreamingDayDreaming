from dagster import asset, MetadataValue
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
    # Collect all materialized evaluation responses
    evaluation_responses_path = Path("data/4_evaluation/evaluation_responses")
    evaluation_responses = {}
    
    if evaluation_responses_path.exists():
        for file_path in evaluation_responses_path.glob("**/*.txt"):
            # Extract evaluation task ID from the nested path structure
            # Path format: combo_X_Y_template_generator/evaluator_model.txt
            # We want to reconstruct the evaluation_task_id: combo_X_Y_template_generator_evaluation_template_evaluator
            relative_path = file_path.relative_to(evaluation_responses_path)
            path_parts = relative_path.parts
            
            if len(path_parts) >= 2:
                generation_task_part = path_parts[0]  # e.g., combo_001_02_problem_solving_deepseek
                eval_part = path_parts[1]  # e.g., deepseek-r1:free_daydreaming_verification_qwen
                model_file = path_parts[2]  # e.g., qwq-32b:free.txt
                
                # Convert back to the original evaluation_task_id format used in CSV files
                # From: combo_001_02_problem_solving_deepseek/deepseek-r1:free_daydreaming_verification_qwen/qwq-32b:free
                # The path structure removes slashes and colons, so we need to reconstruct them
                
                # Parse the generation task part to add slash before model
                gen_parts = generation_task_part.split('_')
                if len(gen_parts) >= 4:
                    # e.g., ['combo', '001', '02', 'problem', 'solving', 'deepseek']
                    combo_template = '_'.join(gen_parts[:-1])  # combo_001_02_problem_solving
                    gen_model_provider = gen_parts[-1]  # deepseek
                    generation_task_with_slash = f"{combo_template}_{gen_model_provider}"  # Keep as is for now
                else:
                    generation_task_with_slash = generation_task_part
                
                # Parse the eval part to add slash before model
                eval_parts = eval_part.split('_')
                if len(eval_parts) >= 3:
                    # e.g., ['deepseek-r1:free', 'daydreaming', 'verification', 'qwen']
                    eval_model_and_template = '_'.join(eval_parts[:-1])  # deepseek-r1:free_daydreaming_verification  
                    eval_model_provider = eval_parts[-1]  # qwen
                    eval_part_with_slash = f"{eval_model_and_template}_{eval_model_provider}"
                else:
                    eval_part_with_slash = eval_part
                
                # Reconstruct with proper format
                model_name = model_file.replace('.txt', '')  # qwq-32b:free
                task_id = f"{generation_task_with_slash}_{eval_part_with_slash}/{model_name}"
                evaluation_responses[task_id] = file_path.read_text()
            else:
                context.log.warning(f"Unexpected file path structure: {file_path}")
    
    # Use existing parse_scores function
    parsed_csv_path = parse_scores(evaluation_responses)
    
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