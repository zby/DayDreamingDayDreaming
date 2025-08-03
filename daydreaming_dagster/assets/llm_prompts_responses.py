from dagster import asset
from pathlib import Path
import pandas as pd
from jinja2 import Environment
from .partitions import generation_tasks_partitions, evaluation_tasks_partitions
from ..utils.nodes_standalone import (
    generate_prompts, 
    generate_evaluation_prompts,
    parse_scores
)

@asset(
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="generation_prompt_io_manager",
    deps=["task_definitions"]  # Ensure partitions are created first
)
def generation_prompt(
    context, 
    generation_tasks, 
    content_combinations, 
    generation_templates
) -> str:
    """
    Generate one LLM prompt per partition using ContentCombination directly.
    Each prompt is cached as a separate file for debugging.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = generation_tasks[generation_tasks["generation_task_id"] == task_id].iloc[0]
    combo_id = task_row["combo_id"]
    template_id = task_row["generation_template"]
    
    # Find the ContentCombination for this combo_id directly
    content_combination = None
    for combo in content_combinations:
        if combo.combo_id == combo_id:
            content_combination = combo
            break
    
    if content_combination is None:
        raise ValueError(f"No ContentCombination found for combo_id: {combo_id}")
    
    # Render template using ContentCombination.contents (already has name + content)
    template_content = generation_templates[template_id]
    env = Environment()
    template = env.from_string(template_content)
    prompt = template.render(concepts=content_combination.contents)
    
    context.log.info(f"Generated prompt for task {task_id} using ContentCombination")
    return prompt

@asset(
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="generation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["task_definitions"],  # Ensure partitions are created first
    pool="llm_api"  # NEW: Pool-based concurrency control
)
def generation_response(context, generation_prompt, generation_tasks) -> str:
    """
    Generate one LLM response per partition. 
    Dagster automatically handles caching - if this partition exists, it won't re-run.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = generation_tasks[generation_tasks["generation_task_id"] == task_id].iloc[0]
    
    # The prompt is already generated and saved as a file by generation_prompt asset
    prompt = generation_prompt
    
    # Generate using Dagster LLM resource directly  
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(prompt, model=task_row["generation_model"])
    
    context.log.info(f"Generated LLM response for task {task_id}")
    return response

@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="llm_evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    deps=["task_definitions"],  # Only depend on task_definitions, load generation_response manually
    required_resource_keys={"generation_response_io_manager"}
)
def evaluation_prompt(context, evaluation_tasks, evaluation_templates) -> str:
    """
    Generate one evaluation prompt per partition and save as individual file.
    Each prompt is cached as a separate file for debugging.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition with debugging
    matching_tasks = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id]
    if len(matching_tasks) == 0:
        available_tasks = evaluation_tasks["evaluation_task_id"].tolist()
        context.log.error(f"Task ID '{task_id}' not found in evaluation_tasks.")
        context.log.error(f"Available evaluation task IDs: {available_tasks[:5]}...")  # Show first 5
        context.log.error(f"Total evaluation tasks: {len(evaluation_tasks)}")
        raise ValueError(f"Evaluation task '{task_id}' not found in evaluation_tasks DataFrame. "
                        f"Available tasks: {len(evaluation_tasks)}")
    
    task_row = matching_tasks.iloc[0]
    generation_task_id = task_row["generation_task_id"]
    
    # Load the generation response from the correct partition using I/O manager
    # Get the generation response I/O manager from resources to respect test vs production paths
    gen_response_io_manager = context.resources.generation_response_io_manager
    
    # Create a mock context for the generation partition
    class MockLoadContext:
        def __init__(self, partition_key):
            self.partition_key = partition_key
    
    mock_context = MockLoadContext(generation_task_id)
    try:
        generation_response = gen_response_io_manager.load_input(mock_context)
    except FileNotFoundError:
        raise ValueError(f"Generation response not found for partition {generation_task_id}. "
                        f"Make sure to materialize generation_response for this partition first.")
    
    # Generate evaluation prompt using existing logic
    response_dict = {generation_task_id: generation_response}
    eval_prompts_dict = generate_evaluation_prompts(
        response_dict,
        evaluation_tasks.iloc[[task_row.name]],  # Single task
        evaluation_templates
    )
    
    eval_prompt = eval_prompts_dict[task_id]
    context.log.info(f"Generated evaluation prompt for task {task_id}, using generation response from {generation_task_id}")
    return eval_prompt

@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="llm_evaluation",
    io_manager_key="evaluation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["task_definitions"],  # Ensure partitions are created first
    pool="llm_api"  # NEW: Pool-based concurrency control
)
def evaluation_response(context, evaluation_prompt, evaluation_tasks) -> str:
    """Generate one evaluation response per partition."""
    task_id = context.partition_key
    
    # Debug: Check if task_id exists in evaluation_tasks
    matching_tasks = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id]
    if len(matching_tasks) == 0:
        available_tasks = evaluation_tasks["evaluation_task_id"].tolist()
        context.log.error(f"Task ID '{task_id}' not found in evaluation_tasks.")
        context.log.error(f"Available evaluation task IDs: {available_tasks[:5]}...")  # Show first 5
        context.log.error(f"Total evaluation tasks: {len(evaluation_tasks)}")
        raise ValueError(f"Evaluation task '{task_id}' not found in evaluation_tasks DataFrame. "
                        f"Available tasks: {len(evaluation_tasks)}")
    
    task_row = matching_tasks.iloc[0]
    
    # The prompt is already generated and saved as a file by evaluation_prompt asset
    eval_prompt = evaluation_prompt
    
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(eval_prompt, model=task_row["evaluation_model"])
    
    context.log.info(f"Generated evaluation response for task {task_id}")
    return response

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
    evaluation_responses_path = Path("data/04_evaluation/evaluation_responses")
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
    
    # Instead of trying to match with the complex task IDs, let's parse the information directly from our task_id
    # Our task_id format: combo_X_Y_template_generator_evaluator_model
    # We can extract the information directly
    
    def extract_task_info(task_id):
        """Extract combo, generation info, and evaluation info from task_id"""
        if pd.isna(task_id):
            return None, None, None, None, None
            
        # Split the task_id to extract components
        # Example: combo_001_02_problem_solving_deepseek_deepseek-r1:free_daydreaming_verification_qwen_qwq-32b:free
        parts = task_id.split('_')
        
        if len(parts) >= 8:
            # Extract combo_id (e.g., combo_001)
            combo_id = f"{parts[0]}_{parts[1]}"
            
            # Extract generation template (e.g., 02_problem_solving)
            gen_template = f"{parts[2]}_{parts[3]}"
            if len(parts) > 8 and parts[4] not in ['deepseek', 'google', 'qwen']:  # Handle multi-word templates
                gen_template += f"_{parts[4]}"
                offset = 1
            else:
                offset = 0
            
            # Extract generation model provider (e.g., deepseek)
            gen_model_provider = parts[4 + offset]
            
            # Find where evaluation part starts (after model info)
            eval_start_idx = 5 + offset
            while eval_start_idx < len(parts) and parts[eval_start_idx] not in ['daydreaming', 'creativity', 'iterative', 'scientific']:
                eval_start_idx += 1
            
            if eval_start_idx < len(parts):
                # Extract evaluation template
                eval_template_parts = []
                eval_idx = eval_start_idx
                while eval_idx < len(parts) and parts[eval_idx] not in ['deepseek', 'google', 'qwen']:
                    eval_template_parts.append(parts[eval_idx])
                    eval_idx += 1
                
                eval_template = '_'.join(eval_template_parts) if eval_template_parts else 'unknown'
                
                # Extract evaluation model provider - look for the last part that matches known providers
                eval_model_provider = 'unknown'
                remaining_parts = parts[eval_idx:]
                
                # Look for known evaluation model providers in the remaining parts
                for part in remaining_parts:
                    if any(provider in part for provider in ['deepseek', 'qwq', 'gemma']):
                        if 'deepseek' in part:
                            eval_model_provider = 'deepseek'
                        elif 'qwq' in part:
                            eval_model_provider = 'qwen'
                        elif 'gemma' in part:
                            eval_model_provider = 'google'
                        break
                
                return combo_id, gen_template, gen_model_provider, eval_template, eval_model_provider
        
        return None, None, None, None, None
    
    # Apply extraction to all task IDs
    parsed_df[['combo_id', 'generation_template', 'generation_model_provider', 'evaluation_template', 'evaluation_model_provider']] = parsed_df['evaluation_task_id'].apply(
        lambda x: pd.Series(extract_task_info(x))
    )
    
    # Fix evaluation model provider by parsing the evaluation_template field
    def extract_eval_model_from_template(eval_template):
        """Extract evaluation model provider from evaluation_template field"""
        if pd.isna(eval_template):
            return 'unknown'
        
        eval_str = str(eval_template).lower()
        if 'deepseek' in eval_str:
            return 'deepseek'
        elif 'qwen' in eval_str or 'qwq' in eval_str:
            return 'qwen'
        elif 'gemma' in eval_str or 'google' in eval_str:
            return 'google'
        else:
            return 'unknown'
    
    # Apply the corrected evaluation model extraction
    parsed_df['evaluation_model_provider'] = parsed_df['evaluation_template'].apply(extract_eval_model_from_template)
    
    # Clean up evaluation_template to just be the template name
    parsed_df['evaluation_template'] = parsed_df['evaluation_template'].apply(
        lambda x: 'daydreaming_verification' if pd.notna(x) and 'daydreaming_verification' in str(x) else str(x) if pd.notna(x) else 'unknown'
    )
    
    # Reorder columns for better readability
    column_order = [
        'combo_id',
        'generation_template', 
        'generation_model_provider',
        'evaluation_template',
        'evaluation_model_provider', 
        'score',
        'error'
    ]
    
    # Only keep columns that exist in the dataframe
    existing_columns = [col for col in column_order if col in parsed_df.columns]
    
    context.log.info(f"Parsed {len(parsed_df)} evaluation responses with extracted metadata")
    return parsed_df[existing_columns]

@asset(
    group_name="results_processing", 
    io_manager_key="summary_results_io_manager"
)
def final_results(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Create comprehensive pivot table summaries with statistics.
    Includes average scores, perfect scores count, and standard deviation.
    """
    import numpy as np
    
    # Filter out rows with errors (no valid scores)
    valid_scores = parsed_scores[parsed_scores['error'].isna() & parsed_scores['score'].notna()].copy()
    
    def create_pivot_summary(df, group_cols, name_prefix):
        """Create pivot summary with statistics"""
        if df.empty:
            return pd.DataFrame()
            
        grouped = df.groupby(group_cols)['score'].agg([
            ('count', 'count'),
            ('average', 'mean'),
            ('std_dev', 'std'),
            ('min_score', 'min'),
            ('max_score', 'max'),
            ('perfect_scores', lambda x: (x == 10.0).sum()),
            ('high_scores_8plus', lambda x: (x >= 8.0).sum()),
            ('low_scores_3minus', lambda x: (x <= 3.0).sum())
        ]).round(2)
        
        # Add perfect score percentage
        grouped['perfect_score_pct'] = (grouped['perfect_scores'] / grouped['count'] * 100).round(1)
        grouped['high_score_pct'] = (grouped['high_scores_8plus'] / grouped['count'] * 100).round(1)
        
        # Reset index to make group columns regular columns
        result = grouped.reset_index()
        
        # Add analysis category
        result['analysis_type'] = name_prefix
        
        return result
    
    # Create different pivot analyses
    summaries = []
    
    # 1. By Generation Template
    template_summary = create_pivot_summary(
        valid_scores, ['generation_template'], 'by_generation_template'
    )
    summaries.append(template_summary)
    
    # 2. By Generation Model Provider
    gen_model_summary = create_pivot_summary(
        valid_scores, ['generation_model_provider'], 'by_generation_model_provider'
    )
    summaries.append(gen_model_summary)
    
    # 3. By Evaluation Model Provider
    eval_model_summary = create_pivot_summary(
        valid_scores, ['evaluation_model_provider'], 'by_evaluation_model_provider'
    )
    summaries.append(eval_model_summary)
    
    # 4. By Combo ID
    combo_summary = create_pivot_summary(
        valid_scores, ['combo_id'], 'by_combo_id'
    )
    summaries.append(combo_summary)
    
    # 5. By Template + Generation Model combination
    template_model_summary = create_pivot_summary(
        valid_scores, ['generation_template', 'generation_model_provider'], 'by_template_and_generation_model'
    )
    summaries.append(template_model_summary)
    
    # 6. By Generation Model + Evaluation Model combination
    gen_eval_model_summary = create_pivot_summary(
        valid_scores, ['generation_model_provider', 'evaluation_model_provider'], 'by_generation_vs_evaluation_model'
    )
    summaries.append(gen_eval_model_summary)
    
    # 7. Overall statistics
    if not valid_scores.empty:
        overall_stats = pd.DataFrame([{
            'analysis_type': 'overall_statistics',
            'count': len(valid_scores),
            'average': valid_scores['score'].mean(),
            'std_dev': valid_scores['score'].std(),
            'min_score': valid_scores['score'].min(),
            'max_score': valid_scores['score'].max(),
            'perfect_scores': (valid_scores['score'] == 10.0).sum(),
            'high_scores_8plus': (valid_scores['score'] >= 8.0).sum(),
            'low_scores_3minus': (valid_scores['score'] <= 3.0).sum(),
            'perfect_score_pct': ((valid_scores['score'] == 10.0).sum() / len(valid_scores) * 100),
            'high_score_pct': ((valid_scores['score'] >= 8.0).sum() / len(valid_scores) * 100)
        }]).round(2)
        summaries.append(overall_stats)
    
    # Combine all summaries
    if summaries:
        final_summary = pd.concat(summaries, ignore_index=True)
        
        # Reorder columns for better readability
        column_order = [
            'analysis_type',
            'generation_template', 
            'generation_model_provider',
            'evaluation_model_provider',
            'combo_id',
            'count',
            'average',
            'std_dev',
            'min_score',
            'max_score',
            'perfect_scores',
            'perfect_score_pct',
            'high_scores_8plus',
            'high_score_pct',
            'low_scores_3minus'
        ]
        
        # Only keep columns that exist
        existing_columns = [col for col in column_order if col in final_summary.columns]
        final_summary = final_summary[existing_columns]
        
        context.log.info(f"Created comprehensive analysis with {len(final_summary)} summary rows from {len(valid_scores)} valid scores")
        return final_summary
    else:
        context.log.warning("No valid scores found for analysis")
        return pd.DataFrame()

@asset(
    group_name="results_processing",
    io_manager_key="summary_results_io_manager"
)
def perfect_score_paths(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a file with paths to all responses that received perfect scores (10.0).
    Includes both generation and evaluation response paths for analysis.
    """
    # Filter for perfect scores only
    perfect_scores = parsed_scores[
        (parsed_scores['score'] == 10.0) & 
        (parsed_scores['error'].isna())
    ].copy()
    
    if perfect_scores.empty:
        context.log.warning("No perfect scores found")
        return pd.DataFrame(columns=[
            'combo_id', 'generation_template', 'generation_model_provider', 
            'evaluation_model_provider', 'score',
            'generation_response_path', 'evaluation_response_path'
        ])
    
    # Reconstruct file paths for each perfect score
    paths_data = []
    
    for _, row in perfect_scores.iterrows():
        # Reconstruct generation response path
        # Format: data/03_generation/generation_responses/combo_X_template_provider/full_model_id.txt
        gen_model_provider = row['generation_model_provider']
        if gen_model_provider == 'google':
            gen_model_full = 'gemma-3-27b-it:free'
        elif gen_model_provider == 'deepseek':
            gen_model_full = 'deepseek-r1:free'
        else:
            gen_model_full = gen_model_provider
            
        gen_dir = f"{row['combo_id']}_{row['generation_template']}_{gen_model_provider}"
        generation_path = f"data/03_generation/generation_responses/{gen_dir}/{gen_model_full}.txt"
        
        # Reconstruct evaluation response path from the original task ID structure
        # We need to rebuild the complex path structure
        task_id = row.name if hasattr(row, 'name') else ''
        
        # Try to extract the original evaluation task ID pattern from the parsed data
        # This is complex due to the nested directory structure
        eval_base_path = "data/04_evaluation/evaluation_responses"
        
        # Build the evaluation path based on the directory structure we know exists
        # Format: combo_X_template_generator/evaluator_model.txt
        gen_model_clean = row['generation_model_provider']
        if gen_model_clean == 'google':
            gen_model_full = 'gemma-3-27b-it:free'
        elif gen_model_clean == 'deepseek':
            gen_model_full = 'deepseek-r1:free'
        else:
            gen_model_full = gen_model_clean
            
        eval_model_clean = row['evaluation_model_provider']
        if eval_model_clean == 'qwen':
            eval_model_full = 'qwq-32b:free'
        elif eval_model_clean == 'deepseek':
            eval_model_full = 'deepseek-r1:free'
        else:
            eval_model_full = eval_model_clean
            
        # Reconstruct the nested path structure
        generation_dir = f"{row['combo_id']}_{row['generation_template']}_{gen_model_clean}"
        eval_subdir = f"{gen_model_full}_{row['evaluation_template']}_{eval_model_clean}"
        evaluation_path = f"{eval_base_path}/{generation_dir}/{eval_subdir}/{eval_model_full}.txt"
        
        paths_data.append({
            'combo_id': row['combo_id'],
            'generation_template': row['generation_template'],
            'generation_model_provider': row['generation_model_provider'],
            'evaluation_model_provider': row['evaluation_model_provider'],
            'score': row['score'],
            'generation_response_path': generation_path,
            'evaluation_response_path': evaluation_path,
            'notes': f"Perfect score from {row['generation_model_provider']} generation + {row['evaluation_model_provider']} evaluation"
        })
    
    result_df = pd.DataFrame(paths_data)
    
    context.log.info(f"Found {len(result_df)} perfect score responses")
    context.log.info(f"Perfect scores by evaluator: {perfect_scores['evaluation_model_provider'].value_counts().to_dict()}")
    context.log.info(f"Perfect scores by template: {perfect_scores['generation_template'].value_counts().to_dict()}")
    
    return result_df