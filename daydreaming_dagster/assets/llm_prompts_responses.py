from dagster import asset, Failure, MetadataValue
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
    deps=["generation_tasks"]  # Ensure partitions are created first
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
    matching_tasks = generation_tasks[generation_tasks["generation_task_id"] == task_id]
    if matching_tasks.empty:
        available_tasks = generation_tasks["generation_task_id"].tolist()[:5]  # Show first 5
        context.log.error(f"Task ID '{task_id}' not found in generation_tasks DataFrame")
        raise Failure(
            description=f"Generation task '{task_id}' not found in task database",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "available_tasks_sample": MetadataValue.text(str(available_tasks)),
                "total_tasks": MetadataValue.int(len(generation_tasks)),
                "resolution_1": MetadataValue.text("Check if generation_tasks asset was materialized recently"),
                "resolution_2": MetadataValue.text("Run: dagster asset materialize --select generation_tasks"),
                "resolution_3": MetadataValue.text("Verify partitions are up to date - stale partitions may reference old task IDs"),
                "resolution_4": MetadataValue.text("If using filtered concepts/templates, ensure the filter includes this task")
            }
        )
    task_row = matching_tasks.iloc[0]
    combo_id = task_row["combo_id"]
    template_id = task_row["generation_template"]
    
    # Find the ContentCombination for this combo_id directly
    content_combination = None
    for combo in content_combinations:
        if combo.combo_id == combo_id:
            content_combination = combo
            break
    
    if content_combination is None:
        available_combos = [combo.combo_id for combo in content_combinations[:5]]  # Show first 5
        context.log.error(f"No ContentCombination found for combo_id: {combo_id}")
        raise Failure(
            description=f"Content combination '{combo_id}' not found in combinations database",
            metadata={
                "combo_id": MetadataValue.text(combo_id),
                "available_combinations_sample": MetadataValue.text(str(available_combos)),
                "total_combinations": MetadataValue.int(len(content_combinations)),
                "resolution_1": MetadataValue.text("Check if content_combinations asset was materialized"),
                "resolution_2": MetadataValue.text("Run: dagster asset materialize --select content_combinations"),
                "resolution_3": MetadataValue.text("Verify the combo_id format matches expected pattern (combo_XXX)")
            }
        )
    
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
    deps=["generation_tasks"],  # Remove generation_models dependency
    pool="llm_api"  # Pool-based concurrency control
)
def generation_response(context, generation_prompt, generation_tasks) -> str:
    """
    Generate one LLM response per partition. 
    Dagster automatically handles caching - if this partition exists, it won't re-run.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = generation_tasks[generation_tasks["generation_task_id"] == task_id].iloc[0]
    
    # Use the model name directly from the task
    model_name = task_row["generation_model_name"]
    
    # The prompt is already generated and saved as a file by generation_prompt asset
    prompt = generation_prompt
    
    # Generate using Dagster LLM resource directly  
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(prompt, model=model_name)
    
    context.log.info(f"Generated LLM response for task {task_id} using model {model_name}")
    return response

@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="llm_evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    deps=["evaluation_tasks"],  # Only depend on evaluation_tasks, load generation_response manually
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
        available_tasks = evaluation_tasks["evaluation_task_id"].tolist()[:5]  # Show first 5
        context.log.error(f"Evaluation task '{task_id}' not found in evaluation_tasks DataFrame")
        raise Failure(
            description=f"Evaluation task '{task_id}' not found in task database",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "available_tasks_sample": MetadataValue.text(str(available_tasks)),
                "total_tasks": MetadataValue.int(len(evaluation_tasks)),
                "resolution_1": MetadataValue.text("Check if evaluation_tasks asset was materialized recently"),
                "resolution_2": MetadataValue.text("Run: dagster asset materialize --select evaluation_tasks"),
                "resolution_3": MetadataValue.text("Verify partitions are up to date - stale partitions may reference old task IDs"),
                "resolution_4": MetadataValue.text("Ensure generation_tasks was materialized first (evaluation depends on it)")
            }
        )
    
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
    except FileNotFoundError as e:
        context.log.error(f"Generation response not found for partition {generation_task_id}")
        raise Failure(
            description=f"Missing generation response required for evaluation task '{task_id}'",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "generation_task_id": MetadataValue.text(generation_task_id),
                "expected_file_path": MetadataValue.path(f"{gen_response_io_manager.base_path}/{generation_task_id}.txt"),
                "resolution_1": MetadataValue.text(f"Check if generation_response was materialized for partition: {generation_task_id}"),
                "resolution_2": MetadataValue.text(f"Run: dagster asset materialize --select generation_response --partition {generation_task_id}"),
                "resolution_3": MetadataValue.text("Or materialize all generation responses: dagster asset materialize --select generation_response"),
                "original_error": MetadataValue.text(str(e))
            }
        ) from e
    
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
    deps=["generation_tasks"],  # Remove evaluation_models dependency
    pool="llm_api"  # Pool-based concurrency control
)
def evaluation_response(context, evaluation_prompt, evaluation_tasks) -> str:
    """Generate one evaluation response per partition."""
    task_id = context.partition_key
    
    # Debug: Check if task_id exists in evaluation_tasks
    matching_tasks = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id]
    if len(matching_tasks) == 0:
        available_tasks = evaluation_tasks["evaluation_task_id"].tolist()[:5]  # Show first 5
        context.log.error(f"Evaluation task '{task_id}' not found in evaluation_tasks DataFrame")
        raise Failure(
            description=f"Evaluation task '{task_id}' not found in task database",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "available_tasks_sample": MetadataValue.text(str(available_tasks)),
                "total_tasks": MetadataValue.int(len(evaluation_tasks)),
                "resolution_1": MetadataValue.text("Check if evaluation_tasks asset was materialized recently"),
                "resolution_2": MetadataValue.text("Run: dagster asset materialize --select evaluation_tasks"),
                "resolution_3": MetadataValue.text("Verify partitions are up to date - stale partitions may reference old task IDs"),
                "resolution_4": MetadataValue.text("Ensure evaluation_prompt was materialized first (evaluation_response depends on it)")
            }
        )
    
    task_row = matching_tasks.iloc[0]
    
    # Use the model name directly from the task
    model_name = task_row["evaluation_model_name"]
    
    # The prompt is already generated and saved as a file by evaluation_prompt asset
    eval_prompt = evaluation_prompt
    
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(eval_prompt, model=model_name)
    
    context.log.info(f"Generated evaluation response for task {task_id} using model {model_name}")
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
            # COMMENTED OUT: Multiple runs feature - no run number extraction for now
            # Extract run number from the end if present (e.g., run01)
            # run_number = None
            # if parts[-1].startswith('run'):
            #     run_part = parts[-1]
            #     try:
            #         run_number = int(run_part[3:])  # Extract number from 'run01'
            #         parts = parts[:-1]  # Remove run part for further processing
            #     except ValueError:
            #         run_number = None
            
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
    
    # Apply extraction to all task IDs - but only if DataFrame is not empty
    if not parsed_df.empty:
        parsed_df[['combo_id', 'generation_template', 'generation_model_provider', 'evaluation_template', 'evaluation_model_provider']] = parsed_df['evaluation_task_id'].apply(
            lambda x: pd.Series(extract_task_info(x))
        )
    else:
        # For empty DataFrame, just add the columns with appropriate defaults
        parsed_df['combo_id'] = pd.Series(dtype='object')
        parsed_df['generation_template'] = pd.Series(dtype='object')
        parsed_df['generation_model_provider'] = pd.Series(dtype='object')
        parsed_df['evaluation_template'] = pd.Series(dtype='object')
        parsed_df['evaluation_model_provider'] = pd.Series(dtype='object')
        # parsed_df['run_number'] = pd.Series(dtype='int64')  # COMMENTED OUT
    
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
    
    # Apply the corrected evaluation model extraction - but only if DataFrame is not empty
    if not parsed_df.empty:
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
    import math
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

@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager"
)
def evaluator_agreement_analysis(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate evaluator agreement metrics for the same generation responses.
    Groups evaluations by generation_task_id to analyze variance across:
    1. Multiple evaluation models (deepseek_r1_f vs qwq_32b_f)  
    2. Multiple evaluation templates (when available)
    
    This provides a comprehensive view of evaluation stability across both dimensions.
    """
    # Filter out rows with errors (no valid scores)
    valid_scores = parsed_scores[
        parsed_scores['error'].isna() & 
        parsed_scores['score'].notna()
    ].copy()
    
    if valid_scores.empty:
        context.log.warning("No valid scores found for evaluator agreement analysis")
        return pd.DataFrame()
    
    # Extract generation_task_id from evaluation_task_id for grouping
    # Format: combo_001_systematic-analytical-v2_deepseek_r1_f_daydreaming-verification-v2_qwq_32b_f
    # We want: combo_001_systematic-analytical-v2_deepseek_r1_f (the generation part)
    
    def extract_generation_task_id(eval_task_id):
        """Extract generation_task_id from evaluation_task_id"""
        if pd.isna(eval_task_id):
            return None
        
        # Split by underscore and try to find the generation task pattern
        parts = eval_task_id.split('_')
        
        # Look for the daydreaming-verification part to know where generation task ends
        if 'daydreaming-verification-v2' in eval_task_id:
            verification_index = eval_task_id.find('_daydreaming-verification-v2')
            if verification_index > 0:
                return eval_task_id[:verification_index]
        
        # Fallback: assume first 4 parts (combo_001_template_model)
        if len(parts) >= 4:
            return '_'.join(parts[:4])
        
        return eval_task_id
    
    # Add generation_task_id column
    valid_scores['generation_task_id'] = valid_scores['evaluation_task_id'].apply(extract_generation_task_id)
    
    # Group by generation_task_id to find cases where multiple evaluators scored the same response
    agreement_stats = valid_scores.groupby('generation_task_id')['score'].agg([
        ('evaluator_count', 'count'),
        ('mean_score', 'mean'),
        ('std_dev', 'std'),
        ('min_score', 'min'),
        ('max_score', 'max'),
        ('median_score', 'median')
    ]).reset_index()
    
    # Only keep cases where we have multiple evaluators (agreement possible)
    multi_evaluator = agreement_stats[agreement_stats['evaluator_count'] >= 2].copy()
    
    if multi_evaluator.empty:
        context.log.warning("No generation responses found with multiple evaluators")
        return pd.DataFrame()
    
    # Calculate agreement metrics
    multi_evaluator['score_range'] = multi_evaluator['max_score'] - multi_evaluator['min_score']
    multi_evaluator['coefficient_of_variation'] = multi_evaluator['std_dev'] / multi_evaluator['mean_score']
    
    # Classify agreement levels
    def classify_agreement(row):
        """Classify evaluator agreement based on score range and CV"""
        if pd.isna(row['std_dev']) or row['evaluator_count'] < 2:
            return 'insufficient_data'
        elif row['score_range'] <= 1.0:  # Within 1 point
            return 'high_agreement'
        elif row['score_range'] <= 2.0:  # Within 2 points
            return 'moderate_agreement'
        elif row['score_range'] <= 3.0:  # Within 3 points
            return 'low_agreement'
        else:
            return 'poor_agreement'
    
    multi_evaluator['agreement_classification'] = multi_evaluator.apply(classify_agreement, axis=1)
    
    # Add relative variance metrics (normalized by score scale)
    multi_evaluator['relative_std_dev'] = multi_evaluator['std_dev'] / 10.0
    multi_evaluator['relative_range'] = multi_evaluator['score_range'] / 10.0
    
    # Extract some metadata from generation_task_id for analysis
    def extract_metadata(gen_task_id):
        """Extract combo and template info from generation_task_id"""
        if pd.isna(gen_task_id):
            return None, None, None
        
        parts = gen_task_id.split('_')
        if len(parts) >= 4:
            combo_id = f"{parts[0]}_{parts[1]}"  # combo_001
            template = parts[2] if len(parts) > 2 else 'unknown'  # systematic-analytical-v2
            model = parts[3] if len(parts) > 3 else 'unknown'  # deepseek
            return combo_id, template, model
        return None, None, None
    
    multi_evaluator[['combo_id', 'generation_template', 'generation_model']] = multi_evaluator['generation_task_id'].apply(
        lambda x: pd.Series(extract_metadata(x))
    )
    
    # Create summary statistics
    agreement_summary = multi_evaluator['agreement_classification'].value_counts()
    context.log.info(f"Evaluator agreement summary: {dict(agreement_summary)}")
    
    # Log overall agreement statistics
    if len(multi_evaluator) > 0:
        overall_range = multi_evaluator['score_range'].median()
        overall_std = multi_evaluator['std_dev'].median()
        high_disagreement = len(multi_evaluator[multi_evaluator['score_range'] > 3.0])
        
        context.log.info(f"Overall median score range: {overall_range:.2f}")
        context.log.info(f"Overall median standard deviation: {overall_std:.2f}")
        
        if high_disagreement > 0:
            context.log.warning(f"Found {high_disagreement} generation responses with high evaluator disagreement (range > 3.0)")
    
    context.log.info(f"Analyzed evaluator agreement for {len(multi_evaluator)} generation responses with multiple evaluators")
    
    # Add output metadata
    agreement_counts = multi_evaluator['agreement_classification'].value_counts().to_dict() if not multi_evaluator.empty else {}
    
    context.add_output_metadata({
        "responses_analyzed": MetadataValue.int(len(multi_evaluator)),
        "total_valid_scores": MetadataValue.int(len(valid_scores)),
        "multi_evaluator_cases": MetadataValue.int(len(multi_evaluator)),
        "high_agreement": MetadataValue.int(agreement_counts.get('high_agreement', 0)),
        "moderate_agreement": MetadataValue.int(agreement_counts.get('moderate_agreement', 0)),
        "low_agreement": MetadataValue.int(agreement_counts.get('low_agreement', 0)),
        "poor_agreement": MetadataValue.int(agreement_counts.get('poor_agreement', 0)),
        "median_score_range": MetadataValue.float(round(float(multi_evaluator['score_range'].median()), 2) if not multi_evaluator.empty and not pd.isna(multi_evaluator['score_range'].median()) else 0.0)
    })
    
    return multi_evaluator

@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager"
)
def comprehensive_variance_analysis(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Comprehensive variance analysis across all evaluation dimensions:
    1. Template variance: Same model, different evaluation templates
    2. Model variance: Same template, different evaluation models  
    3. Overall variance: All evaluations of the same generation response
    
    This creates a detailed breakdown of where evaluation instability comes from.
    """
    # Filter out rows with errors
    valid_scores = parsed_scores[
        parsed_scores['error'].isna() & 
        parsed_scores['score'].notna()
    ].copy()
    
    if valid_scores.empty:
        context.log.warning("No valid scores found for comprehensive variance analysis")
        return pd.DataFrame()
    
    # Extract generation_task_id (same logic as before)
    def extract_generation_task_id(eval_task_id):
        if pd.isna(eval_task_id):
            return None
        
        if 'daydreaming-verification-v2' in eval_task_id:
            verification_index = eval_task_id.find('_daydreaming-verification-v2')
            if verification_index > 0:
                return eval_task_id[:verification_index]
        
        # Generalized for any evaluation template
        # Find pattern: _templatename_model at the end
        parts = eval_task_id.split('_')
        if len(parts) >= 6:  # combo_XXX_gentemplate_genmodel_evaltemplate_evalmodel
            return '_'.join(parts[:-2])  # Remove last 2 parts (eval template + eval model)
        
        return eval_task_id
    
    valid_scores['generation_task_id'] = valid_scores['evaluation_task_id'].apply(extract_generation_task_id)
    
    # Parse evaluation model and template from evaluation_task_id for grouping
    def parse_evaluation_info(eval_task_id):
        """Extract evaluation template and model from task ID"""
        if pd.isna(eval_task_id):
            return None, None
            
        # Look for known evaluation templates
        eval_templates = ['daydreaming-verification-v2', 'creativity-metrics', 'scientific-rigor', 'iterative-loops']
        
        eval_template = 'unknown'
        eval_model = 'unknown'
        
        for template in eval_templates:
            if template in eval_task_id:
                eval_template = template
                # Extract model after template
                template_index = eval_task_id.find(f'_{template}_')
                if template_index >= 0:
                    remaining = eval_task_id[template_index + len(template) + 2:]  # +2 for underscores
                    eval_model = remaining if remaining else 'unknown'
                break
        
        return eval_template, eval_model
    
    valid_scores[['eval_template', 'eval_model']] = valid_scores['evaluation_task_id'].apply(
        lambda x: pd.Series(parse_evaluation_info(x))
    )
    
    # Now we can analyze variance across multiple dimensions
    variance_analyses = []
    
    # 1. Overall variance: Group only by generation_task_id
    overall_variance = valid_scores.groupby('generation_task_id').agg({
        'score': ['count', 'mean', 'std', 'min', 'max'],
        'eval_template': lambda x: x.nunique(),  # How many different templates
        'eval_model': lambda x: x.nunique()     # How many different models
    }).round(3)
    
    overall_variance.columns = ['total_evaluations', 'mean_score', 'std_dev', 'min_score', 'max_score', 'num_templates', 'num_models']
    overall_variance = overall_variance.reset_index()
    overall_variance['score_range'] = overall_variance['max_score'] - overall_variance['min_score']
    overall_variance['coefficient_of_variation'] = overall_variance['std_dev'] / overall_variance['mean_score']
    overall_variance['analysis_type'] = 'overall_variance'
    
    # 2. Template variance: Same generation + same model, different templates
    template_groups = valid_scores.groupby(['generation_task_id', 'eval_model'])
    template_variance_list = []
    
    for (gen_id, model), group in template_groups:
        if len(group) >= 2:  # Need multiple evaluations to calculate variance
            template_stats = {
                'generation_task_id': gen_id,
                'eval_model': model,
                'template_evaluations': len(group),
                'mean_score': group['score'].mean(),
                'std_dev': group['score'].std(),
                'min_score': group['score'].min(),
                'max_score': group['score'].max(),
                'num_templates': group['eval_template'].nunique(),
                'templates_used': ', '.join(sorted(group['eval_template'].unique())),
                'analysis_type': 'template_variance'
            }
            template_stats['score_range'] = template_stats['max_score'] - template_stats['min_score']
            template_stats['coefficient_of_variation'] = template_stats['std_dev'] / template_stats['mean_score'] if template_stats['mean_score'] != 0 else 0
            template_variance_list.append(template_stats)
    
    template_variance = pd.DataFrame(template_variance_list) if template_variance_list else pd.DataFrame()
    
    # 3. Model variance: Same generation + same template, different models  
    model_groups = valid_scores.groupby(['generation_task_id', 'eval_template'])
    model_variance_list = []
    
    for (gen_id, template), group in model_groups:
        if len(group) >= 2:  # Need multiple evaluations to calculate variance
            model_stats = {
                'generation_task_id': gen_id,
                'eval_template': template,
                'model_evaluations': len(group),
                'mean_score': group['score'].mean(),
                'std_dev': group['score'].std(),
                'min_score': group['score'].min(),
                'max_score': group['score'].max(),
                'num_models': group['eval_model'].nunique(),
                'models_used': ', '.join(sorted(group['eval_model'].unique())),
                'analysis_type': 'model_variance'
            }
            model_stats['score_range'] = model_stats['max_score'] - model_stats['min_score']
            model_stats['coefficient_of_variation'] = model_stats['std_dev'] / model_stats['mean_score'] if model_stats['mean_score'] != 0 else 0
            model_variance_list.append(model_stats)
    
    model_variance = pd.DataFrame(model_variance_list) if model_variance_list else pd.DataFrame()
    
    # Combine all analyses
    result_dfs = []
    
    if not overall_variance.empty:
        result_dfs.append(overall_variance)
        context.log.info(f"Overall variance: {len(overall_variance)} generation responses with multiple evaluations")
    
    if not template_variance.empty:
        result_dfs.append(template_variance)
        context.log.info(f"Template variance: {len(template_variance)} cases where same model used different templates")
    
    if not model_variance.empty:
        result_dfs.append(model_variance)  
        context.log.info(f"Model variance: {len(model_variance)} cases where same template used different models")
    
    if result_dfs:
        # Combine all analyses into one comprehensive DataFrame
        combined_analysis = pd.concat(result_dfs, ignore_index=True, sort=False)
        
        # Add stability classifications
        def classify_variance_stability(row):
            if pd.isna(row['coefficient_of_variation']) or row.get('total_evaluations', row.get('template_evaluations', row.get('model_evaluations', 1))) < 2:
                return 'insufficient_data'
            elif row['score_range'] <= 1.0:
                return 'high_agreement'
            elif row['score_range'] <= 2.0:
                return 'moderate_agreement'
            elif row['score_range'] <= 3.0:
                return 'low_agreement'
            else:
                return 'poor_agreement'
        
        combined_analysis['stability_classification'] = combined_analysis.apply(classify_variance_stability, axis=1)
        
        # Log summary statistics by analysis type
        for analysis_type in combined_analysis['analysis_type'].unique():
            subset = combined_analysis[combined_analysis['analysis_type'] == analysis_type]
            stability_summary = subset['stability_classification'].value_counts()
            median_range = subset['score_range'].median()
            context.log.info(f"{analysis_type} - Median range: {median_range:.2f}, Stability: {dict(stability_summary)}")
        
        context.log.info(f"Comprehensive variance analysis complete: {len(combined_analysis)} variance measurements")
        
        # Add output metadata
        analysis_type_counts = combined_analysis['analysis_type'].value_counts().to_dict()
        stability_counts = combined_analysis['stability_classification'].value_counts().to_dict()
        
        context.add_output_metadata({
            "variance_measurements": MetadataValue.int(len(combined_analysis)),
            "overall_variance": MetadataValue.int(analysis_type_counts.get('overall_variance', 0)),
            "template_variance": MetadataValue.int(analysis_type_counts.get('template_variance', 0)),
            "model_variance": MetadataValue.int(analysis_type_counts.get('model_variance', 0)),
            "high_agreement": MetadataValue.int(stability_counts.get('high_agreement', 0)),
            "moderate_agreement": MetadataValue.int(stability_counts.get('moderate_agreement', 0)),
            "low_agreement": MetadataValue.int(stability_counts.get('low_agreement', 0)),
            "poor_agreement": MetadataValue.int(stability_counts.get('poor_agreement', 0)),
            "median_score_range": MetadataValue.float(round(float(combined_analysis['score_range'].median()), 2) if not combined_analysis.empty and not pd.isna(combined_analysis['score_range'].median()) else 0.0)
        })
        
        return combined_analysis
    else:
        context.log.warning("No variance patterns found - all evaluations appear to be single instances")
        
        # Add output metadata for empty result
        context.add_output_metadata({
            "variance_measurements": MetadataValue.int(0),
            "source_evaluations": MetadataValue.int(len(valid_scores)),
            "analysis_result": MetadataValue.text("No multi-evaluator patterns found")
        })
        
        return pd.DataFrame()

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
    score_col = 'score'
    analysis_df = valid_scores
    
    def create_pivot_summary(df, group_cols, name_prefix):
        """Create pivot summary with statistics"""
        if df.empty:
            return pd.DataFrame()
            
        grouped = df.groupby(group_cols)[score_col].agg([
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
        analysis_df, ['generation_template'], 'by_generation_template'
    )
    summaries.append(template_summary)
    
    # 2. By Generation Model Provider
    gen_model_summary = create_pivot_summary(
        analysis_df, ['generation_model_provider'], 'by_generation_model_provider'
    )
    summaries.append(gen_model_summary)
    
    # 3. By Evaluation Model Provider
    eval_model_summary = create_pivot_summary(
        analysis_df, ['evaluation_model_provider'], 'by_evaluation_model_provider'
    )
    summaries.append(eval_model_summary)
    
    # 4. By Combo ID
    combo_summary = create_pivot_summary(
        analysis_df, ['combo_id'], 'by_combo_id'
    )
    summaries.append(combo_summary)
    
    # 5. By Template + Generation Model combination
    template_model_summary = create_pivot_summary(
        analysis_df, ['generation_template', 'generation_model_provider'], 'by_template_and_generation_model'
    )
    summaries.append(template_model_summary)
    
    # 6. By Generation Model + Evaluation Model combination
    gen_eval_model_summary = create_pivot_summary(
        analysis_df, ['generation_model_provider', 'evaluation_model_provider'], 'by_generation_vs_evaluation_model'
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
        
        # Add output metadata
        analysis_type_counts = final_summary['analysis_type'].value_counts().to_dict() if 'analysis_type' in final_summary.columns else {}
        
        context.add_output_metadata({
            "summary_rows": MetadataValue.int(len(final_summary)),
            "source_evaluations": MetadataValue.int(len(valid_scores)),
            "analysis_categories": MetadataValue.int(len(analysis_type_counts)),
            "by_template": MetadataValue.int(analysis_type_counts.get('by_generation_template', 0)),
            "by_model": MetadataValue.int(analysis_type_counts.get('by_generation_model_provider', 0)),
            "by_combo": MetadataValue.int(analysis_type_counts.get('by_combo_id', 0)),
            "overall_stats": MetadataValue.int(analysis_type_counts.get('overall_statistics', 0)),
            "columns_included": MetadataValue.text(", ".join(existing_columns))
        })
        
        return final_summary
    else:
        context.log.warning("No valid scores found for analysis")
        
        # Add output metadata for empty result
        context.add_output_metadata({
            "summary_rows": MetadataValue.int(0),
            "source_evaluations": MetadataValue.int(0),
            "analysis_result": MetadataValue.text("No valid scores found for analysis")
        })
        
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
        generation_path = f"data/3_generation/generation_responses/{gen_dir}/{gen_model_full}.txt"
        
        # Reconstruct evaluation response path from the original task ID structure
        # We need to rebuild the complex path structure
        task_id = row.name if hasattr(row, 'name') else ''
        
        # Try to extract the original evaluation task ID pattern from the parsed data
        # This is complex due to the nested directory structure
        eval_base_path = "data/4_evaluation/evaluation_responses"
        
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
    
    # Add output metadata
    evaluator_counts = perfect_scores['evaluation_model_provider'].value_counts().to_dict() if not perfect_scores.empty else {}
    template_counts = perfect_scores['generation_template'].value_counts().to_dict() if not perfect_scores.empty else {}
    
    context.add_output_metadata({
        "perfect_scores": MetadataValue.int(len(result_df)),
        "unique_combinations": MetadataValue.int(result_df['combo_id'].nunique() if 'combo_id' in result_df.columns else 0),
        "unique_templates": MetadataValue.int(result_df['generation_template'].nunique() if 'generation_template' in result_df.columns else 0),
        "unique_evaluators": MetadataValue.int(result_df['evaluation_model_provider'].nunique() if 'evaluation_model_provider' in result_df.columns else 0),
        "deepseek_perfect": MetadataValue.int(evaluator_counts.get('deepseek', 0)),
        "qwen_perfect": MetadataValue.int(evaluator_counts.get('qwen', 0)),
        "google_perfect": MetadataValue.int(evaluator_counts.get('google', 0)),
        "top_template": MetadataValue.text(max(template_counts.keys(), key=template_counts.get) if template_counts else "None")
    })
    
    return result_df