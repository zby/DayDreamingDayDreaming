from dagster import asset
from pathlib import Path
import pandas as pd
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
    concept_combinations, 
    concept_contents, 
    generation_templates, 
    concepts_metadata
) -> str:
    """
    Generate one LLM prompt per partition and save as individual file.
    Each prompt is cached as a separate file for debugging.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = generation_tasks[generation_tasks["generation_task_id"] == task_id].iloc[0]
    
    # Generate prompt using existing logic
    prompts_dict = generate_prompts(
        generation_tasks.iloc[[task_row.name]],  # Single task
        concept_combinations[0],  # combo_df  
        concept_combinations[1],  # combo_relationships
        concept_contents,
        generation_templates,
        concepts_metadata
    )
    
    prompt = prompts_dict[task_id]
    context.log.info(f"Generated prompt for task {task_id}")
    return prompt

@asset(
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="generation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["task_definitions"]  # Ensure partitions are created first
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
    deps=["task_definitions"]  # Ensure partitions are created first
)
def evaluation_prompt(context, evaluation_tasks, generation_response, evaluation_templates) -> str:
    """
    Generate one evaluation prompt per partition and save as individual file.
    Each prompt is cached as a separate file for debugging.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id].iloc[0]
    
    # Generate evaluation prompt using existing logic
    response_dict = {task_row["generation_task_id"]: generation_response}
    eval_prompts_dict = generate_evaluation_prompts(
        response_dict,
        evaluation_tasks.iloc[[task_row.name]],  # Single task
        evaluation_templates
    )
    
    eval_prompt = eval_prompts_dict[task_id]
    context.log.info(f"Generated evaluation prompt for task {task_id}")
    return eval_prompt

@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="llm_evaluation",
    io_manager_key="evaluation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["task_definitions"]  # Ensure partitions are created first
)
def evaluation_response(context, evaluation_prompt, evaluation_tasks) -> str:
    """Generate one evaluation response per partition."""
    task_id = context.partition_key
    
    task_row = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id].iloc[0]
    
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
def parsed_scores(context) -> pd.DataFrame:
    """
    Parse evaluation responses to extract scores and metadata.
    Aggregates all evaluation responses and parses them into structured data.
    """
    # Collect all materialized evaluation responses
    evaluation_responses_path = Path("data/04_evaluation/evaluation_responses")
    evaluation_responses = {}
    
    if evaluation_responses_path.exists():
        for file_path in evaluation_responses_path.glob("*.txt"):
            task_id = file_path.stem
            evaluation_responses[task_id] = file_path.read_text()
    
    # Use existing parse_scores function
    parsed_csv_path = parse_scores(evaluation_responses)
    
    # Load and return the parsed scores
    return pd.read_csv(parsed_csv_path)

@asset(
    group_name="results_processing", 
    io_manager_key="summary_results_io_manager"
)
def final_results(parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Compile final aggregated results and analysis summaries.
    """
    # Add any final aggregation logic here
    # For now, pass through parsed scores
    return parsed_scores