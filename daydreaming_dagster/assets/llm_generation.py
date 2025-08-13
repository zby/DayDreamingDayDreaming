from dagster import asset, Failure, MetadataValue
from pathlib import Path
import pandas as pd
from jinja2 import Environment
from .partitions import generation_tasks_partitions
from ..utils.generation_response_parser import parse_generation_response, get_parsing_strategy, validate_essay_content


@asset(
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="generation_prompt_io_manager",
    deps=["generation_tasks"],  # Ensure partitions are created first
    pool="llm_api"  # Pool-based concurrency control
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
    
    # Get template content from DataFrame
    template_rows = generation_templates[generation_templates["template_id"] == template_id]
    if template_rows.empty:
        available_templates = generation_templates["template_id"].tolist()
        context.log.error(f"Template '{template_id}' not found in generation_templates DataFrame")
        raise Failure(
            description=f"Generation template '{template_id}' not found",
            metadata={
                "template_id": MetadataValue.text(template_id),
                "available_templates": MetadataValue.text(str(available_templates)),
                "total_templates": MetadataValue.int(len(generation_templates))
            }
        )
    
    template_content = template_rows.iloc[0]["content"]
    
    # Render template using ContentCombination.contents (already has name + content)
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
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="parsed_generation_io_manager",
    deps=["generation_tasks"],
    description="Parse generation responses to extract essay portions for evaluation. "
                "Uses template-aware parsing strategies to separate essay content from "
                "thinking/analysis sections. This asset sits between generation and "
                "evaluation in the pipeline.",
    compute_kind="python"
)
def parsed_generation_responses(context, generation_response: str, generation_tasks: pd.DataFrame) -> str:
    """
    Parse generation responses to extract essay portions for evaluation.
    
    This asset implements the parsing step between generation and evaluation:
    1. Loads the full generation response
    2. Determines parsing strategy based on template and model
    3. Extracts the essay portion using appropriate strategy
    4. Validates the extracted essay content
    5. Returns just the essay content as a string
    
    Args:
        context: Dagster asset context (provides partition_key and resources)
        generation_response: Raw LLM generation response text
        generation_tasks: DataFrame with generation task definitions
        
    Returns:
        str: The extracted essay content
    """
    task_id = context.partition_key
    
    # Get the specific generation task for this partition
    matching_tasks = generation_tasks[generation_tasks["generation_task_id"] == task_id]
    if len(matching_tasks) == 0:
        available_tasks = generation_tasks["generation_task_id"].tolist()[:5]
        context.log.error(f"Generation task '{task_id}' not found in generation_tasks DataFrame")
        raise Failure(
            description=f"Generation task '{task_id}' not found in task database",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "available_tasks_sample": MetadataValue.text(str(available_tasks)),
                "total_tasks": MetadataValue.int(len(generation_tasks)),
                "resolution_1": MetadataValue.text("Check if generation_tasks asset was materialized recently"),
                "resolution_2": MetadataValue.text("Run: dagster asset materialize --select generation_tasks")
            }
        )
    
    task_row = matching_tasks.iloc[0]
    template_id = task_row["generation_template"]
    model_name = task_row["generation_model"]
    
    # Determine parsing strategy based on template and model
    strategy = get_parsing_strategy(template_id, model_name)
    context.log.info(f"Using parsing strategy '{strategy}' for template '{template_id}' and model '{model_name}'")
    
    try:
        # Parse the generation response
        parsing_result = parse_generation_response(generation_response, strategy)
        
        # Extract essay content
        essay_content = parsing_result.get("essay", generation_response)  # Fallback to full response
        
        # Validate essay content
        validation = validate_essay_content(essay_content)
        
        # Log parsing results
        parsing_success = essay_content != generation_response
        context.log.info(f"Successfully parsed generation response using strategy '{strategy}'")
        if validation.get("is_valid"):
            context.log.info(f"Essay validation passed: {validation.get('metrics', {}).get('word_count', 0)} words")
        else:
            context.log.warning(f"Essay validation failed: {validation.get('error', 'Unknown error')}")
        
        # Add metadata for monitoring
        context.add_output_metadata({
            "parsing_strategy": MetadataValue.text(strategy),
            "template_id": MetadataValue.text(template_id),
            "model_name": MetadataValue.text(model_name),
            "essay_word_count": MetadataValue.int(len(essay_content.split())),
            "full_response_word_count": MetadataValue.int(len(generation_response.split())),
            "parsing_success": MetadataValue.bool(parsing_success),
            "validation_passed": MetadataValue.bool(validation.get("is_valid", False)),
            "sections_found": MetadataValue.text(str([k for k in ["essay", "thinking", "endnotes"] if parsing_result.get(k) is not None]))
        })
        
        return essay_content
        
    except Exception as e:
        context.log.error(f"Failed to parse generation response for task '{task_id}': {e}")
        
        # Return full response as fallback
        context.log.warning(f"Using full generation response as fallback for task '{task_id}'")
        context.add_output_metadata({
            "parsing_strategy": MetadataValue.text("fallback_error"),
            "parsing_error": MetadataValue.text(str(e)),
            "essay_word_count": MetadataValue.int(len(generation_response.split())),
            "validation_passed": MetadataValue.bool(False)
        })
        
        return generation_response