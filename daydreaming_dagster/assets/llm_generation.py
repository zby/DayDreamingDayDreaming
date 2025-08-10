from dagster import asset, Failure, MetadataValue
from pathlib import Path
import pandas as pd
from jinja2 import Environment
from .partitions import generation_tasks_partitions


@asset(
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="generation_prompt_io_manager",
    deps=["generation_tasks"],  # Ensure partitions are created first
    tags={"dagster/concurrency_key": "llm_api"}
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
    tags={"dagster/concurrency_key": "llm_api"}
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


# Split-pool variants to allow separate global concurrency for free vs paid

@asset(
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="generation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["generation_tasks"],
    tags={"dagster/concurrency_key": "llm_api_free"}
)
def generation_response_free(context, generation_prompt, generation_tasks) -> str:
    """Generate LLM responses for partitions targeting free-tier models (use with free pool)."""
    task_id = context.partition_key
    task_row = generation_tasks[generation_tasks["generation_task_id"] == task_id].iloc[0]
    model_name = task_row["generation_model_name"]
    if ":free" not in (model_name or ""):
        # Mark as failed so a subsequent paid run with "failed/missing" can pick it up
        raise Failure(f"[FREE] Partition {task_id} uses paid model {model_name}; run generation_response_paid instead.")
    prompt = generation_prompt
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(prompt, model=model_name)
    context.log.info(f"[FREE] Generated LLM response for task {task_id} using model {model_name}")
    return response


@asset(
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="generation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["generation_tasks"],
    tags={"dagster/concurrency_key": "llm_api_paid"}
)
def generation_response_paid(context, generation_prompt, generation_tasks) -> str:
    """Generate LLM responses for partitions targeting paid models (use with paid pool)."""
    task_id = context.partition_key
    task_row = generation_tasks[generation_tasks["generation_task_id"] == task_id].iloc[0]
    model_name = task_row["generation_model_name"]
    if ":free" in (model_name or ""):
        # Mark as failed so a subsequent free run with "failed/missing" can pick it up
        raise Failure(f"[PAID] Partition {task_id} uses free model {model_name}; run generation_response_free instead.")
    prompt = generation_prompt
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(prompt, model=model_name)
    context.log.info(f"[PAID] Generated LLM response for task {task_id} using model {model_name}")
    return response