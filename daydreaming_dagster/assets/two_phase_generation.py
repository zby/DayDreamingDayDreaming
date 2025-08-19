from dagster import asset, Failure, MetadataValue, AutoMaterializePolicy
from pathlib import Path
import pandas as pd
from jinja2 import Environment
from .partitions import generation_tasks_partitions
from ..utils.template_loader import load_generation_template


@asset(
    partitions_def=generation_tasks_partitions,
    group_name="two_phase_generation",
    io_manager_key="links_prompt_io_manager",
    deps=["generation_tasks"],
)
def links_prompt(
    context, 
    generation_tasks, 
    content_combinations, 
    generation_templates
) -> str:
    """
    Generate Phase 1 prompts for concept link generation.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    matching_tasks = generation_tasks[generation_tasks["generation_task_id"] == task_id]
    if matching_tasks.empty:
        available_tasks = generation_tasks["generation_task_id"].tolist()[:5]
        context.log.error(f"Task ID '{task_id}' not found in generation_tasks DataFrame")
        raise Failure(
            description=f"Generation task '{task_id}' not found in task database",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "available_tasks_sample": MetadataValue.text(str(available_tasks)),
                "total_tasks": MetadataValue.int(len(generation_tasks)),
                "resolution_1": MetadataValue.text("Check if generation_tasks asset was materialized recently"),
                "resolution_2": MetadataValue.text("Run: dagster asset materialize --select generation_tasks"),
            }
        )
    task_row = matching_tasks.iloc[0]
    combo_id = task_row["combo_id"]
    template_name = task_row["generation_template"]
    
    # Find the ContentCombination for this combo_id
    content_combination = None
    for combo in content_combinations:
        if combo.combo_id == combo_id:
            content_combination = combo
            break
    
    if content_combination is None:
        available_combos = [combo.combo_id for combo in content_combinations[:5]]
        context.log.error(f"No ContentCombination found for combo_id: {combo_id}")
        raise Failure(
            description=f"Content combination '{combo_id}' not found in combinations database",
            metadata={
                "combo_id": MetadataValue.text(combo_id),
                "available_combinations_sample": MetadataValue.text(str(available_combos)),
                "total_combinations": MetadataValue.int(len(content_combinations)),
            }
        )
    
    # Load the links phase template
    try:
        template_content = load_generation_template(template_name, "links")
    except FileNotFoundError as e:
        context.log.error(f"Links template '{template_name}' not found")
        raise Failure(
            description=f"Links template '{template_name}' not found",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("links"),
                "error": MetadataValue.text(str(e)),
                "resolution": MetadataValue.text("Ensure the template exists in data/1_raw/generation_templates/links/")
            }
        )
    
    # Render template using ContentCombination.contents
    env = Environment()
    template = env.from_string(template_content)
    prompt = template.render(concepts=content_combination.contents)
    
    context.log.info(f"Generated links prompt for task {task_id} using template {template_name}")
    return prompt


@asset(
    partitions_def=generation_tasks_partitions,
    group_name="two_phase_generation",
    io_manager_key="links_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["generation_tasks"],
)
def links_response(context, links_prompt, generation_tasks) -> str:
    """
    Generate Phase 1 LLM responses for concept links.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = generation_tasks[generation_tasks["generation_task_id"] == task_id].iloc[0]
    
    # Use the model name directly from the task
    model_name = task_row["generation_model_name"]
    
    # Generate using Dagster LLM resource
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(links_prompt, model=model_name)
    
    context.log.info(f"Generated links response for task {task_id} using model {model_name}")
    return response


@asset(
    partitions_def=generation_tasks_partitions,
    group_name="two_phase_generation",
    io_manager_key="essay_prompt_io_manager",
    deps=["generation_tasks"],
)
def essay_prompt(
    context, 
    links_response,
    generation_tasks, 
    content_combinations
) -> str:
    """
    Generate Phase 2 prompts for essay generation based on Phase 1 links.
    Hard fails if links are missing or insufficient.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    matching_tasks = generation_tasks[generation_tasks["generation_task_id"] == task_id]
    if matching_tasks.empty:
        context.log.error(f"Task ID '{task_id}' not found in generation_tasks DataFrame")
        raise Failure(
            description=f"Generation task '{task_id}' not found in task database",
            metadata={"task_id": MetadataValue.text(task_id)}
        )
    task_row = matching_tasks.iloc[0]
    template_name = task_row["generation_template"]
    
    # Use links_response directly from asset dependency
    links_content = links_response
    context.log.info(f"Loaded {len(links_content)} chars from links response for task {task_id}")
    
    # Validate links content - fail hard if insufficient
    links_lines = [line.strip() for line in links_content.split('\n') if line.strip()]
    if len(links_lines) < 3:
        raise Failure(
            description=f"Insufficient links generated for task {task_id}",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "links_line_count": MetadataValue.int(len(links_lines)),
                "minimum_required": MetadataValue.int(3),
                "links_content_preview": MetadataValue.text(links_content[:200] + "..." if len(links_content) > 200 else links_content),
                "resolution": MetadataValue.text("Re-run links_response or check prompt quality"),
            }
        )
    
    # Load the essay phase template
    try:
        template_content = load_generation_template(template_name, "essay")
    except FileNotFoundError as e:
        context.log.error(f"Essay template '{template_name}' not found")
        raise Failure(
            description=f"Essay template '{template_name}' not found",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("essay"),
                "error": MetadataValue.text(str(e)),
                "resolution": MetadataValue.text("Ensure the template exists in data/1_raw/generation_templates/essay/")
            }
        )
    
    # Render essay template with links block
    env = Environment()
    template = env.from_string(template_content)
    prompt = template.render(links_block=links_content)
    
    context.log.info(f"Generated essay prompt for task {task_id} with {len(links_lines)} links")
    context.add_output_metadata({
        "links_line_count": MetadataValue.int(len(links_lines)),
        "fk_relationship": MetadataValue.text(f"essay_prompt:{task_id} -> links_response:{task_id}")
    })
    
    return prompt


@asset(
    partitions_def=generation_tasks_partitions,
    group_name="two_phase_generation",
    io_manager_key="essay_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["generation_tasks"],
)
def essay_response(context, essay_prompt, generation_tasks) -> str:
    """
    Generate Phase 2 LLM responses for essays.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = generation_tasks[generation_tasks["generation_task_id"] == task_id].iloc[0]
    
    # Use the model name directly from the task
    model_name = task_row["generation_model_name"]
    
    # Generate using Dagster LLM resource
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(essay_prompt, model=model_name)
    
    context.log.info(f"Generated essay response for task {task_id} using model {model_name}")
    return response


