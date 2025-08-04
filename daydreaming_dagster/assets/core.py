from dagster import asset
from typing import Dict, Tuple, List
import pandas as pd
from itertools import combinations
from ..utils.nodes_standalone import (
    create_generation_tasks_from_content_combinations,
    create_evaluation_tasks_from_generation_tasks,
)
from ..models import Concept, ContentCombination
from .partitions import generation_tasks_partitions, evaluation_tasks_partitions

@asset(
    group_name="llm_tasks",
    required_resource_keys={"config"}
)
def content_combinations(
    context,
    concepts: List[Concept],
) -> List[ContentCombination]:
    """Generate k-max combinations of concepts with resolved content using ContentCombination."""
    config = context.resources.config
    k_max = config.k_max
    
    context.log.info(f"Generating content combinations with k_max={k_max}")
    
    # Generate all k-max combinations and create ContentCombination objects
    content_combos = []
    combo_id = 1
    
    for combo in combinations(concepts, k_max):
        combo_id_str = f"combo_{combo_id:03d}"
        
        # Create ContentCombination with resolved content and explicit combo_id
        content_combo = ContentCombination.from_concepts(
            list(combo), 
            config.description_level,
            combo_id=combo_id_str
        )
        content_combos.append(content_combo)
        combo_id += 1
    
    context.log.info(f"Generated {len(content_combos)} content combinations")
    
    return content_combos

@asset(
    group_name="llm_tasks", 
    io_manager_key="csv_io_manager",
    required_resource_keys={"config"}
)
def generation_tasks(
    context,
    content_combinations: List[ContentCombination],
    generation_models: pd.DataFrame,
    generation_templates: dict,
) -> pd.DataFrame:
    """Create generation tasks and save as CSV for understanding task IDs."""
    tasks_df = create_generation_tasks_from_content_combinations(
        content_combinations, 
        generation_models, 
        generation_templates
    )
    
    # Clear existing partitions and register new ones for LLM processing
    existing_partitions = context.instance.get_dynamic_partitions(generation_tasks_partitions.name)
    if existing_partitions:
        context.log.info(f"Removing {len(existing_partitions)} existing partitions")
        for partition in existing_partitions:
            context.instance.delete_dynamic_partition(
                generation_tasks_partitions.name, 
                partition
            )
    
    new_partitions = tasks_df["generation_task_id"].tolist()
    context.instance.add_dynamic_partitions(
        generation_tasks_partitions.name,
        new_partitions
    )
    
    context.log.info(f"Created {len(tasks_df)} generation task partitions")
    
    return tasks_df

@asset(
    group_name="llm_tasks",
    io_manager_key="csv_io_manager",
    required_resource_keys={"config"}
)
def evaluation_tasks(
    context,
    generation_tasks: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    evaluation_templates: dict,
) -> pd.DataFrame:
    """Create evaluation tasks and save as CSV for understanding task IDs."""
    # COMMENTED OUT: Multiple runs feature for future use
    # config = context.resources.config
    # num_evaluation_runs = getattr(config, 'num_evaluation_runs', 3)  # Default to 3 runs
    
    tasks_df = create_evaluation_tasks_from_generation_tasks(
        generation_tasks,
        evaluation_models, 
        evaluation_templates
        # num_evaluation_runs=num_evaluation_runs  # COMMENTED OUT
    )
    
    # Clear existing partitions and register new ones for LLM processing
    existing_partitions = context.instance.get_dynamic_partitions(evaluation_tasks_partitions.name)
    if existing_partitions:
        context.log.info(f"Removing {len(existing_partitions)} existing evaluation partitions")
        for partition in existing_partitions:
            context.instance.delete_dynamic_partition(
                evaluation_tasks_partitions.name, 
                partition
            )
    
    new_partitions = tasks_df["evaluation_task_id"].tolist()
    context.instance.add_dynamic_partitions(
        evaluation_tasks_partitions.name,
        new_partitions
    )
    
    context.log.info(f"Created {len(tasks_df)} evaluation task partitions")
    
    return tasks_df