from dagster import asset, MetadataValue
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
    
    # Add output metadata
    context.add_output_metadata({
        "combination_count": MetadataValue.int(len(content_combos)),
        "k_max_used": MetadataValue.int(k_max),
        "source_concepts": MetadataValue.int(len(concepts)),
        "description_level": MetadataValue.text(config.description_level),
        "combo_ids_range": MetadataValue.text(f"combo_001 to combo_{len(content_combos):03d}")
    })
    
    return content_combos

@asset(
    group_name="llm_tasks",
    io_manager_key="csv_io_manager"
)
def content_combinations_csv(
    context,
    content_combinations: List[ContentCombination],
) -> pd.DataFrame:
    """Export content combinations as normalized relational table with combo_id and concept_id columns."""
    # Create normalized rows: one row per concept in each combination
    rows = []
    for combo in content_combinations:
        for concept_id in combo.concept_ids:
            rows.append({
                "combo_id": combo.combo_id,
                "concept_id": concept_id
            })
    
    df = pd.DataFrame(rows)
    
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "unique_combinations": MetadataValue.int(len(content_combinations)),
        "unique_concepts": MetadataValue.int(df["concept_id"].nunique()),
        "sample_rows": MetadataValue.text(str(df.head(5).to_dict("records")))
    })
    
    return df

@asset(
    group_name="llm_tasks", 
    io_manager_key="csv_io_manager",
    required_resource_keys={"config"}
)
def generation_tasks(
    context,
    content_combinations: List[ContentCombination],
    llm_models: pd.DataFrame,
    generation_templates: dict,
    generation_templates_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Create generation tasks and save as CSV for understanding task IDs."""
    # Filter for generation models
    generation_models = llm_models[llm_models["for_generation"] == True]
    
    # Filter for active templates based on metadata
    active_template_metadata = generation_templates_metadata[generation_templates_metadata["active"] == True]
    active_template_ids = set(active_template_metadata["template_id"].tolist())
    
    # Filter generation_templates dict to only include active templates
    active_generation_templates = {
        template_id: template_content 
        for template_id, template_content in generation_templates.items()
        if template_id in active_template_ids
    }
    
    tasks_df = create_generation_tasks_from_content_combinations(
        content_combinations, 
        generation_models, 
        active_generation_templates
    )
    
    # Clear existing partitions and register new ones for LLM processing
    existing_partitions = context.instance.get_dynamic_partitions(generation_tasks_partitions.name)
    partitions_removed = len(existing_partitions) if existing_partitions else 0
    
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
    
    # Add output metadata
    context.add_output_metadata({
        "task_count": MetadataValue.int(len(tasks_df)),
        "partitions_created": MetadataValue.int(len(new_partitions)),
        "partitions_removed": MetadataValue.int(partitions_removed),
        "unique_combinations": MetadataValue.int(tasks_df["combo_id"].nunique()),
        "unique_templates": MetadataValue.int(tasks_df["generation_template"].nunique()),
        "unique_models": MetadataValue.int(tasks_df["generation_model"].nunique())
    })
    
    return tasks_df

@asset(
    group_name="llm_tasks",
    io_manager_key="csv_io_manager",
    required_resource_keys={"config"}
)
def evaluation_tasks(
    context,
    generation_tasks: pd.DataFrame,
    llm_models: pd.DataFrame,
    evaluation_templates: dict,
    evaluation_templates_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Create evaluation tasks and save as CSV for understanding task IDs."""
    # COMMENTED OUT: Multiple runs feature for future use
    # config = context.resources.config
    # num_evaluation_runs = getattr(config, 'num_evaluation_runs', 3)  # Default to 3 runs
    
    # Filter for evaluation models
    evaluation_models = llm_models[llm_models["for_evaluation"] == True]
    
    # Filter for active templates based on metadata
    active_template_metadata = evaluation_templates_metadata[evaluation_templates_metadata["active"] == True]
    active_template_ids = set(active_template_metadata["template_id"].tolist())
    
    # Filter evaluation_templates dict to only include active templates
    active_evaluation_templates = {
        template_id: template_content 
        for template_id, template_content in evaluation_templates.items()
        if template_id in active_template_ids
    }
    
    tasks_df = create_evaluation_tasks_from_generation_tasks(
        generation_tasks,
        evaluation_models, 
        active_evaluation_templates
        # num_evaluation_runs=num_evaluation_runs  # COMMENTED OUT
    )
    
    # Clear existing partitions and register new ones for LLM processing
    existing_partitions = context.instance.get_dynamic_partitions(evaluation_tasks_partitions.name)
    partitions_removed = len(existing_partitions) if existing_partitions else 0
    
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
    
    # Add output metadata
    context.add_output_metadata({
        "task_count": MetadataValue.int(len(tasks_df)),
        "partitions_created": MetadataValue.int(len(new_partitions)),
        "partitions_removed": MetadataValue.int(partitions_removed),
        "unique_generation_tasks": MetadataValue.int(tasks_df["generation_task_id"].nunique()),
        "unique_eval_templates": MetadataValue.int(tasks_df["evaluation_template"].nunique()),
        "unique_eval_models": MetadataValue.int(tasks_df["evaluation_model"].nunique())
    })
    
    return tasks_df