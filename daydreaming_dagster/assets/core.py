from dagster import asset
from typing import Dict, Tuple, List
import pandas as pd
from itertools import combinations
from ..utils.nodes_standalone import create_tasks_from_content_combinations
from ..models import Concept, ContentCombination

@asset(
    group_name="daydreaming_experiment",
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
    group_name="daydreaming_experiment", 
    io_manager_key="csv_io_manager",
    required_resource_keys={"config"}
)
def generation_tasks(
    context,
    content_combinations: List[ContentCombination],
    generation_models: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    generation_templates: dict,
    evaluation_templates: dict,
) -> pd.DataFrame:
    """Create generation tasks and save as CSV for understanding task IDs."""
    tasks, _ = create_tasks_from_content_combinations(
        content_combinations, 
        generation_models, 
        evaluation_models, 
        generation_templates,
        evaluation_templates
    )
    return tasks

@asset(
    group_name="daydreaming_experiment",
    io_manager_key="csv_io_manager",
    required_resource_keys={"config"}
)
def evaluation_tasks(
    context,
    content_combinations: List[ContentCombination],
    generation_models: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    generation_templates: dict,
    evaluation_templates: dict,
) -> pd.DataFrame:
    """Create evaluation tasks and save as CSV for understanding task IDs."""
    _, tasks = create_tasks_from_content_combinations(
        content_combinations, 
        generation_models, 
        evaluation_models, 
        generation_templates,
        evaluation_templates
    )
    return tasks