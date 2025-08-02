from dagster import asset
from typing import Dict, Tuple
import pandas as pd
from ..utils.nodes_standalone import (
    load_and_prepare_concepts, 
    generate_concept_combinations,
    create_all_tasks
)

@asset(group_name="daydreaming_experiment", required_resource_keys={"experiment_config"})  
def concepts_and_content(
    context,
    concepts_metadata: pd.DataFrame,
    concept_descriptions_sentence: dict,
    concept_descriptions_paragraph: dict,
    concept_descriptions_article: dict,
) -> Tuple[list[dict[str, str]], dict[str, str]]:  
    # Get parameters from config resource
    config = context.resources.experiment_config
    parameters = config.to_dict()
    
    return load_and_prepare_concepts(
        concepts_metadata, 
        concept_descriptions_sentence,
        concept_descriptions_paragraph,
        concept_descriptions_article,
        parameters
    )

@asset(
    group_name="daydreaming_experiment",
    io_manager_key="partitioned_concept_io_manager"
)
def concept_contents(concepts_and_content) -> dict[str, str]:
    """Save individual concept content files for clean access without ConceptDB dependencies."""
    concepts_list, concept_contents_dict = concepts_and_content
    return concept_contents_dict

@asset(
    group_name="daydreaming_experiment",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config"}
)  
def concept_combinations(context, concepts_and_content) -> Tuple[pd.DataFrame, pd.DataFrame]:  
    """Generate concept combinations and save as CSV files for easy lookup of combo_001, etc."""
    # Get parameters from config resource
    config = context.resources.experiment_config
    parameters = config.to_dict()
    
    concepts_list, concept_contents = concepts_and_content  
    return generate_concept_combinations(concepts_list, parameters)

@asset(
    group_name="daydreaming_experiment", 
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config"}
)
def generation_tasks(
    context,
    concept_combinations: Tuple[pd.DataFrame, pd.DataFrame],
    generation_models: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    generation_templates: dict,
    evaluation_templates: dict,
) -> pd.DataFrame:
    """Create generation tasks and save as CSV for understanding task IDs."""
    tasks, _ = create_all_tasks(
        concept_combinations, 
        generation_models, 
        evaluation_models, 
        generation_templates,
        evaluation_templates
    )
    return tasks

@asset(
    group_name="daydreaming_experiment",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config"}
)
def evaluation_tasks(
    context,
    concept_combinations: Tuple[pd.DataFrame, pd.DataFrame],
    generation_models: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    generation_templates: dict,
    evaluation_templates: dict,
) -> pd.DataFrame:
    """Create evaluation tasks and save as CSV for understanding task IDs."""
    _, tasks = create_all_tasks(
        concept_combinations, 
        generation_models, 
        evaluation_models, 
        generation_templates,
        evaluation_templates
    )
    return tasks