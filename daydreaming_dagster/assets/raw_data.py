from dagster import asset
import pandas as pd
from pathlib import Path
from typing import List

from ..models import Concept
from ..resources.experiment_config import ExperimentConfig

@asset(group_name="raw_data")
def concepts(config: ExperimentConfig) -> List[Concept]:
    """Load concepts from metadata CSV and description files."""
    # Load metadata
    metadata_df = pd.read_csv("data/1_raw/concepts/concepts_metadata.csv")
    
    # Load all description levels
    description_levels = ["sentence", "paragraph", "article"]
    all_descriptions = {}
    
    for level in description_levels:
        level_descriptions = {}
        level_path = Path(f"data/1_raw/concepts/descriptions-{level}")
        if level_path.exists():
            for file_path in level_path.glob("*.txt"):
                concept_id = file_path.stem
                level_descriptions[concept_id] = file_path.read_text().strip()
        all_descriptions[level] = level_descriptions
    
    # Build concept objects
    concepts = []
    for _, row in metadata_df.iterrows():
        concept_id = row["concept_id"]
        name = row["name"]
        
        # Apply filter if provided
        if config.concept_ids_filter is not None:
            if concept_id not in config.concept_ids_filter:
                continue
        
        # Collect all available descriptions for this concept
        descriptions = {}
        for level in description_levels:
            if concept_id in all_descriptions[level]:
                descriptions[level] = all_descriptions[level][concept_id]
        
        concepts.append(Concept(concept_id=concept_id, name=name, descriptions=descriptions))
    
    return concepts

@asset(group_name="raw_data")
def concepts_metadata(concepts: List[Concept]) -> pd.DataFrame:
    """Create concepts metadata DataFrame for backward compatibility."""
    return pd.DataFrame([
        {"concept_id": c.concept_id, "name": c.name}
        for c in concepts
    ])

@asset(group_name="raw_data")
def generation_models() -> pd.DataFrame:
    """Load generation models configuration."""
    # Filter for generation models only
    models_df = pd.read_csv("data/1_raw/llm_models.csv")
    return models_df[models_df["for_generation"] == True]

@asset(group_name="raw_data")
def evaluation_models() -> pd.DataFrame:
    """Load evaluation models configuration."""
    # Filter for evaluation models only
    models_df = pd.read_csv("data/1_raw/llm_models.csv")
    return models_df[models_df["for_evaluation"] == True]

@asset(group_name="raw_data")
def generation_templates_metadata() -> pd.DataFrame:
    """Load generation templates metadata."""
    return pd.read_csv("data/1_raw/generation_templates.csv")

@asset(group_name="raw_data", required_resource_keys={"config"})
def generation_templates(context, generation_templates_metadata: pd.DataFrame) -> dict[str, str]:
    """Load generation templates based on CSV configuration and active status."""
    config = context.resources.config
    templates = {}
    templates_path = Path("data/1_raw/generation_templates")
    
    # Filter to only active templates
    active_templates = generation_templates_metadata[generation_templates_metadata["active"] == True]
    
    for _, row in active_templates.iterrows():
        template_id = row["template_id"]
        
        # Apply additional filter if provided
        if config.template_names_filter is not None:
            if template_id not in config.template_names_filter:
                continue
        
        template_file = templates_path / f"{template_id}.txt"
        if template_file.exists():
            templates[template_id] = template_file.read_text()
    
    return templates

@asset(group_name="raw_data")
def evaluation_templates_metadata() -> pd.DataFrame:
    """Load evaluation templates metadata."""
    return pd.read_csv("data/1_raw/evaluation_templates.csv")

@asset(group_name="raw_data", required_resource_keys={"config"})
def evaluation_templates(context, evaluation_templates_metadata: pd.DataFrame) -> dict[str, str]:
    """Load evaluation templates based on CSV configuration and active status."""
    config = context.resources.config
    templates = {}
    templates_path = Path("data/1_raw/evaluation_templates")
    
    # Filter to only active templates
    active_templates = evaluation_templates_metadata[evaluation_templates_metadata["active"] == True]
    
    for _, row in active_templates.iterrows():
        template_id = row["template_id"]
        
        # Note: Currently no separate eval template filter, but could be added to ExperimentConfig
        template_file = templates_path / f"{template_id}.txt"
        if template_file.exists():
            templates[template_id] = template_file.read_text()
    
    return templates