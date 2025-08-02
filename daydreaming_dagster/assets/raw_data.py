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
    metadata_df = pd.read_csv("data/01_raw/concepts/concepts_metadata.csv")
    
    # Load all description levels
    description_levels = ["sentence", "paragraph", "article"]
    all_descriptions = {}
    
    for level in description_levels:
        level_descriptions = {}
        level_path = Path(f"data/01_raw/concepts/descriptions/{level}")
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
    return pd.read_csv("data/01_raw/generation_models.csv")

@asset(group_name="raw_data")
def evaluation_models() -> pd.DataFrame:
    """Load evaluation models configuration."""
    return pd.read_csv("data/01_raw/evaluation_models.csv")

@asset(group_name="raw_data")
def generation_templates(config: ExperimentConfig) -> dict[str, str]:
    """Load generation templates."""
    templates = {}
    templates_path = Path("data/01_raw/generation_templates")
    for file_path in templates_path.glob("*.txt"):
        template_name = file_path.stem
        
        # Apply filter if provided
        if config.template_names_filter is not None:
            if template_name not in config.template_names_filter:
                continue
        
        templates[template_name] = file_path.read_text()
    return templates

@asset(group_name="raw_data")
def evaluation_templates(config: ExperimentConfig) -> dict[str, str]:
    """Load evaluation templates."""
    templates = {}
    templates_path = Path("data/01_raw/evaluation_templates")
    for file_path in templates_path.glob("*.txt"):
        template_name = file_path.stem
        
        # Apply filter if provided (note: evaluation templates use same filter as generation for simplicity)
        if config.template_names_filter is not None:
            # For evaluation templates, we'll load all since they have different names
            # This is a simplification - in practice you might want separate eval template filtering
            pass
        
        templates[template_name] = file_path.read_text()
    return templates