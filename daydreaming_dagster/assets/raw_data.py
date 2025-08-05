from dagster import asset, MetadataValue
import pandas as pd
from pathlib import Path
from typing import List

from ..models import Concept
from ..resources.experiment_config import ExperimentConfig

@asset(group_name="raw_data")
def concepts(context, config: ExperimentConfig) -> List[Concept]:
    """Load concepts from metadata CSV and description files."""
    # Load metadata
    metadata_df = pd.read_csv("data/1_raw/concepts/concepts_metadata.csv")
    total_available = len(metadata_df)
    
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
    
    # Add output metadata
    filtered_out = total_available - len(concepts)
    context.add_output_metadata({
        "concept_count": MetadataValue.int(len(concepts)),
        "total_available": MetadataValue.int(total_available),
        "filtered_out": MetadataValue.int(filtered_out),
        "description_level": MetadataValue.text(config.description_level),
        "has_filter": MetadataValue.bool(config.concept_ids_filter is not None),
        "filter_applied": MetadataValue.text(str(config.concept_ids_filter) if config.concept_ids_filter else "None")
    })
    
    return concepts

@asset(group_name="raw_data")
def concepts_metadata(context, concepts: List[Concept]) -> pd.DataFrame:
    """Create concepts metadata DataFrame for backward compatibility."""
    df = pd.DataFrame([
        {"concept_id": c.concept_id, "name": c.name}
        for c in concepts
    ])
    
    # Add output metadata
    context.add_output_metadata({
        "row_count": MetadataValue.int(len(df)),
        "column_count": MetadataValue.int(len(df.columns)),
        "columns": MetadataValue.text(", ".join(df.columns))
    })
    
    return df

@asset(group_name="raw_data")
def generation_models(context) -> pd.DataFrame:
    """Load generation models configuration."""
    # Filter for generation models only
    models_df = pd.read_csv("data/1_raw/llm_models.csv")
    total_models = len(models_df)
    active_models_df = models_df[models_df["for_generation"] == True]
    inactive_models = total_models - len(active_models_df)
    
    # Add output metadata
    context.add_output_metadata({
        "active_models": MetadataValue.int(len(active_models_df)),
        "total_models": MetadataValue.int(total_models),
        "inactive_models": MetadataValue.int(inactive_models),
        "model_providers": MetadataValue.text(", ".join(active_models_df["provider"].unique()) if "provider" in active_models_df.columns else "N/A")
    })
    
    return active_models_df

@asset(group_name="raw_data")
def evaluation_models(context) -> pd.DataFrame:
    """Load evaluation models configuration."""
    # Filter for evaluation models only
    models_df = pd.read_csv("data/1_raw/llm_models.csv")
    total_models = len(models_df)
    active_models_df = models_df[models_df["for_evaluation"] == True]
    inactive_models = total_models - len(active_models_df)
    
    # Add output metadata
    context.add_output_metadata({
        "active_models": MetadataValue.int(len(active_models_df)),
        "total_models": MetadataValue.int(total_models),
        "inactive_models": MetadataValue.int(inactive_models),
        "model_providers": MetadataValue.text(", ".join(active_models_df["provider"].unique()) if "provider" in active_models_df.columns else "N/A")
    })
    
    return active_models_df

@asset(group_name="raw_data")
def generation_templates_metadata(context) -> pd.DataFrame:
    """Load generation templates metadata."""
    df = pd.read_csv("data/1_raw/generation_templates.csv")
    
    # Add output metadata
    context.add_output_metadata({
        "template_count": MetadataValue.int(len(df)),
        "column_count": MetadataValue.int(len(df.columns)),
        "columns": MetadataValue.text(", ".join(df.columns)),
        "active_templates": MetadataValue.int(len(df[df["active"] == True]) if "active" in df.columns else len(df))
    })
    
    return df

@asset(group_name="raw_data", required_resource_keys={"config"})
def generation_templates(context, generation_templates_metadata: pd.DataFrame) -> dict[str, str]:
    """Load generation templates based on CSV configuration and active status."""
    config = context.resources.config
    templates = {}
    templates_path = Path("data/1_raw/generation_templates")
    
    # Filter to only active templates
    active_templates = generation_templates_metadata[generation_templates_metadata["active"] == True]
    total_available = len(active_templates)
    
    for _, row in active_templates.iterrows():
        template_id = row["template_id"]
        
        # Apply additional filter if provided
        if config.template_names_filter is not None:
            if template_id not in config.template_names_filter:
                continue
        
        template_file = templates_path / f"{template_id}.txt"
        if template_file.exists():
            templates[template_id] = template_file.read_text()
    
    # Add output metadata
    filtered_out = total_available - len(templates)
    context.add_output_metadata({
        "loaded_templates": MetadataValue.int(len(templates)),
        "total_available": MetadataValue.int(total_available),
        "filtered_out": MetadataValue.int(filtered_out),
        "template_names": MetadataValue.text(", ".join(templates.keys())),
        "has_filter": MetadataValue.bool(config.template_names_filter is not None),
        "filter_applied": MetadataValue.text(str(config.template_names_filter) if config.template_names_filter else "None")
    })
    
    return templates

@asset(group_name="raw_data")
def evaluation_templates_metadata(context) -> pd.DataFrame:
    """Load evaluation templates metadata."""
    df = pd.read_csv("data/1_raw/evaluation_templates.csv")
    
    # Add output metadata
    context.add_output_metadata({
        "template_count": MetadataValue.int(len(df)),
        "column_count": MetadataValue.int(len(df.columns)),
        "columns": MetadataValue.text(", ".join(df.columns)),
        "active_templates": MetadataValue.int(len(df[df["active"] == True]) if "active" in df.columns else len(df))
    })
    
    return df

@asset(group_name="raw_data", required_resource_keys={"config"})
def evaluation_templates(context, evaluation_templates_metadata: pd.DataFrame) -> dict[str, str]:
    """Load evaluation templates based on CSV configuration and active status."""
    config = context.resources.config
    templates = {}
    templates_path = Path("data/1_raw/evaluation_templates")
    
    # Filter to only active templates
    active_templates = evaluation_templates_metadata[evaluation_templates_metadata["active"] == True]
    total_available = len(active_templates)
    
    for _, row in active_templates.iterrows():
        template_id = row["template_id"]
        
        # Note: Currently no separate eval template filter, but could be added to ExperimentConfig
        template_file = templates_path / f"{template_id}.txt"
        if template_file.exists():
            templates[template_id] = template_file.read_text()
    
    # Add output metadata
    context.add_output_metadata({
        "loaded_templates": MetadataValue.int(len(templates)),
        "total_available": MetadataValue.int(total_available),
        "template_names": MetadataValue.text(", ".join(templates.keys())),
        "templates_with_content": MetadataValue.int(sum(1 for t in templates.values() if t.strip()))
    })
    
    return templates