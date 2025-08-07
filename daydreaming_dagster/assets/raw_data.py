from dagster import asset, MetadataValue, Failure
import pandas as pd
from pathlib import Path
from typing import List

from ..models import Concept
from ..resources.experiment_config import ExperimentConfig

@asset(group_name="raw_data", required_resource_keys={"data_root"})
def concepts_metadata(context) -> pd.DataFrame:
    """Load ALL concepts metadata from CSV - no filtering."""
    data_root = context.resources.data_root
    metadata_path = Path(data_root) / "1_raw" / "concepts" / "concepts_metadata.csv"
    
    if not metadata_path.exists():
        raise FileNotFoundError(f"Required concepts metadata CSV not found: {metadata_path}")
    
    df = pd.read_csv(metadata_path)
    
    context.add_output_metadata({
        "total_concepts": MetadataValue.int(len(df)),
        "active_concepts": MetadataValue.int(len(df[df["active"] == True]) if "active" in df.columns else 0),
        "inactive_concepts": MetadataValue.int(len(df[df["active"] == False]) if "active" in df.columns else 0),
        "source_file": MetadataValue.text(str(metadata_path))
    })
    
    return df

@asset(group_name="raw_data")
def concepts(context, concepts_metadata: pd.DataFrame, config: ExperimentConfig) -> List[Concept]:
    """Create concept objects from metadata DataFrame."""
    total_available = len(concepts_metadata)
    
    # Filter for active concepts first
    if "active" in concepts_metadata.columns:
        concepts_metadata = concepts_metadata[concepts_metadata["active"] == True]
    
    # Apply concept filtering if configured
    if config.concept_ids_filter:
        concepts_metadata = concepts_metadata[
            concepts_metadata["concept_id"].isin(config.concept_ids_filter)
        ]
    
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
    for _, row in concepts_metadata.iterrows():
        concept_id = row["concept_id"]
        name = row["name"]
        
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

@asset(group_name="raw_data", required_resource_keys={"data_root"})
def llm_models(context) -> pd.DataFrame:
    """Load ALL LLM models from CSV - no filtering."""
    data_root = context.resources.data_root
    models_path = Path(data_root) / "1_raw" / "llm_models.csv"
    
    if not models_path.exists():
        raise FileNotFoundError(f"Required LLM models CSV not found: {models_path}")
    
    df = pd.read_csv(models_path)
    
    context.add_output_metadata({
        "total_models": MetadataValue.int(len(df)),
        "generation_models": MetadataValue.int(len(df[df["for_generation"] == True])),
        "evaluation_models": MetadataValue.int(len(df[df["for_evaluation"] == True])),
        "source_file": MetadataValue.text(str(models_path))
    })
    
    return df

@asset(group_name="raw_data", required_resource_keys={"data_root"})
def generation_templates(context) -> pd.DataFrame:
    """Load generation templates CSV with template file content."""
    data_root = context.resources.data_root
    metadata_path = Path(data_root) / "1_raw" / "generation_templates.csv"
    templates_dir = Path(data_root) / "1_raw" / "generation_templates"
    
    if not metadata_path.exists():
        raise Failure(f"Required generation templates CSV not found: {metadata_path}")
    
    # Load CSV metadata
    templates_df = pd.read_csv(metadata_path)
    
    # Add content column by reading template files in explicit loop
    content_list = []
    for _, row in templates_df.iterrows():
        template_id = row["template_id"]
        template_file = templates_dir / f"{template_id}.txt"
        
        if not template_file.exists():
            raise Failure(f"Required template file not found: {template_file}")
        
        content = template_file.read_text().strip()
        content_list.append(content)
        context.log.info(f"Loaded template content for {template_id}")
    
    templates_df['content'] = content_list
    
    context.add_output_metadata({
        "total_templates": MetadataValue.int(len(templates_df)),
        "active_templates": MetadataValue.int(len(templates_df[templates_df["active"] == True])),
        "inactive_templates": MetadataValue.int(len(templates_df[templates_df["active"] == False])),
        "source_csv": MetadataValue.text(str(metadata_path)),
        "templates_dir": MetadataValue.text(str(templates_dir))
    })
    
    return templates_df

@asset(group_name="raw_data", required_resource_keys={"data_root"})
def evaluation_templates(context) -> pd.DataFrame:
    """Load evaluation templates CSV with template file content."""
    data_root = context.resources.data_root
    metadata_path = Path(data_root) / "1_raw" / "evaluation_templates.csv"
    templates_dir = Path(data_root) / "1_raw" / "evaluation_templates"
    
    if not metadata_path.exists():
        raise Failure(f"Required evaluation templates CSV not found: {metadata_path}")
    
    # Load CSV metadata
    templates_df = pd.read_csv(metadata_path)
    
    # Add content column by reading template files in explicit loop
    content_list = []
    for _, row in templates_df.iterrows():
        template_id = row["template_id"]
        template_file = templates_dir / f"{template_id}.txt"
        
        if not template_file.exists():
            raise Failure(f"Required template file not found: {template_file}")
        
        content = template_file.read_text().strip()
        content_list.append(content)
        context.log.info(f"Loaded template content for {template_id}")
    
    templates_df['content'] = content_list
    
    context.add_output_metadata({
        "total_templates": MetadataValue.int(len(templates_df)),
        "active_templates": MetadataValue.int(len(templates_df[templates_df["active"] == True])),
        "inactive_templates": MetadataValue.int(len(templates_df[templates_df["active"] == False])),
        "source_csv": MetadataValue.text(str(metadata_path)),
        "templates_dir": MetadataValue.text(str(templates_dir))
    })
    
    return templates_df