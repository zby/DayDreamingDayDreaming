from dagster import asset, MetadataValue, Failure
import pandas as pd
from pathlib import Path
from typing import List

from ..models import Concept

@asset(group_name="raw_data", required_resource_keys={"data_root"})
def concepts(context) -> List[Concept]:
    """Load ALL concepts from CSV with description files, applying active filtering."""
    data_root = context.resources.data_root
    metadata_path = Path(data_root) / "1_raw" / "concepts" / "concepts_metadata.csv"
    
    if not metadata_path.exists():
        raise Failure(f"Required concepts metadata CSV not found: {metadata_path}")
    
    # Load CSV metadata
    concepts_df = pd.read_csv(metadata_path)
    total_available = len(concepts_df)
    
    # Filter for active concepts first
    if "active" in concepts_df.columns:
        concepts_df = concepts_df[concepts_df["active"] == True]
    
    
    # Load all description levels
    description_levels = ["sentence", "paragraph", "article"]
    all_descriptions = {}
    
    for level in description_levels:
        level_descriptions = {}
        level_path = Path(data_root) / "1_raw" / "concepts" / f"descriptions-{level}"
        if level_path.exists():
            for file_path in level_path.glob("*.txt"):
                concept_id = file_path.stem
                level_descriptions[concept_id] = file_path.read_text().strip()
        all_descriptions[level] = level_descriptions
    
    # Build concept objects (explicit loop)
    concepts = []
    for _, row in concepts_df.iterrows():
        concept_id = row["concept_id"]
        name = row["name"]
        
        # Collect all available descriptions for this concept
        descriptions = {}
        for level in description_levels:
            if concept_id in all_descriptions[level]:
                descriptions[level] = all_descriptions[level][concept_id]
        
        concepts.append(Concept(concept_id=concept_id, name=name, descriptions=descriptions))
        context.log.info(f"Loaded concept {concept_id} with {len(descriptions)} description levels")
    
    # Add output metadata
    filtered_out = total_available - len(concepts)
    context.add_output_metadata({
        "concept_count": MetadataValue.int(len(concepts)),
        "total_available": MetadataValue.int(total_available),
        "filtered_out": MetadataValue.int(filtered_out),
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