from dagster import asset, MetadataValue
import pandas as pd
from pathlib import Path
from typing import List

from ..models import Concept
from ..resources.experiment_config import ExperimentConfig

@asset(group_name="raw_data", io_manager_key="enhanced_csv_io_manager")
def concepts_metadata(context) -> pd.DataFrame:
    """Load concepts metadata from CSV - now the source of truth."""
    # Enhanced CSVIOManager handles loading from source via source_mappings
    # Return empty placeholder - the real loading happens during input loading
    return pd.DataFrame()  # Placeholder - I/O manager will handle source loading

@asset(group_name="raw_data")
def concepts(context, concepts_metadata: pd.DataFrame, config: ExperimentConfig) -> List[Concept]:
    """Create concept objects from metadata DataFrame."""
    total_available = len(concepts_metadata)
    
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

@asset(group_name="raw_data", io_manager_key="enhanced_csv_io_manager")
def generation_models(context) -> pd.DataFrame:
    """Load generation models - filtering handled by I/O manager."""
    # Enhanced CSVIOManager loads from data/1_raw/llm_models.csv 
    # and filters for for_generation=True via source_mappings
    return pd.DataFrame()  # Placeholder - I/O manager handles actual loading and filtering

@asset(group_name="raw_data", io_manager_key="enhanced_csv_io_manager") 
def evaluation_models(context) -> pd.DataFrame:
    """Load evaluation models - filtering handled by I/O manager."""
    # Enhanced CSVIOManager loads from data/1_raw/llm_models.csv
    # and filters for for_evaluation=True via source_mappings
    return pd.DataFrame()  # Placeholder - I/O manager handles actual loading and filtering

@asset(group_name="raw_data", io_manager_key="generation_template_io_manager")
def generation_templates(context) -> dict[str, str]:
    """Load active generation templates - CSV metadata + file loading in one stage."""
    # TemplateIOManager handles both CSV filtering and file loading
    return {}  # Placeholder - I/O manager handles actual loading

@asset(group_name="raw_data", io_manager_key="evaluation_template_io_manager")
def evaluation_templates(context) -> dict[str, str]:
    """Load active evaluation templates - CSV metadata + file loading in one stage."""
    # TemplateIOManager handles both CSV filtering and file loading
    return {}  # Placeholder - I/O manager handles actual loading