from dagster import asset
import pandas as pd
from pathlib import Path

@asset(group_name="raw_data")
def concepts_metadata() -> pd.DataFrame:
    """Load concepts metadata from CSV."""
    return pd.read_csv("data/01_raw/concepts/concepts_metadata.csv")

@asset(group_name="raw_data")
def concept_descriptions_sentence() -> dict[str, str]:
    """Load sentence-level concept descriptions."""
    descriptions = {}
    sentence_path = Path("data/01_raw/concepts/descriptions/sentence")
    for file_path in sentence_path.glob("*.txt"):
        concept_id = file_path.stem
        descriptions[concept_id] = file_path.read_text().strip()
    return descriptions

@asset(group_name="raw_data")
def concept_descriptions_paragraph() -> dict[str, str]:
    """Load paragraph-level concept descriptions."""
    descriptions = {}
    paragraph_path = Path("data/01_raw/concepts/descriptions/paragraph")
    for file_path in paragraph_path.glob("*.txt"):
        concept_id = file_path.stem
        descriptions[concept_id] = file_path.read_text().strip()
    return descriptions

@asset(group_name="raw_data")
def concept_descriptions_article() -> dict[str, str]:
    """Load article-level concept descriptions."""
    descriptions = {}
    article_path = Path("data/01_raw/concepts/descriptions/article")
    for file_path in article_path.glob("*.txt"):
        concept_id = file_path.stem
        descriptions[concept_id] = file_path.read_text().strip()
    return descriptions

@asset(group_name="raw_data")
def generation_models() -> pd.DataFrame:
    """Load generation models configuration."""
    return pd.read_csv("data/01_raw/generation_models.csv")

@asset(group_name="raw_data")
def evaluation_models() -> pd.DataFrame:
    """Load evaluation models configuration."""
    return pd.read_csv("data/01_raw/evaluation_models.csv")

@asset(group_name="raw_data")
def generation_templates() -> dict[str, str]:
    """Load generation templates."""
    templates = {}
    templates_path = Path("data/01_raw/generation_templates")
    for file_path in templates_path.glob("*.txt"):
        template_name = file_path.stem
        templates[template_name] = file_path.read_text()
    return templates

@asset(group_name="raw_data")
def evaluation_templates() -> dict[str, str]:
    """Load evaluation templates."""
    templates = {}
    templates_path = Path("data/01_raw/evaluation_templates")
    for file_path in templates_path.glob("*.txt"):
        template_name = file_path.stem
        templates[template_name] = file_path.read_text()
    return templates