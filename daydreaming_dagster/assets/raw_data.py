from dagster import AssetSpec, AssetKey
from pathlib import Path

RAW_BASE = Path("data") / "1_raw"

# CSV-only SourceAssets (no per-description or per-template .txt assets)
CONCEPTS_METADATA_KEY = AssetKey(["raw_source", "concepts_metadata_csv"])
LLM_MODELS_KEY = AssetKey(["raw_source", "llm_models_csv"])
LINK_TEMPLATES_KEY = AssetKey(["raw_source", "link_templates_csv"])
ESSAY_TEMPLATES_KEY = AssetKey(["raw_source", "essay_templates_csv"])
EVALUATION_TEMPLATES_KEY = AssetKey(["raw_source", "evaluation_templates_csv"])

RAW_SOURCE_ASSETS = [
    AssetSpec(
        key=CONCEPTS_METADATA_KEY,
        description=f"External raw CSV: {RAW_BASE / 'concepts_metadata.csv'}",
        group_name="raw_sources",
        metadata={"path": str(RAW_BASE / "concepts_metadata.csv")},
    ),
    AssetSpec(
        key=LLM_MODELS_KEY,
        description=f"External raw CSV: {RAW_BASE / 'llm_models.csv'}",
        group_name="raw_sources",
        metadata={"path": str(RAW_BASE / "llm_models.csv")},
    ),
    AssetSpec(
        key=LINK_TEMPLATES_KEY,
        description=f"External raw CSV: {RAW_BASE / 'link_templates.csv'}",
        group_name="raw_sources",
        metadata={"path": str(RAW_BASE / "link_templates.csv")},
    ),
    AssetSpec(
        key=ESSAY_TEMPLATES_KEY,
        description=f"External raw CSV: {RAW_BASE / 'essay_templates.csv'}",
        group_name="raw_sources",
        metadata={"path": str(RAW_BASE / "essay_templates.csv")},
    ),
    AssetSpec(
        key=EVALUATION_TEMPLATES_KEY,
        description=f"External raw CSV: {RAW_BASE / 'evaluation_templates.csv'}",
        group_name="raw_sources",
        metadata={"path": str(RAW_BASE / "evaluation_templates.csv")},
    ),
]
