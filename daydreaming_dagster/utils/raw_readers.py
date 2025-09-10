from __future__ import annotations

from pathlib import Path
from typing import List
import pandas as pd
from .csv_reading import read_csv_with_context

from ..models import Concept


def read_concepts(data_root: Path, filter_active: bool = True) -> List[Concept]:
    base = Path(data_root) / "1_raw" / "concepts"
    metadata_path = Path(data_root) / "1_raw" / "concepts_metadata.csv"
    df = read_csv_with_context(metadata_path)
    if filter_active and "active" in df.columns:
        df = df[df["active"] == True]

    description_levels = ["sentence", "paragraph", "article"]
    all_descriptions: dict[str, dict[str, str]] = {}
    for level in description_levels:
        d = base / f"descriptions-{level}"
        level_map: dict[str, str] = {}
        if d.exists():
            for file_path in d.glob("*.txt"):
                level_map[file_path.stem] = file_path.read_text().strip()
        all_descriptions[level] = level_map

    concepts: List[Concept] = []
    for _, row in df.iterrows():
        cid = row["concept_id"]
        name = row["name"]
        descriptions: dict[str, str] = {}
        for level in description_levels:
            if cid in all_descriptions[level]:
                descriptions[level] = all_descriptions[level][cid]
        concepts.append(Concept(concept_id=cid, name=name, descriptions=descriptions))
    return concepts


def read_llm_models(data_root: Path) -> pd.DataFrame:
    fp = Path(data_root) / "1_raw" / "llm_models.csv"
    return read_csv_with_context(fp)

def _validate_templates_df(df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    """Validate that template CSVs share a minimal uniform schema.

    Required columns: template_id, active
    Optional column (not enforced here): parser
    """
    required = {"template_id", "active"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{csv_path} must include columns {sorted(required)}; missing {missing}")
    return df


def read_draft_templates(data_root: Path, filter_active: bool = True) -> pd.DataFrame:
    base = Path(data_root) / "1_raw"
    csv_path = base / "draft_templates.csv"
    df = read_csv_with_context(csv_path)
    df = _validate_templates_df(df, csv_path)
    if filter_active and "active" in df.columns:
        df = df[df["active"] == True]
    return df


def read_essay_templates(data_root: Path, filter_active: bool = True) -> pd.DataFrame:
    base = Path(data_root) / "1_raw"
    csv_path = base / "essay_templates.csv"
    df = read_csv_with_context(csv_path)
    df = _validate_templates_df(df, csv_path)
    if filter_active and "active" in df.columns:
        df = df[df["active"] == True]
    return df


def read_evaluation_templates(data_root: Path) -> pd.DataFrame:
    base = Path(data_root) / "1_raw"
    csv_path = base / "evaluation_templates.csv"
    df = read_csv_with_context(csv_path)
    df = _validate_templates_df(df, csv_path)
    return df
