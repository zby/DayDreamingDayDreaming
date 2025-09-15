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


def read_replication_config(data_root: Path) -> dict[str, int] | None:
    """Read per-stage replication factors from data/1_raw/replication_config.csv.

    Expected columns: stage, replicates
    - stage: one of {draft, essay, evaluation}
    - replicates: int >= 1

    Returns a mapping {stage: replicates} or None if the file is missing/empty.
    Invalid rows are ignored; at least one valid row is required to return a dict.
    """
    path = Path(data_root) / "1_raw" / "replication_config.csv"
    if not path.exists():
        return None
    try:
        df = read_csv_with_context(path)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    required = {"stage", "replicates"}
    if not required.issubset(set(df.columns)):
        return None
    out: dict[str, int] = {}
    from ..types import STAGES
    valid_stages = set(STAGES)
    for _, row in df.iterrows():
        stg = str(row.get("stage") or "").strip()
        reps = row.get("replicates")
        try:
            reps_i = int(reps)
        except Exception:
            continue
        if stg in valid_stages and reps_i >= 1:
            out[stg] = reps_i
    return out or None

def _read_templates(data_root: Path, filename: str, *, filter_active: bool = True) -> pd.DataFrame:
    base = Path(data_root) / "1_raw"
    csv_path = base / filename
    df = read_csv_with_context(csv_path)
    df = _validate_templates_df(df, csv_path)
    if filter_active and "active" in df.columns:
        df = df[df["active"] == True]
    return df

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


def read_templates(data_root: Path, kind: str, filter_active: bool = True) -> pd.DataFrame:
    """Unified reader for draft/essay/evaluation templates.

    - kind: one of {"draft", "essay", "evaluation"}
    - filter_active: filter rows where active == True when present
    """
    kind = str(kind).strip().lower()
    if kind not in {"draft", "essay", "evaluation"}:
        raise ValueError(f"Unknown template kind: {kind}")
    filename = f"{kind}_templates.csv"
    return _read_templates(data_root, filename, filter_active=filter_active)
