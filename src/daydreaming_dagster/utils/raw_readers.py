from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Sequence
import pandas as pd
from .csv_reading import read_csv_with_context
from .errors import DDError, Err

from ..models import Concept


LEGACY_COLUMN = "active"


def _normalize_allowlist(values: Iterable[str] | None) -> Sequence[str]:
    if values is None:
        return []
    normalized = []
    for value in values:
        text = str(value).strip()
        if text:
            normalized.append(text)
    if not normalized:
        return []
    seen = set()
    deduped: list[str] = []
    for item in normalized:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _reject_legacy_column(df, *, csv_path: Path, column: str) -> None:
    if column in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "path": str(csv_path),
                "column": column,
                "reason": "legacy_column_rejected",
            },
        )


def _validate_allowlist(df, *, csv_path: Path, column: str, allowlist: Sequence[str]) -> None:
    if not allowlist:
        return
    unknown = sorted(
        value for value in allowlist if value not in set(df[column].astype(str))
    )
    if unknown:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "path": str(csv_path),
                "column": column,
                "unknown_values": unknown,
                "reason": "allowlist_entries_missing",
            },
        )


def read_concepts(
    data_root: Path,
    *,
    allowlist: Iterable[str] | None = None,
) -> List[Concept]:
    base = Path(data_root) / "1_raw" / "concepts"
    metadata_path = Path(data_root) / "1_raw" / "concepts_metadata.csv"
    df = read_csv_with_context(metadata_path)
    _reject_legacy_column(df, csv_path=metadata_path, column=LEGACY_COLUMN)

    normalized_allowlist = _normalize_allowlist(allowlist)
    if normalized_allowlist:
        _validate_allowlist(df, csv_path=metadata_path, column="concept_id", allowlist=normalized_allowlist)
        df = df[df["concept_id"].astype(str).isin(normalized_allowlist)]

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
    df = read_csv_with_context(path)
    if df is None or df.empty:
        return None
    required = {"stage", "replicates"}
    if not required.issubset(set(df.columns)):
        return None
    out: dict[str, int] = {}
    from ..types import STAGES
    valid_stages = set(STAGES)
    df = df[list(required)].copy()
    df["stage"] = df["stage"].astype(str).str.strip()
    df["replicates"] = pd.to_numeric(df["replicates"], errors="coerce")
    df = df.dropna(subset=["replicates"])
    df["replicates"] = df["replicates"].astype(int)
    df = df[df["replicates"] >= 1]
    df = df[df["stage"].isin(valid_stages)]
    for _, row in df.iterrows():
        out[row["stage"]] = int(row["replicates"])
    return out or None

def _read_templates(
    data_root: Path,
    filename: str,
    *,
    allowlist: Iterable[str] | None = None,
) -> pd.DataFrame:
    base = Path(data_root) / "1_raw"
    csv_path = base / filename
    df = read_csv_with_context(csv_path)
    df = _validate_templates_df(df, csv_path)

    normalized_allowlist = _normalize_allowlist(allowlist)
    if normalized_allowlist:
        _validate_allowlist(df, csv_path=csv_path, column="template_id", allowlist=normalized_allowlist)
        df = df[df["template_id"].astype(str).isin(normalized_allowlist)]
    return df

def _validate_templates_df(df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    """Validate that template CSVs share a minimal uniform schema."""

    if "template_id" not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "path": str(csv_path),
                "missing": ["template_id"],
                "reason": "required_column_missing",
            },
        )
    _reject_legacy_column(df, csv_path=csv_path, column=LEGACY_COLUMN)
    return df


def read_templates(
    data_root: Path,
    kind: str,
    *,
    allowlist: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Unified reader for draft/essay/evaluation templates.

    - kind: one of {"draft", "essay", "evaluation"}
    - allowlist: optional iterable of template_ids to keep
    """
    kind = str(kind).strip().lower()
    if kind not in {"draft", "essay", "evaluation"}:
        raise DDError(Err.INVALID_CONFIG, ctx={"kind": kind})
    filename = f"{kind}_templates.csv"
    return _read_templates(data_root, filename, allowlist=allowlist)
