"""Helpers for constructing content combinations from catalog data."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import pandas as pd

from ..models import ContentCombination
from ..utils.errors import DDError, Err
from ..utils.raw_readers import read_concepts


def read_combo_mappings_df(data_root: Path | str) -> pd.DataFrame:
    """Read the combo mappings catalog, enforcing presence and non-empty content."""

    base_path = Path(data_root)
    combo_path = base_path / "combo_mappings.csv"
    if not combo_path.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={
                "reason": "combo_mappings_missing",
                "path": str(combo_path),
            },
        )

    combos_df = pd.read_csv(combo_path)
    if combos_df.empty:
        raise DDError(
            Err.DATA_MISSING,
            ctx={
                "reason": "combo_mappings_empty",
                "path": str(combo_path),
            },
        )

    return combos_df


def build_content_combinations(data_root: Path | str) -> List[ContentCombination]:
    """Construct `ContentCombination` objects from combo mappings and concept metadata."""

    base_path = Path(data_root)
    combos_df = read_combo_mappings_df(base_path)

    concepts = read_concepts(base_path)
    concept_index = {str(concept.concept_id): concept for concept in concepts}
    combo_path = base_path / "combo_mappings.csv"

    combinations: list[ContentCombination] = []
    for combo_id, combo_rows in combos_df.groupby("combo_id"):
        normalized_combo_id = str(combo_id).strip()
        if not normalized_combo_id:
            # Skip rows with empty combo identifiers to keep behaviour aligned with the asset.
            continue

        level_value = combo_rows.iloc[0].get("description_level", "paragraph")
        level = str(level_value).strip() or "paragraph"

        concept_ids: Iterable[str] = (
            str(value).strip()
            for value in combo_rows["concept_id"].astype(str).tolist()
        )
        resolved_concepts = []
        missing_concepts: list[str] = []
        for concept_id in concept_ids:
            if not concept_id:
                continue
            concept = concept_index.get(concept_id)
            if concept is None:
                missing_concepts.append(concept_id)
            else:
                resolved_concepts.append(concept)

        if missing_concepts:
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "concepts_missing_for_combo",
                    "combo_id": normalized_combo_id,
                    "missing_concepts": missing_concepts,
                    "path": str(combo_path),
                },
            )

        combinations.append(
            ContentCombination.from_concepts(
                resolved_concepts,
                level=level,
                combo_id=normalized_combo_id,
            )
        )

    combinations.sort(key=lambda combo: combo.combo_id)
    return combinations


__all__ = ["build_content_combinations", "read_combo_mappings_df"]
