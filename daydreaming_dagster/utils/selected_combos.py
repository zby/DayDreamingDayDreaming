from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def read_selected_combo_mappings(data_root: Path) -> pd.DataFrame:
    """Read selected combos CSV from data/2_tasks/selected_combo_mappings.csv.

    Returns a DataFrame; raises FileNotFoundError with a clear message if missing.
    """
    selected_path = Path(data_root) / "2_tasks" / "selected_combo_mappings.csv"
    if not selected_path.exists():
        raise FileNotFoundError(
            f"Selected combos file not found: {selected_path}. "
            "Create it by filtering rows from data/combo_mappings.csv."
        )
    df = pd.read_csv(selected_path)
    return df


def validate_selected_is_subset(selected_df: pd.DataFrame, superset_path: Path) -> None:
    """Validate that selected_df rows are a strict subset of superset CSV rows.

    - Schema must include columns present in superset (at minimum, we require combo_id and concept_id).
    - Each (combo_id, concept_id, description_level, k_max) row in selected must exist in superset.
    - Duplicate (combo_id, concept_id) pairs in selected are not allowed.
    Raises ValueError on any violation.
    """
    if not superset_path.exists():
        # If there's no superset yet, allow only an empty selection to avoid silent drift
        if selected_df.empty:
            return
        raise FileNotFoundError(
            f"Superset mapping file not found: {superset_path}. "
            "Generate data/combo_mappings.csv before selecting subsets."
        )

    superset = pd.read_csv(superset_path)
    required = {"combo_id", "concept_id", "description_level", "k_max"}
    missing = required - set(selected_df.columns)
    if missing:
        raise ValueError(
            f"Selected combos missing columns: {', '.join(sorted(missing))}"
        )

    # No duplicate pairs in selection
    dupes = selected_df.duplicated(subset=["combo_id", "concept_id"]).sum()
    if dupes:
        raise ValueError(f"Selected combos contain {dupes} duplicate (combo_id, concept_id) rows")

    # Check subset by joining on key columns
    join_cols: Iterable[str] = ["combo_id", "concept_id", "description_level", "k_max"]
    merged = selected_df.merge(superset[join_cols], on=list(join_cols), how="left", indicator=True)
    missing_rows = merged[merged["_merge"] == "left_only"]
    if not missing_rows.empty:
        # Show at most a few problematic rows for clarity
        sample = missing_rows["combo_id"].astype(str).head(5).tolist()
        raise ValueError(
            "Selected combos contain rows not present in superset mapping. "
            f"Examples combo_id: {sample}"
        )

