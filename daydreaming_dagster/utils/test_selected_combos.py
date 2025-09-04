import pandas as pd
from pathlib import Path
import tempfile
import pytest

from daydreaming_dagster.utils.selected_combos import validate_selected_is_subset


def _write_csv(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def test_validate_selected_subset_success():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        superset = pd.DataFrame([
            {"combo_id": "c1", "concept_id": "a", "description_level": "sentence", "k_max": 2, "version": "v1", "created_at": "2024-01-01"},
            {"combo_id": "c1", "concept_id": "b", "description_level": "sentence", "k_max": 2, "version": "v1", "created_at": "2024-01-01"},
            {"combo_id": "c2", "concept_id": "a", "description_level": "sentence", "k_max": 2, "version": "v1", "created_at": "2024-01-01"},
        ])
        sel = pd.DataFrame([
            {"combo_id": "c1", "concept_id": "a", "description_level": "sentence", "k_max": 2},
            {"combo_id": "c1", "concept_id": "b", "description_level": "sentence", "k_max": 2},
        ])
        superset_path = root / "data" / "combo_mappings.csv"
        _write_csv(superset_path, superset)
        # Should not raise
        validate_selected_is_subset(sel, superset_path)


def test_validate_selected_subset_missing_superset_raises_when_nonempty_selection():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        sel = pd.DataFrame([
            {"combo_id": "c1", "concept_id": "a", "description_level": "sentence", "k_max": 2},
        ])
        with pytest.raises(FileNotFoundError):
            validate_selected_is_subset(sel, root / "data" / "combo_mappings.csv")


def test_validate_selected_subset_detects_non_subset():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        superset = pd.DataFrame([
            {"combo_id": "c1", "concept_id": "a", "description_level": "sentence", "k_max": 2},
        ])
        sel = pd.DataFrame([
            {"combo_id": "c9", "concept_id": "z", "description_level": "sentence", "k_max": 2},
        ])
        superset_path = root / "data" / "combo_mappings.csv"
        _write_csv(superset_path, superset)
        with pytest.raises(ValueError):
            validate_selected_is_subset(sel, superset_path)


def test_validate_selected_subset_duplicate_pairs_rejected():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        superset = pd.DataFrame([
            {"combo_id": "c1", "concept_id": "a", "description_level": "sentence", "k_max": 2},
        ])
        sel = pd.DataFrame([
            {"combo_id": "c1", "concept_id": "a", "description_level": "sentence", "k_max": 2},
            {"combo_id": "c1", "concept_id": "a", "description_level": "sentence", "k_max": 2},
        ])
        superset_path = root / "data" / "combo_mappings.csv"
        _write_csv(superset_path, superset)
        with pytest.raises(ValueError):
            validate_selected_is_subset(sel, superset_path)
