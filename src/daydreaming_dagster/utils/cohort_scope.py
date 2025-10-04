"""Cohort scope helper for resolving cohort-backed generations and metadata."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

import pandas as pd

from .errors import DDError, Err

class CohortScope:
    """Resolve cohort membership and metadata directly from gens store."""

    def __init__(self, data_root: Path | str):
        self._data_root = Path(data_root)
        self._cohort_cache: dict[str, pd.DataFrame] = {}

    def _membership(self, cohort_id: str) -> pd.DataFrame:
        if cohort_id not in self._cohort_cache:
            csv_path = self._data_root / "cohorts" / cohort_id / "membership.csv"
            try:
                df = pd.read_csv(csv_path)
            except FileNotFoundError as exc:
                raise DDError(Err.DATA_MISSING, ctx={"path": str(csv_path)}) from exc
            except pd.errors.ParserError as exc:
                raise DDError(Err.PARSER_FAILURE, ctx={"path": str(csv_path)}) from exc
            if not {"stage", "gen_id"}.issubset(df.columns):
                raise DDError(Err.INVALID_CONFIG, ctx={"path": str(csv_path), "missing": ["stage", "gen_id"]})
            self._cohort_cache[cohort_id] = df
        return self._cohort_cache[cohort_id]

    def stage_gen_ids(self, cohort_id: str, stage: str) -> list[str]:
        df = self._membership(str(cohort_id))
        return (
            df[df["stage"].astype(str).str.lower() == stage.lower()]["gen_id"].astype(str).tolist()
        )

    def iter_stage_gen_ids(self, cohort_id: str, stages: Iterable[str]) -> dict[str, list[str]]:
        return {stage: self.stage_gen_ids(cohort_id, stage) for stage in stages}

__all__ = ["CohortScope"]
