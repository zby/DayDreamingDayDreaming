"""Cohort scope helper for resolving active generations and metadata."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd

from .errors import DDError, Err


@dataclass(frozen=True)
class GenerationScope:
    stage: str
    gen_id: str
    template_id: Optional[str]
    parent_gen_id: Optional[str]
    combo_id: Optional[str]
    llm_model_id: Optional[str]
    mode: Optional[str]
    origin_cohort_id: Optional[str]
    replicate: Optional[int]


class CohortScope:
    """Resolve cohort membership and metadata directly from gens store."""

    def __init__(self, data_root: Path | str):
        self._data_root = Path(data_root)
        self._cohort_cache: dict[str, pd.DataFrame] = {}
        self._metadata_cache: dict[tuple[str, str], dict] = {}
        self._signature_index: dict[str, dict[Tuple, str]] = {}

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

    def _load_metadata(self, stage: str, gen_id: str) -> dict:
        key = (stage, gen_id)
        if key not in self._metadata_cache:
            path = self._data_root / "gens" / stage / gen_id / "metadata.json"
            if not path.exists():
                raise DDError(Err.DATA_MISSING, ctx={"path": str(path)})
            try:
                self._metadata_cache[key] = json.loads(path.read_text(encoding="utf-8"))
            except OSError as exc:
                raise DDError(Err.IO_ERROR, ctx={"path": str(path)}) from exc
            except json.JSONDecodeError as exc:
                raise DDError(Err.PARSER_FAILURE, ctx={"path": str(path)}) from exc
        return self._metadata_cache[key]

    def get_generation_scope(self, stage: str, gen_id: str) -> GenerationScope:
        meta = self._load_metadata(stage, gen_id)
        return GenerationScope(
            stage=stage,
            gen_id=gen_id,
            template_id=_clean(meta.get("template_id") or meta.get(f"{stage}_template")),
            parent_gen_id=_clean(meta.get("parent_gen_id")),
            combo_id=_clean(meta.get("combo_id")),
            llm_model_id=_clean(meta.get("llm_model_id")),
            mode=_clean(meta.get("mode")),
            origin_cohort_id=_clean(meta.get("origin_cohort_id")),
            replicate=_coerce_int(meta.get("replicate")),
        )

    # --- Deterministic signature helpers ---

    def signature_for_gen(self, stage: str, gen_id: str) -> Tuple:
        from .ids import signature_from_metadata

        meta = self._load_metadata(stage, gen_id)
        return signature_from_metadata(stage, meta)

    def find_existing_gen_id(self, stage: str, signature: Tuple) -> Optional[str]:
        from .ids import signature_from_metadata

        stage_norm = str(stage).lower()
        stage_root = self._data_root / "gens" / stage_norm
        if not stage_root.exists():
            return None
        for child in stage_root.iterdir():
            if not child.is_dir():
                continue
            try:
                meta = self._load_metadata(stage_norm, child.name)
                sig = signature_from_metadata(stage_norm, meta)
            except DDError:
                continue
            if sig == signature:
                return child.name
        return None


def _clean(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_int(value) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


__all__ = ["CohortScope", "GenerationScope"]
