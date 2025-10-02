"""
Group: cohort

Assets for building cohort membership (authoritative, wide rows) and
registering dynamic partitions based on cohort membership.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from dagster import MetadataValue
from ._decorators import asset_with_boundary

from ..utils.ids import (
    draft_signature,
    essay_signature,
    evaluation_signature,
    compute_deterministic_gen_id,
)
from ..utils.raw_readers import (
    read_concepts,
    read_templates,
    read_llm_models,
    read_replication_config,
)
from ..cohorts import CohortPlan, load_cohort_plan
from ..models import ContentCombination
from daydreaming_dagster.models.content_combination import generate_combo_id
from ..data_layer.gens_data_layer import GensDataLayer
from ..utils.cohorts import (
    get_env_cohort_id,
    compute_cohort_id,
    write_manifest,
)
from .partitions import (
    draft_gens_partitions,
    essay_gens_partitions,
    evaluation_gens_partitions,
    cohort_reports_partitions,
)
from ..utils.errors import DDError, Err
from ..data_layer.paths import Paths


@dataclass
class CuratedSelectionConfig:
    selection_type: Optional[str]  # 'essay', 'draft', or None
    ids: List[str]


@dataclass
class CuratedEssay:
    essay_gen_id: str
    draft_gen_id: str
    combo_id: str
    draft_template_id: str
    essay_template_id: str
    draft_llm_model_id: str
    essay_llm_model_id: str
    draft_replicate: int
    essay_replicate: int


@dataclass
class CuratedDraft:
    draft_gen_id: str
    combo_id: str
    draft_template_id: str
    draft_llm_model_id: str
    draft_replicate: int


@dataclass
class ExistingEvaluation:
    gen_id: str
    replicate: int


@dataclass(frozen=True)
class MembershipRow:
    """Normalized cohort membership row with consistent defaults."""

    stage: str
    gen_id: str
    origin_cohort_id: str
    parent_gen_id: str = ""
    combo_id: str = ""
    template_id: str = ""
    llm_model_id: str = ""
    replicate: int = 1

    def to_dict(self) -> Dict[str, object]:
        return {
            "stage": self.stage,
            "gen_id": self.gen_id,
            "origin_cohort_id": self.origin_cohort_id,
            "parent_gen_id": self.parent_gen_id,
            "combo_id": self.combo_id,
            "template_id": self.template_id,
            "llm_model_id": self.llm_model_id,
            "replicate": int(self.replicate),
        }


MEMBERSHIP_COLUMNS = [
    "stage",
    "gen_id",
    "origin_cohort_id",
    "parent_gen_id",
    "combo_id",
    "template_id",
    "llm_model_id",
    "replicate",
]


def _build_spec_catalogs(
    data_root: Path,
    selected_combo_mappings: pd.DataFrame,
) -> dict[str, list[str]]:
    catalogs: dict[str, list[str]] = {}

    def _template_ids(kind: str) -> list[str]:
        df = read_templates(data_root, kind, filter_active=False)
        if df.empty:
            return []
        values = {
            str(value).strip()
            for value in df["template_id"].dropna().tolist()
            if str(value).strip()
        }
        return sorted(values)

    drafts = _template_ids("draft")
    if drafts:
        catalogs["draft_templates"] = drafts

    essays = _template_ids("essay")
    if essays:
        catalogs["essay_templates"] = essays

    evaluations = _template_ids("evaluation")
    if evaluations:
        catalogs["evaluation_templates"] = evaluations

    llm_df = read_llm_models(data_root)
    if not llm_df.empty:
        generation_llms = {
            str(value).strip()
            for value in llm_df[llm_df["for_generation"] == True]["id"].dropna().tolist()
            if str(value).strip()
        }
        evaluation_llms = {
            str(value).strip()
            for value in llm_df[llm_df["for_evaluation"] == True]["id"].dropna().tolist()
            if str(value).strip()
        }
        if generation_llms:
            sorted_generation = sorted(generation_llms)
            catalogs["generation_llms"] = sorted_generation
            catalogs.setdefault("draft_llms", sorted_generation)
            catalogs.setdefault("essay_llms", sorted_generation)
        if evaluation_llms:
            catalogs["evaluation_llms"] = sorted(evaluation_llms)

    combos: set[str] = set()
    if isinstance(selected_combo_mappings, pd.DataFrame) and not selected_combo_mappings.empty:
        combos.update(
            str(value).strip()
            for value in selected_combo_mappings.get("combo_id", pd.Series(dtype=str)).dropna().tolist()
            if str(value).strip()
        )

    combo_path = data_root / "combo_mappings.csv"
    if combo_path.exists():
        try:
            combo_df = pd.read_csv(combo_path, usecols=["combo_id"])
        except Exception:  # pragma: no cover - best-effort catalog hydration
            combo_df = pd.DataFrame()
        if not combo_df.empty:
            combos.update(
                str(value).strip()
                for value in combo_df["combo_id"].dropna().tolist()
                if str(value).strip()
            )

    if combos:
        catalogs["combos"] = sorted(combos)

    return catalogs


class CohortBuilder:
    """Build cohort membership rows using data-layer helpers for deterministic IDs."""

    def __init__(
        self,
        *,
        cohort_id: str,
        data_layer: GensDataLayer,
        replication_config: Dict[str, int] | None = None,
    ) -> None:
        self._cohort_id = str(cohort_id)
        self._data_layer = data_layer
        self._replication = replication_config or {}
        self._allocator = _ReplicateAllocator(self._data_layer.paths.gens_root)
        self._essay_seed_combo: Dict[str, str] = {}
        self._existing_eval_cache: Dict[
            str, Dict[tuple[str, str], List[ExistingEvaluation]]
        ] = {}

    @property
    def cohort_id(self) -> str:
        return self._cohort_id

    @property
    def data_root(self) -> Path:
        return self._data_layer.data_root

    def _rep_count(self, stage: str) -> int:
        raw = self._replication.get(stage, 1)
        try:
            return int(raw)
        except Exception:
            return 1

    def _draft_row(
        self,
        *,
        gen_id: str,
        combo_id: str,
        template_id: str,
        llm_model_id: str,
        replicate: int | str,
    ) -> MembershipRow:
        replicate_int = _normalize_int(replicate, default=1)
        return MembershipRow(
            stage="draft",
            gen_id=gen_id,
            origin_cohort_id=self._cohort_id,
            combo_id=combo_id,
            template_id=template_id,
            llm_model_id=llm_model_id,
            replicate=replicate_int,
        )

    def _essay_row(
        self,
        *,
        gen_id: str,
        parent_gen_id: str,
        combo_id: str,
        template_id: str,
        llm_model_id: str,
        replicate: int | str,
    ) -> MembershipRow:
        replicate_int = _normalize_int(replicate, default=1)
        self._essay_seed_combo[str(gen_id)] = combo_id
        return MembershipRow(
            stage="essay",
            gen_id=gen_id,
            origin_cohort_id=self._cohort_id,
            parent_gen_id=parent_gen_id,
            combo_id=combo_id,
            template_id=template_id,
            llm_model_id=llm_model_id,
            replicate=replicate_int,
        )

    def _evaluation_row(
        self,
        *,
        gen_id: str,
        parent_gen_id: str,
        combo_id: str,
        template_id: str,
        llm_model_id: str,
        replicate: int | str,
    ) -> MembershipRow:
        replicate_int = _normalize_int(replicate, default=1)
        return MembershipRow(
            stage="evaluation",
            gen_id=gen_id,
            origin_cohort_id=self._cohort_id,
            parent_gen_id=parent_gen_id,
            combo_id=combo_id,
            template_id=template_id,
            llm_model_id=llm_model_id,
            replicate=replicate_int,
        )

    def _ensure_existing_eval_cache(
        self, essay_gen_id: str
    ) -> Dict[tuple[str, str], List[ExistingEvaluation]]:
        cache = self._existing_eval_cache.get(essay_gen_id)
        if cache is None:
            cache = _existing_evaluations_by_template_model(self.data_root, essay_gen_id)
            self._existing_eval_cache[essay_gen_id] = cache
        return cache

    def build_from_essays(self, essay_ids: Sequence[str]) -> List[MembershipRow]:
        if not essay_ids:
            return []

        entries = _prepare_curated_entries(self.data_root, essay_ids)
        rows: List[MembershipRow] = []
        for entry in entries:
            draft_llm_for_row = entry.draft_llm_model_id or entry.essay_llm_model_id
            rows.append(
                self._draft_row(
                    gen_id=entry.draft_gen_id,
                    combo_id=entry.combo_id,
                    template_id=entry.draft_template_id,
                    llm_model_id=draft_llm_for_row,
                    replicate=entry.draft_replicate,
                )
            )

            essay_llm_model = entry.essay_llm_model_id or draft_llm_for_row
            rows.append(
                self._essay_row(
                    gen_id=entry.essay_gen_id,
                    parent_gen_id=entry.draft_gen_id,
                    combo_id=entry.combo_id,
                    template_id=entry.essay_template_id,
                    llm_model_id=essay_llm_model,
                    replicate=entry.essay_replicate,
                )
            )

            self._existing_eval_cache[entry.essay_gen_id] = _existing_evaluations_by_template_model(
                self.data_root, entry.essay_gen_id
            )

        return rows

    def build_from_drafts(
        self,
        draft_ids: Sequence[str],
        *,
        essay_template_ids: Sequence[str],
    ) -> List[MembershipRow]:
        if not draft_ids:
            return []

        entries = _prepare_curated_drafts(self.data_root, draft_ids)
        rows: List[MembershipRow] = []
        essay_rep_count = self._rep_count("essay")

        for entry in entries:
            rows.append(
                self._draft_row(
                    gen_id=entry.draft_gen_id,
                    combo_id=entry.combo_id,
                    template_id=entry.draft_template_id,
                    llm_model_id=entry.draft_llm_model_id,
                    replicate=entry.draft_replicate,
                )
            )

            for essay_tpl in essay_template_ids:
                base_signature = (entry.draft_gen_id, essay_tpl)
                replicate_indices = self._allocator.allocate(
                    "essay", base_signature, essay_rep_count
                )
                for replicate_index in replicate_indices:
                    essay_gen_id = self._data_layer.reserve_essay_id(
                        draft_gen_id=entry.draft_gen_id,
                        template_id=essay_tpl,
                        cohort_id=self._cohort_id,
                        replicate=int(replicate_index),
                    )
                    rows.append(
                        self._essay_row(
                            gen_id=essay_gen_id,
                            parent_gen_id=entry.draft_gen_id,
                            combo_id=entry.combo_id,
                            template_id=essay_tpl,
                            llm_model_id=entry.draft_llm_model_id,
                            replicate=int(replicate_index),
                        )
                    )

        return rows

    def build_cartesian(
        self,
        *,
        combo_ids: Sequence[str],
        draft_template_ids: Sequence[str],
        essay_template_ids: Sequence[str],
        generation_model_ids: Sequence[str],
    ) -> List[MembershipRow]:
        rows: List[MembershipRow] = []
        draft_rep_count = self._rep_count("draft")
        essay_rep_count = self._rep_count("essay")

        draft_context: List[Dict[str, object]] = []

        for combo_id in combo_ids:
            for draft_tpl in draft_template_ids:
                for model_id in generation_model_ids:
                    for replicate_index in range(1, draft_rep_count + 1):
                        draft_gen_id = self._data_layer.reserve_draft_id(
                            combo_id=combo_id,
                            template_id=draft_tpl,
                            llm_model_id=model_id,
                            cohort_id=self._cohort_id,
                            replicate=replicate_index,
                        )
                        rows.append(
                            self._draft_row(
                                gen_id=draft_gen_id,
                                combo_id=combo_id,
                                template_id=draft_tpl,
                                llm_model_id=model_id,
                                replicate=replicate_index,
                            )
                        )
                        draft_context.append(
                            {
                                "gen_id": draft_gen_id,
                                "combo_id": combo_id,
                                "template_id": draft_tpl,
                                "llm_model_id": model_id,
                                "replicate": replicate_index,
                            }
                        )

        if not draft_context or not essay_template_ids:
            return rows

        for draft in draft_context:
            draft_gen_id = str(draft.get("gen_id"))
            combo_id = str(draft.get("combo_id"))
            draft_template_id = str(draft.get("template_id"))
            llm_model_id = str(draft.get("llm_model_id"))
            for essay_tpl in essay_template_ids:
                base_signature = (draft_gen_id, essay_tpl)
                replicate_indices = self._allocator.allocate(
                    "essay", base_signature, essay_rep_count
                )
                for replicate_index in replicate_indices:
                    essay_gen_id = self._data_layer.reserve_essay_id(
                        draft_gen_id=draft_gen_id,
                        template_id=essay_tpl,
                        cohort_id=self._cohort_id,
                        replicate=int(replicate_index),
                    )
                    rows.append(
                        self._essay_row(
                            gen_id=essay_gen_id,
                            parent_gen_id=draft_gen_id,
                            combo_id=combo_id,
                            template_id=essay_tpl,
                            llm_model_id=llm_model_id,
                            replicate=int(replicate_index),
                        )
                    )

        return rows

    def build_from_spec_plan(self, plan: CohortPlan) -> List[MembershipRow]:
        if not plan:
            return []

        rows: List[MembershipRow] = []
        draft_ids: Dict[tuple[str, str, str, int], str] = {}

        for draft_entry in plan.drafts:
            draft_key = draft_entry.key()
            gen_id = self._data_layer.reserve_draft_id(
                combo_id=draft_entry.combo_id,
                template_id=draft_entry.template_id,
                llm_model_id=draft_entry.llm_model_id,
                cohort_id=self._cohort_id,
                replicate=draft_entry.replicate,
            )
            draft_ids[draft_key] = gen_id
            rows.append(
                self._draft_row(
                    gen_id=gen_id,
                    combo_id=draft_entry.combo_id,
                    template_id=draft_entry.template_id,
                    llm_model_id=draft_entry.llm_model_id,
                    replicate=draft_entry.replicate,
                )
            )

        essay_ids: Dict[tuple[tuple[str, str, str, int], str, str, int], str] = {}

        for essay_entry in plan.essays:
            draft_key = essay_entry.draft.key()
            if draft_key not in draft_ids:
                raise DDError(
                    Err.INVALID_CONFIG,
                    ctx={
                        "reason": "missing_draft_for_essay",
                        "draft": draft_key,
                        "essay_template": essay_entry.template_id,
                    },
                )

            draft_gen_id = draft_ids[draft_key]
            essay_gen_id = self._data_layer.reserve_essay_id(
                draft_gen_id=draft_gen_id,
                template_id=essay_entry.template_id,
                cohort_id=self._cohort_id,
                replicate=essay_entry.replicate,
            )
            essay_ids[essay_entry.key()] = essay_gen_id
            rows.append(
                self._essay_row(
                    gen_id=essay_gen_id,
                    parent_gen_id=draft_gen_id,
                    combo_id=essay_entry.draft.combo_id,
                    template_id=essay_entry.template_id,
                    llm_model_id=essay_entry.llm_model_id,
                    replicate=essay_entry.replicate,
                )
            )

        for evaluation_entry in plan.evaluations:
            essay_key = evaluation_entry.essay.key()
            if essay_key not in essay_ids:
                raise DDError(
                    Err.INVALID_CONFIG,
                    ctx={
                        "reason": "missing_essay_for_evaluation",
                        "evaluation_template": evaluation_entry.template_id,
                    },
                )

            essay_gen_id = essay_ids[essay_key]
            evaluation_gen_id = self._data_layer.reserve_evaluation_id(
                essay_gen_id=essay_gen_id,
                template_id=evaluation_entry.template_id,
                llm_model_id=evaluation_entry.llm_model_id,
                cohort_id=self._cohort_id,
                replicate=evaluation_entry.replicate,
            )
            rows.append(
                self._evaluation_row(
                    gen_id=evaluation_gen_id,
                    parent_gen_id=essay_gen_id,
                    combo_id=evaluation_entry.essay.draft.combo_id,
                    template_id=evaluation_entry.template_id,
                    llm_model_id=evaluation_entry.llm_model_id,
                    replicate=evaluation_entry.replicate,
                )
            )

        return rows

    def build_evaluations(
        self,
        *,
        evaluation_templates: Sequence[str],
        evaluation_models: Sequence[str],
    ) -> tuple[List[MembershipRow], Dict[str, int]]:
        rows: List[MembershipRow] = []
        total_created = 0
        fully_covered = 0
        evaluation_rep_count = self._rep_count("evaluation")

        eval_templates = [str(t).strip() for t in evaluation_templates if str(t).strip()]
        eval_models = [str(m).strip() for m in evaluation_models if str(m).strip()]
        if not eval_templates or not eval_models:
            return rows, {"created": 0, "fully_covered": 0}

        for essay_gen_id, combo_id in self._essay_seed_combo.items():
            essay_created = 0
            existing_counts = self._ensure_existing_eval_cache(essay_gen_id)

            for tpl in eval_templates:
                for model_id in eval_models:
                    existing_entries = sorted(
                        existing_counts.get((tpl, model_id), []),
                        key=lambda item: item.replicate,
                    )
                    reuse_entries = existing_entries[:evaluation_rep_count]
                    for reuse in reuse_entries:
                        rows.append(
                            self._evaluation_row(
                                gen_id=reuse.gen_id,
                                parent_gen_id=essay_gen_id,
                                combo_id=combo_id,
                                template_id=tpl,
                                llm_model_id=model_id,
                                replicate=reuse.replicate,
                            )
                        )

                    needed = max(0, evaluation_rep_count - len(reuse_entries))
                    if needed <= 0:
                        continue

                    replicate_indices = self._allocator.allocate(
                        "evaluation", (essay_gen_id, tpl, model_id), needed
                    )
                    cache_entries = existing_counts.setdefault((tpl, model_id), [])
                    for replicate_index in replicate_indices:
                        eval_gen_id = self._data_layer.reserve_evaluation_id(
                            essay_gen_id=essay_gen_id,
                            template_id=tpl,
                            llm_model_id=model_id,
                            cohort_id=self._cohort_id,
                            replicate=int(replicate_index),
                        )
                        rows.append(
                            self._evaluation_row(
                                gen_id=eval_gen_id,
                                parent_gen_id=essay_gen_id,
                                combo_id=combo_id,
                                template_id=tpl,
                                llm_model_id=model_id,
                                replicate=int(replicate_index),
                            )
                        )
                        cache_entries.append(
                            ExistingEvaluation(gen_id=eval_gen_id, replicate=int(replicate_index))
                        )
                        essay_created += 1
                        total_created += 1

            if essay_created == 0:
                fully_covered += 1

        return rows, {"created": total_created, "fully_covered": fully_covered}
def _require_replication_config(data_root: Path) -> dict[str, int]:
    rep_cfg = read_replication_config(data_root)
    if not isinstance(rep_cfg, dict):
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "replication_config_missing"},
        )
    for stage in ("draft", "essay", "evaluation"):
        value = rep_cfg.get(stage)
        if not isinstance(value, int) or value < 1:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "invalid_replication_config",
                    "stage": stage,
                    "value": value,
                },
            )
    return rep_cfg


def _parse_selection_file(path: Path) -> List[str]:
    ids: List[str] = []

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        ids.append(line)

    return ids


def _load_curated_selection_config(data_root: Path) -> CuratedSelectionConfig:
    essays_path = data_root / "2_tasks" / "selected_essays.txt"
    drafts_path = data_root / "2_tasks" / "selected_drafts.txt"

    essays_exists = essays_path.exists()
    drafts_exists = drafts_path.exists()
    if essays_exists and drafts_exists:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "multiple_curated_inputs"},
        )

    if essays_exists:
        ids = _parse_selection_file(essays_path)
        return CuratedSelectionConfig(
            selection_type="essay",
            ids=ids,
        )

    if drafts_exists:
        ids = _parse_selection_file(drafts_path)
        return CuratedSelectionConfig(
            selection_type="draft",
            ids=ids,
        )

    return CuratedSelectionConfig(
        selection_type=None,
        ids=[],
    )


class _ReplicateAllocator:
    """Allocate deterministic replicate indices without reusing existing ids."""

    def __init__(self, gens_root: Path):
        self._gens_root = gens_root
        self._next_indices: Dict[tuple[str, tuple], int] = {}

    def allocate(self, stage: str, base_signature: tuple, count: int) -> List[int]:
        if count <= 0:
            return []
        stage_norm = str(stage).lower()
        key = (stage_norm, base_signature)
        next_rep = self._next_indices.get(key)
        if next_rep is None:
            next_rep = self._discover_next(stage_norm, base_signature)
        allocations = [next_rep + offset for offset in range(count)]
        self._next_indices[key] = allocations[-1] + 1
        return allocations

    def _discover_next(self, stage: str, base_signature: tuple) -> int:
        probe = 1
        while True:
            gen_id = _deterministic_id_for_base(stage, base_signature, probe)
            if not (self._gens_root / stage / gen_id).exists():
                return probe
            probe += 1


def _deterministic_id_for_base(stage: str, base_signature: tuple, replicate_index: int) -> str:
    stage_norm = str(stage).lower()
    if stage_norm == "draft":
        combo_id, draft_template_id, llm_model_id = base_signature
        signature = draft_signature(combo_id, draft_template_id, llm_model_id, replicate_index)
    elif stage_norm == "essay":
        draft_gen_id, essay_template_id = base_signature
        signature = essay_signature(draft_gen_id, essay_template_id, replicate_index)
    elif stage_norm == "evaluation":
        essay_gen_id, evaluation_template_id, evaluation_model_id = base_signature
        signature = evaluation_signature(essay_gen_id, evaluation_template_id, evaluation_model_id, replicate_index)
    else:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "unsupported_replicate_stage", "stage": stage},
        )
    return compute_deterministic_gen_id(stage_norm, signature)


def _existing_evaluations_by_template_model(
    data_root: Path, essay_id: str
) -> Dict[tuple[str, str], list[ExistingEvaluation]]:
    """Return existing evaluation gen_ids keyed by (template_id, llm_model_id)."""

    existing: Dict[tuple[str, str], list[ExistingEvaluation]] = defaultdict(list)
    eval_root = data_root / "gens" / "evaluation"
    if not eval_root.exists():
        return existing
    for gen_dir in eval_root.iterdir():
        if not gen_dir.is_dir():
            continue
        meta_path = gen_dir / "metadata.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        if str(meta.get("parent_gen_id") or "").strip() != essay_id:
            continue
        tpl = str(meta.get("template_id") or meta.get("evaluation_template") or "").strip()
        model = str(meta.get("llm_model_id") or "").strip()
        if not tpl or not model:
            continue
        replicate = _normalize_int(meta.get("replicate"), default=1)
        existing[(tpl, model)].append(ExistingEvaluation(gen_dir.name, replicate))
    return existing


def _prepare_curated_entries(data_root: Path, essay_ids: Iterable[str]) -> List[CuratedEssay]:
    entries: List[CuratedEssay] = []
    data_layer = GensDataLayer.from_root(data_root)
    paths = data_layer.paths
    for essay_src_id in essay_ids:
        essay_meta_path = paths.metadata_path("essay", essay_src_id)
        if not essay_meta_path.exists():
            draft_meta_path = paths.metadata_path("draft", essay_src_id)
            if draft_meta_path.exists():
                raise DDError(
                    Err.INVALID_CONFIG,
                    ctx={
                        "reason": "curated_essay_is_draft",
                        "gen_id": essay_src_id,
                    },
                )
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "essay_metadata_missing",
                    "gen_id": essay_src_id,
                },
            )

        essay_meta = data_layer.read_main_metadata("essay", essay_src_id)
        essay_tpl = str(essay_meta.get("template_id") or essay_meta.get("essay_template") or "").strip()
        essay_llm_model = str(essay_meta.get("llm_model_id") or "").strip()
        draft_parent_src = str(essay_meta.get("parent_gen_id") or "").strip()
        if not essay_tpl:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "essay_missing_template",
                    "gen_id": essay_src_id,
                },
            )
        if not draft_parent_src:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "essay_missing_parent",
                    "gen_id": essay_src_id,
                },
            )

        draft_meta_path = paths.metadata_path("draft", draft_parent_src)
        if not draft_meta_path.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "draft_parent_metadata_missing",
                    "draft_gen_id": draft_parent_src,
                    "essay_gen_id": essay_src_id,
                },
            )
        draft_meta = data_layer.read_main_metadata("draft", draft_parent_src)
        combo_id = str(draft_meta.get("combo_id") or "").strip()
        draft_tpl = str(draft_meta.get("template_id") or draft_meta.get("draft_template") or "").strip()
        draft_llm_model = str(draft_meta.get("llm_model_id") or "").strip()

        llm_model_id = essay_llm_model or draft_llm_model
        missing_fields: List[str] = []
        if not combo_id:
            missing_fields.append("draft.combo_id")
        if not draft_tpl:
            missing_fields.append("draft.template_id")
        if not llm_model_id:
            missing_fields.append("llm_model_id")
        if missing_fields:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "missing_metadata_for_tasks",
                    "missing": missing_fields,
                    "essay_gen_id": essay_src_id,
                },
            )

        entries.append(
            CuratedEssay(
                essay_gen_id=essay_src_id,
                draft_gen_id=draft_parent_src,
                combo_id=combo_id,
                draft_template_id=draft_tpl,
                essay_template_id=essay_tpl,
                draft_llm_model_id=draft_llm_model,
                essay_llm_model_id=essay_llm_model,
                draft_replicate=_normalize_int(draft_meta.get("replicate"), default=1),
                essay_replicate=_normalize_int(essay_meta.get("replicate"), default=1),
            )
        )
    return entries


def _prepare_curated_drafts(data_root: Path, draft_ids: Iterable[str]) -> List[CuratedDraft]:
    entries: List[CuratedDraft] = []
    data_layer = GensDataLayer.from_root(data_root)
    paths = data_layer.paths
    for draft_id in draft_ids:
        meta_path = paths.metadata_path("draft", draft_id)
        if not meta_path.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "draft_metadata_missing",
                    "draft_gen_id": draft_id,
                },
            )
        draft_meta = data_layer.read_main_metadata("draft", draft_id)
        combo_id = str(draft_meta.get("combo_id") or "").strip()
        draft_tpl = str(draft_meta.get("template_id") or draft_meta.get("draft_template") or "").strip()
        llm_model_id = str(draft_meta.get("llm_model_id") or "").strip()
        replicate = _normalize_int(draft_meta.get("replicate"), default=1)

        missing_fields: List[str] = []
        if not combo_id:
            missing_fields.append("draft.combo_id")
        if not draft_tpl:
            missing_fields.append("draft.template_id")
        if not llm_model_id:
            missing_fields.append("llm_model_id")
        if missing_fields:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "missing_draft_metadata",
                    "draft_gen_id": draft_id,
                    "missing": missing_fields,
                },
            )

        entries.append(
            CuratedDraft(
                draft_gen_id=draft_id,
                combo_id=combo_id,
                draft_template_id=draft_tpl,
                draft_llm_model_id=llm_model_id,
                draft_replicate=replicate,
            )
        )
    return entries


# Builders extracted from cohort_membership for curated selections.




# Model provider name mapping removed from cohort generation to reduce complexity.


def _eval_axes(data_root: Path) -> Tuple[List[str], List[str]]:
    """Return active evaluation template IDs and evaluation model IDs."""
    models_df = read_llm_models(data_root)
    evaluation_models = models_df[models_df["for_evaluation"] == True]
    eval_model_ids = (
        evaluation_models["id"].astype(str).tolist() if not evaluation_models.empty else []
    )
    eval_templates_df = read_templates(data_root, "evaluation", filter_active=True)
    eval_tpl_ids = (
        eval_templates_df["template_id"].astype(str).tolist()
        if not eval_templates_df.empty
        else []
    )
    return eval_tpl_ids, eval_model_ids


def _read_templates_safe(data_root: Path, kind: str) -> pd.DataFrame:
    try:
        return read_templates(data_root, kind, filter_active=False)
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _template_mode_map(df: pd.DataFrame | None, default: str = "llm") -> Dict[str, str]:
    """Build a mapping of template_id -> generator mode."""
    if df is None or df.empty:
        return {}
    mode_map: Dict[str, str] = {}
    for _, row in df.iterrows():
        template_id = str(row.get("template_id") or "").strip()
        if not template_id:
            continue
        raw_mode = row.get("generator") if "generator" in row.index else None
        mode = str(raw_mode or default).strip().lower() or default
        mode_map[template_id] = mode
    return mode_map


def _normalize_str(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


def _normalize_int(value, default: int = 1) -> int:
    try:
        if value is None or pd.isna(value):  # type: ignore[arg-type]
            return default
    except Exception:
        if value is None:
            return default
    try:
        return int(value)
    except Exception:
        return default


def seed_cohort_metadata(
    *,
    data_layer: GensDataLayer,
    cohort_id: str,
    membership: pd.DataFrame,
    template_modes: Dict[str, Dict[str, str]],
) -> None:
    """Ensure metadata.json exists for each cohort generation prior to running stage assets."""

    if membership is None or membership.empty:
        return

    paths = data_layer.paths

    for _, row in membership.iterrows():
        stage = _normalize_str(row.get("stage"))
        gen_id = _normalize_str(row.get("gen_id"))
        if stage not in {"draft", "essay", "evaluation"} or not gen_id:
            continue

        meta_path = data_layer.paths.metadata_path(stage, gen_id)
        if meta_path.exists():
            continue

        template_id = _normalize_str(row.get("template_id"))
        combo_id = _normalize_str(row.get("combo_id"))
        parent_gen_id = _normalize_str(row.get("parent_gen_id"))
        llm_model_id = _normalize_str(row.get("llm_model_id"))

        stage_modes = template_modes.get(stage or "", {})
        mode = stage_modes.get(template_id or "", None)
        if stage == "draft":
            mode = mode or "llm"
        elif stage == "essay":
            mode = mode or "llm"
        elif stage == "evaluation":
            mode = mode or "llm"

        metadata: Dict[str, object] = {
            "stage": stage,
            "gen_id": gen_id,
            "origin_cohort_id": str(cohort_id),
            "mode": mode or "llm",
        }
        if template_id:
            metadata["template_id"] = template_id
        if combo_id and stage == "draft":
            metadata["combo_id"] = combo_id
        if parent_gen_id:
            metadata["parent_gen_id"] = parent_gen_id
        if llm_model_id:
            metadata["llm_model_id"] = llm_model_id

        replicate_val = row.get("replicate")
        replicate = _normalize_int(replicate_val, default=1)
        metadata["replicate"] = replicate

        data_layer.write_main_metadata(stage, gen_id, metadata)


def persist_membership_csv(
    *,
    cohort_id: str,
    membership: pd.DataFrame,
    data_layer: GensDataLayer,
) -> tuple[pd.DataFrame, Path]:
    """Write the slim membership CSV and return (slim_df, path)."""

    paths = data_layer.paths
    cohort_dir = paths.cohorts_dir / str(cohort_id)
    cohort_dir.mkdir(parents=True, exist_ok=True)
    out_path = cohort_dir / "membership.csv"

    columns = ["stage", "gen_id"]
    if membership is None or membership.empty:
        slim_df = pd.DataFrame(columns=columns)
    else:
        missing = [col for col in columns if col not in membership.columns]
        if missing:
            working = membership.copy()
            for col in missing:
                working[col] = ""
            slim_df = working[columns].drop_duplicates(subset=columns)
        else:
            slim_df = membership[columns].drop_duplicates(subset=columns)

    slim_df = slim_df.reset_index(drop=True)
    slim_df.to_csv(out_path, index=False)
    return slim_df, out_path


def validate_cohort_membership(
    membership: pd.DataFrame,
    *,
    data_root: Path,
    strict: bool = True,
) -> None:
    """Ensure parent references are present in the cohort membership."""

    if membership is None or membership.empty:
        return

    if "stage" not in membership.columns or "gen_id" not in membership.columns:
        return

    drafts = set(
        membership[membership["stage"] == "draft"]["gen_id"].astype(str).tolist()
    )
    essays = set(
        membership[membership["stage"] == "essay"]["gen_id"].astype(str).tolist()
    )

    essay_parent_missing: List[str] = []
    eval_parent_missing: List[str] = []
    data_layer = GensDataLayer.from_root(data_root)

    if "parent_gen_id" in membership.columns:
        essay_parents = (
            membership[
                (membership["stage"] == "essay")
                & membership["parent_gen_id"].notna()
            ]["parent_gen_id"].astype(str)
        )
        for pid in essay_parents:
            if pid not in drafts:
                essay_parent_missing.append(pid)

        eval_parents = (
            membership[
                (membership["stage"] == "evaluation")
                & membership["parent_gen_id"].notna()
            ]["parent_gen_id"].astype(str)
        )
        for pid in eval_parents:
            if pid in essays:
                continue
            try:
                data_layer.read_main_metadata("essay", pid)
            except DDError as err:
                if err.code is not Err.DATA_MISSING:
                    raise
                eval_parent_missing.append(pid)
                continue

    if not strict:
        return

    if essay_parent_missing or eval_parent_missing:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "cohort_parent_integrity_failed",
                "missing_draft_parents": essay_parent_missing,
                "missing_essay_parents": eval_parent_missing,
            },
        )


@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    required_resource_keys={"data_root"},
)
def cohort_membership(
    context,
    cohort_id: str,
    selected_combo_mappings: pd.DataFrame,
) -> pd.DataFrame:
    """Build the authoritative cohort membership CSV with normalized columns per stage.

    Branches on the presence of data/2_tasks/selected_essays.txt:
    - If present: curated mode — recover task fields from existing gens metadata for drafts/essays
      using the selected essay gen_ids; expand evaluations over active axes.
    - If absent: Cartesian mode — derive drafts from (content_combinations × active draft templates ×
      generation models), essays from (drafts × active essay templates), and evaluations from
      (essays × active evaluation templates × evaluation models).

    Writes data/cohorts/<cohort_id>/membership.csv and registers dynamic partitions add-only.
    Validates parent integrity (essay parents among draft ids; evaluation parents among essay ids).
    Returns a DataFrame of all rows written.

    Note: This asset does not delete previously registered partitions. To reset the
    partition registry, use the global maintenance asset `prune_dynamic_partitions`
    before rebuilding a cohort, or add a cohort-scoped pruner as a separate asset.
    """

    paths_obj = Paths.from_context(context)
    data_root = paths_obj.data_root
    data_layer = GensDataLayer.from_root(data_root)

    selection_cfg = _load_curated_selection_config(data_root)
    selected_ids = selection_cfg.ids

    template_modes = {
        "draft": _template_mode_map(_read_templates_safe(data_root, "draft")),
        "essay": _template_mode_map(_read_templates_safe(data_root, "essay")),
        "evaluation": _template_mode_map(_read_templates_safe(data_root, "evaluation")),
    }

    essay_templates_df = read_templates(data_root, "essay", filter_active=True)
    essay_template_ids: List[str] = (
        essay_templates_df["template_id"].astype(str).tolist()
        if not essay_templates_df.empty
        else []
    )

    replication_cfg = _require_replication_config(data_root)
    builder = CohortBuilder(
        cohort_id=str(cohort_id),
        data_layer=data_layer,
        replication_config=replication_cfg,
    )

    rows: List[MembershipRow]
    eval_stats: Dict[str, int]

    spec_dir = data_root / "cohorts" / str(cohort_id) / "spec"
    if spec_dir.exists():
        catalogs = _build_spec_catalogs(data_root, selected_combo_mappings)
        spec_plan = load_cohort_plan(spec_dir, catalogs=catalogs)
        rows = builder.build_from_spec_plan(spec_plan)
        unique_essays = {
            evaluation.essay.key() for evaluation in spec_plan.evaluations
        }
        eval_stats = {
            "created": len(spec_plan.evaluations),
            "fully_covered": len(unique_essays),
        }
    else:
        rows = []

        if selection_cfg.selection_type == "essay":
            rows.extend(builder.build_from_essays(selected_ids))
        elif selection_cfg.selection_type == "draft":
            rows.extend(
                builder.build_from_drafts(
                    selected_ids,
                    essay_template_ids=essay_template_ids,
                )
            )
        else:
            draft_df = read_templates(data_root, "draft", filter_active=True)
            draft_template_ids = (
                draft_df["template_id"].astype(str).tolist() if not draft_df.empty else []
            )

            gen_models_df = read_llm_models(data_root)
            generation_models = gen_models_df[gen_models_df["for_generation"] == True]
            generation_model_ids = (
                generation_models["id"].astype(str).tolist()
                if not generation_models.empty
                else []
            )

            sel_df = (
                selected_combo_mappings
                if isinstance(selected_combo_mappings, pd.DataFrame)
                else pd.DataFrame()
            )

            combo_ids: List[str] = []
            if not sel_df.empty and "combo_id" in sel_df.columns:
                combo_ids = sel_df["combo_id"].astype(str).dropna().unique().tolist()

            rows.extend(
                builder.build_cartesian(
                    combo_ids=combo_ids,
                    draft_template_ids=draft_template_ids,
                    essay_template_ids=essay_template_ids,
                    generation_model_ids=generation_model_ids,
                )
            )

        eval_tpl_ids, eval_model_ids = _eval_axes(data_root)
        eval_rows, eval_stats = builder.build_evaluations(
            evaluation_templates=eval_tpl_ids,
            evaluation_models=eval_model_ids,
        )
        rows.extend(eval_rows)

    row_dicts = [row.to_dict() for row in rows]
    if row_dicts:
        membership_df = pd.DataFrame(row_dicts)
        if {"stage", "gen_id"}.issubset(membership_df.columns):
            membership_df = membership_df.drop_duplicates(subset=["stage", "gen_id"])
        else:
            membership_df = membership_df.drop_duplicates()
    else:
        membership_df = pd.DataFrame(columns=MEMBERSHIP_COLUMNS)

    validate_cohort_membership(membership_df, data_root=data_root)

    seed_cohort_metadata(
        data_layer=data_layer,
        cohort_id=str(cohort_id),
        membership=membership_df,
        template_modes=template_modes,
    )

    slim_df, membership_path = persist_membership_csv(
        cohort_id=str(cohort_id),
        membership=membership_df,
        data_layer=data_layer,
    )

    draft_count = int((slim_df["stage"] == "draft").sum() if not slim_df.empty else 0)
    essay_count = int((slim_df["stage"] == "essay").sum() if not slim_df.empty else 0)
    evaluation_count = int((slim_df["stage"] == "evaluation").sum() if not slim_df.empty else 0)

    evaluation_fill_added = int(eval_stats.get("created", 0))
    fully_covered = int(eval_stats.get("fully_covered", 0))

    context.add_output_metadata(
        {
            "rows": MetadataValue.int(len(slim_df)),
            "drafts": MetadataValue.int(draft_count),
            "essays": MetadataValue.int(essay_count),
            "evaluations": MetadataValue.int(evaluation_count),
            "evaluation_fill_added": MetadataValue.int(evaluation_fill_added),
            "fill_up_fully_covered": MetadataValue.int(fully_covered),
            "origin_cohort_id": MetadataValue.text(str(cohort_id)),
            "membership_path": MetadataValue.path(str(membership_path)),
        }
    )

    return membership_df

@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    required_resource_keys={"data_root"},
    io_manager_key="io_manager",
    deps=["prune_dynamic_partitions"],
)
def register_cohort_partitions(context, cohort_membership: pd.DataFrame) -> Dict[str, int]:
    """Register dynamic partitions by gen_id for draft/essay/evaluation (add-only).

    Accepts the cohort_membership DataFrame to guarantee ordering and avoid side effects
    inside the membership builder.
    """
    instance = context.instance

    def _add_only(name: str, keys: Iterable[str]) -> int:
        keys = [k for k in keys if isinstance(k, str) and k]
        if not keys:
            return 0
        existing = set(instance.get_dynamic_partitions(name))
        to_add = [k for k in keys if k not in existing]
        if to_add:
            instance.add_dynamic_partitions(name, to_add)
        return len(to_add)

    df = cohort_membership if isinstance(cohort_membership, pd.DataFrame) else pd.DataFrame()
    if df.empty:
        added_draft = added_essay = added_eval = 0
    else:
        added_draft = _add_only(draft_gens_partitions.name, df[df["stage"] == "draft"]["gen_id"].astype(str))
        added_essay = _add_only(essay_gens_partitions.name, df[df["stage"] == "essay"]["gen_id"].astype(str))
        added_eval = _add_only(
            evaluation_gens_partitions.name, df[df["stage"] == "evaluation"]["gen_id"].astype(str)
        )

    context.add_output_metadata(
        {
            "partitions_added_draft": MetadataValue.int(added_draft),
            "partitions_added_essay": MetadataValue.int(added_essay),
            "partitions_added_evaluation": MetadataValue.int(added_eval),
        }
    )
    return {
        "draft": added_draft,
        "essay": added_essay,
        "evaluation": added_eval,
    }
@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    io_manager_key="io_manager",
    required_resource_keys={"data_root"},
)
def cohort_id(context, content_combinations: list[ContentCombination]) -> str:
    """Compute a deterministic cohort_id from the current manifest and persist it."""
    data_root = Paths.from_context(context).data_root
    # Build manifest from active axes
    # Load axes strictly; let underlying errors surface naturally
    def _tpl_ids(kind: str) -> list[str]:
        df = read_templates(data_root, kind, filter_active=True)
        if "active" in df.columns:
            df = df[df["active"] == True]
        return sorted(df["template_id"].astype(str).tolist()) if not df.empty else []

    drafts = _tpl_ids("draft")
    essays = _tpl_ids("essay")
    evals = _tpl_ids("evaluation")

    mdf = read_llm_models(data_root)
    gen_models = sorted(mdf[mdf["for_generation"] == True]["id"].astype(str).tolist()) if not mdf.empty else []
    eval_models = sorted(mdf[mdf["for_evaluation"] == True]["id"].astype(str).tolist()) if not mdf.empty else []
    combos = sorted([str(c.combo_id) for c in (content_combinations or [])])
    rep_cfg = _require_replication_config(data_root)

    manifest = {
        "combos": combos,
        "templates": {"draft": drafts, "essay": essays, "evaluation": evals},
        "llms": {"generation": gen_models, "evaluation": eval_models},
        "replication": rep_cfg,
    }
    override = None
    asset_cfg = getattr(context, "asset_config", None)
    if asset_cfg:
        override = asset_cfg.get("override")
    else:
        op_ctx = getattr(context, "op_execution_context", None)
        if op_ctx and getattr(op_ctx, "op_config", None):
            override = op_ctx.op_config.get("override")
    env_override = get_env_cohort_id()
    cid = compute_cohort_id("cohort", manifest, explicit=(override or env_override))
    write_manifest(str(data_root), cid, manifest)
    instance = context.instance
    has_dynamic_partition = getattr(instance, "has_dynamic_partition", None)
    if callable(has_dynamic_partition):
        already_registered = has_dynamic_partition(cohort_reports_partitions.name, cid)
    else:
        existing = set(instance.get_dynamic_partitions(cohort_reports_partitions.name))
        already_registered = cid in existing
    if not already_registered:
        instance.add_dynamic_partitions(cohort_reports_partitions.name, [cid])

    context.add_output_metadata({
        "origin_cohort_id": MetadataValue.text(cid),
        "manifest_path": MetadataValue.path(str((data_root / "cohorts" / cid / "manifest.json").resolve())),
        "partition_registered": MetadataValue.bool(True),
    })
    return cid


@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
)
def selected_combo_mappings(context) -> pd.DataFrame:
    """Regenerate selected combo mappings from active concepts (deterministic ID).

    Output is kept in-memory via the in-memory IO manager; no CSV is written to disk.
    """
    from ..utils.combo_ids import ComboIDManager
    data_root = Paths.from_context(context).data_root
    cfg = context.resources.experiment_config
    level = getattr(cfg, "description_level", "paragraph")
    k_max = int(getattr(cfg, "k_max", 2))
    concepts = read_concepts(data_root, filter_active=True)
    if not concepts:
        context.add_output_metadata({"count": MetadataValue.int(0), "reason": MetadataValue.text("no active concepts")})
        return pd.DataFrame(columns=["combo_id","version","concept_id","description_level","k_max","created_at"])  # empty
    selected = concepts[: max(1, min(k_max, len(concepts)))]
    manager = ComboIDManager(str(data_root / "combo_mappings.csv"))
    combo_id = manager.get_or_create_combo_id([c.concept_id for c in selected], level, k_max)
    rows: list[dict] = []
    now = None
    for c in sorted([c.concept_id for c in selected]):
        rows.append({
            "combo_id": combo_id,
            "version": "v1",
            "concept_id": c,
            "description_level": level,
            "k_max": int(k_max),
            "created_at": now or "",
        })
    df = pd.DataFrame(rows)
    context.add_output_metadata({"count": MetadataValue.int(len(df)), "combo_id": MetadataValue.text(combo_id)})
    return df


@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    io_manager_key="io_manager",
    required_resource_keys={"experiment_config", "data_root"},
)
def content_combinations(
    context,
    selected_combo_mappings: pd.DataFrame,
) -> list[ContentCombination]:
    """Build combinations for generation using in-memory selected_combo_mappings data.

    If the provided DataFrame is empty or lacks valid combos, fall back to deriving a single
    combination from the active concepts.
    """
    data_root = Paths.from_context(context).data_root

    sel = selected_combo_mappings if isinstance(selected_combo_mappings, pd.DataFrame) else pd.DataFrame()

    if not sel.empty:
        all_concepts = {c.concept_id: c for c in read_concepts(data_root, filter_active=False)}
        combos: list[ContentCombination] = []
        for combo_id, group in sel.groupby("combo_id"):
            level = (
                str(group.iloc[0]["description_level"])
                if "description_level" in group.columns
                else "paragraph"
            )
            concept_ids = [str(cid) for cid in group["concept_id"].astype(str).tolist()]
            concepts = [all_concepts[cid] for cid in concept_ids if cid in all_concepts]
            if len(concepts) != len(concept_ids):
                continue
            combos.append(
                ContentCombination.from_concepts(
                    concepts,
                    level=level,
                    combo_id=str(combo_id),
                )
            )
        if combos:
            return combos
    # Fallback: derive from active concepts using description_level/k_max
    cfg = context.resources.experiment_config
    level = getattr(cfg, "description_level", "paragraph")
    k_max = int(getattr(cfg, "k_max", 2))
    concepts = [c for c in read_concepts(data_root, filter_active=True)]
    concepts = concepts[: max(1, min(k_max, len(concepts)))]
    combo_id = generate_combo_id([c.concept_id for c in concepts], level, k_max)
    return [ContentCombination.from_concepts(concepts, level=level, combo_id=combo_id)]
