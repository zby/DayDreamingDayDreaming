"""
Group: cohort

Assets for building cohort membership (authoritative, wide rows) and
registering dynamic partitions based on cohort membership.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

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
from ..cohorts import (
    CohortDefinition,
    build_allowlists_from_definition,
)
from ..models import ContentCombination
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
) -> dict[str, list[str]]:
    catalogs: dict[str, list[str]] = {}

    def _template_ids(kind: str) -> list[str]:
        df = read_templates(data_root, kind)
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
        catalogs["draft_template"] = drafts

    essays = _template_ids("essay")
    if essays:
        catalogs["essay_template"] = essays

    evaluations = _template_ids("evaluation")
    if evaluations:
        catalogs["evaluation_template"] = evaluations

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
            catalogs["draft_llm"] = sorted_generation
            catalogs.setdefault("essay_llm", sorted_generation)
        if evaluation_llms:
            catalogs["evaluation_llm"] = sorted(evaluation_llms)

    if "essay_llm" in catalogs:
        values = set(catalogs["essay_llm"])
        values.add("None")
        catalogs["essay_llm"] = sorted(values)

    combos: set[str] = set()

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
        catalogs["combo_id"] = sorted(combos)

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

    def build_from_spec_plan(self, plan: CohortDefinition) -> List[MembershipRow]:
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


def _read_templates_safe(
    data_root: Path,
    stage: str,
    *,
    allowlist: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Load templates for a stage and optionally filter by allowlist."""

    df = read_templates(data_root, stage)
    if df.empty:
        return pd.DataFrame()
    if allowlist:
        allowed = {str(item).strip() for item in allowlist if str(item).strip()}
        if allowed:
            df = df[df["template_id"].astype(str).str.strip().isin(allowed)]
    return df


def _template_mode_map(df: pd.DataFrame, *, default: str = "llm") -> Dict[str, str]:
    """Build a template_id → mode map from a templates DataFrame."""

    if df is None or getattr(df, "empty", True):
        return {}

    mode_map: Dict[str, str] = {}
    for _, row in df.iterrows():
        template_id = _normalize_str(row.get("template_id") or row.get("id"))
        if not template_id:
            continue
        raw_mode = row.get("generator") if "generator" in row.index else None
        mode = (raw_mode or default)
        if isinstance(mode, str):
            mode = mode.strip().lower() or default
        else:
            mode = str(mode).strip().lower() or default
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
    required_resource_keys={"data_root", "cohort_spec"},
)
def cohort_membership(
    context,
    cohort_id: str,
) -> pd.DataFrame:
    """Build the authoritative cohort membership CSV using the cohort spec as the source of truth.

    The cohort spec fully determines the membership rows (draft, essay, evaluation). Evaluation
    coverage derives exclusively from the spec's evaluation templates and models.

    Writes data/cohorts/<cohort_id>/membership.csv and registers dynamic partitions add-only.
    Validates parent integrity (essay parents among draft ids; evaluation parents among essay ids).
    Returns a DataFrame of all rows written.

    Note: This asset does not delete previously registered partitions. To reset the partition
    registry, use the global maintenance asset `prune_dynamic_partitions` before rebuilding a cohort,
    or add a cohort-scoped pruner as a separate asset.
    """

    paths_obj = Paths.from_context(context)
    data_root = paths_obj.data_root
    data_layer = GensDataLayer.from_root(data_root)

    spec_dir = data_root / "cohorts" / str(cohort_id) / "spec"
    if not spec_dir.exists():
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "cohort_spec_required",
                "cohort_id": cohort_id,
                "path": str(spec_dir),
            },
        )

    catalogs = _build_spec_catalogs(data_root)
    spec_plan = context.resources.cohort_spec.compile_definition(
        path=spec_dir,
        catalogs=catalogs,
    )
    allowlists = build_allowlists_from_definition(spec_plan)

    template_modes = {
        "draft": _template_mode_map(
            _read_templates_safe(data_root, "draft", allowlist=allowlists.draft_templates)
        ),
        "essay": _template_mode_map(
            _read_templates_safe(data_root, "essay", allowlist=allowlists.essay_templates)
        ),
        "evaluation": _template_mode_map(
            _read_templates_safe(
                data_root, "evaluation", allowlist=allowlists.evaluation_templates
            )
        ),
    }

    replication_cfg = _require_replication_config(data_root)
    builder = CohortBuilder(
        cohort_id=str(cohort_id),
        data_layer=data_layer,
        replication_config=replication_cfg,
    )

    rows = builder.build_from_spec_plan(spec_plan)
    unique_essays = {
        evaluation.essay.key() for evaluation in spec_plan.evaluations
    }
    eval_stats = {
        "created": len(spec_plan.evaluations),
        "fully_covered": len(unique_essays),
    }

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
    required_resource_keys={"data_root", "cohort_spec"},
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
    required_resource_keys={"data_root", "cohort_spec"},
)
def cohort_id(context) -> str:
    """Compute a deterministic cohort_id from the current manifest and persist it."""
    data_root = Paths.from_context(context).data_root
    asset_cfg = getattr(context, "asset_config", None)
    override = None
    if asset_cfg:
        override = asset_cfg.get("override")
    else:
        op_ctx = getattr(context, "op_execution_context", None)
        if op_ctx and getattr(op_ctx, "op_config", None):
            override = op_ctx.op_config.get("override")

    env_override = get_env_cohort_id()
    spec_name = override or env_override
    if not spec_name:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "cohort_spec_required",
                "hint": "set asset_config.override or DD_COHORT",
            },
        )

    spec_dir = data_root / "cohorts" / spec_name / "spec"
    if not spec_dir.exists():
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "cohort_spec_missing",
                "cohort_id": spec_name,
                "path": str(spec_dir),
            },
        )

    catalogs = _build_spec_catalogs(data_root)
    spec_plan = context.resources.cohort_spec.compile_definition(
        path=spec_dir,
        catalogs=catalogs,
    )
    allowlists = build_allowlists_from_definition(spec_plan)

    combos = sorted(set(allowlists.combos))
    rep_cfg = _require_replication_config(data_root)

    manifest = {
        "combos": combos,
        "templates": {
            "draft": list(allowlists.draft_templates),
            "essay": list(allowlists.essay_templates),
            "evaluation": list(allowlists.evaluation_templates),
        },
        "llms": {
            "generation": list(allowlists.generation_models),
            "evaluation": list(allowlists.evaluation_models),
        },
        "replication": rep_cfg,
    }
    explicit_id = override or env_override
    cid = compute_cohort_id("cohort", manifest, explicit=explicit_id)
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


def _load_manifest(data_root: Path, cohort_id: str) -> dict[str, object]:
    manifest_path = data_root / "cohorts" / str(cohort_id) / "manifest.json"
    if not manifest_path.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={
                "reason": "cohort_manifest_missing",
                "cohort_id": cohort_id,
                "path": str(manifest_path),
            },
        )
    try:
        return json.loads(manifest_path.read_text())
    except json.JSONDecodeError as err:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "invalid_manifest_json",
                "cohort_id": cohort_id,
                "path": str(manifest_path),
            },
        ) from err


def _manifest_combo_ids(manifest: dict[str, object]) -> list[str]:
    return [str(combo).strip() for combo in manifest.get("combos", []) if str(combo).strip()]


def _read_combo_mappings(data_root: Path) -> pd.DataFrame:
    combo_path = data_root / "combo_mappings.csv"
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
            ctx={"reason": "combo_mappings_empty", "path": str(combo_path)},
        )
    return combos_df


def _combo_rows_for_manifest(data_root: Path, cohort_id: str) -> tuple[list[str], pd.DataFrame]:
    manifest = _load_manifest(data_root, cohort_id)
    manifest_combos = _manifest_combo_ids(manifest)
    if not manifest_combos:
        return [], pd.DataFrame()

    combos_df = _read_combo_mappings(data_root)
    filtered = combos_df[combos_df["combo_id"].astype(str).isin(manifest_combos)]
    if filtered.empty:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "manifest_combos_missing",
                "combos": manifest_combos,
                "path": str(data_root / "combo_mappings.csv"),
            },
        )
    return manifest_combos, filtered


@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    io_manager_key="in_memory_io_manager",
    required_resource_keys={"data_root"},
)
def selected_combo_mappings(
    context,
    cohort_id: str,
) -> pd.DataFrame:
    """Return combo mapping rows referenced by the cohort manifest."""

    paths = Paths.from_context(context)
    data_root = paths.data_root

    manifest_combos, combos_df = _combo_rows_for_manifest(data_root, cohort_id)
    if not manifest_combos:
        columns = [
            "combo_id",
            "version",
            "concept_id",
            "description_level",
            "k_max",
            "created_at",
        ]
        context.add_output_metadata({"count": MetadataValue.int(0), "reason": MetadataValue.text("no combos in manifest")})
        return pd.DataFrame(columns=columns)

    filtered = combos_df.copy()
    filtered = filtered[filtered["combo_id"].astype(str).isin(manifest_combos)]
    filtered["combo_id"] = pd.Categorical(
        filtered["combo_id"].astype(str), categories=manifest_combos, ordered=True
    )
    filtered = filtered.sort_values("combo_id").reset_index(drop=True)
    filtered["combo_id"] = filtered["combo_id"].astype(str)

    context.add_output_metadata({"count": MetadataValue.int(len(filtered)), "combos": MetadataValue.int(len(manifest_combos))})
    return filtered


@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    io_manager_key="io_manager",
    required_resource_keys={"data_root"},
)
def content_combinations(
    context,
    cohort_id: str,
) -> list[ContentCombination]:
    """Hydrate content combinations referenced by the cohort manifest.

    The cohort spec is authoritative for combo selection. This asset resolves each combo_id listed
    in the cohort manifest into concrete concept content using the canonical combo mappings CSV.
    """

    paths = Paths.from_context(context)
    data_root = paths.data_root

    manifest_combos, combos_df = _combo_rows_for_manifest(data_root, cohort_id)
    if not manifest_combos:
        context.add_output_metadata({"count": MetadataValue.int(0), "reason": MetadataValue.text("no combos in manifest")})
        return []

    combo_path = data_root / "combo_mappings.csv"

    concepts = read_concepts(data_root)
    concept_index = {str(concept.concept_id): concept for concept in concepts}

    combos: list[ContentCombination] = []
    for combo_id in manifest_combos:
        combo_rows = combos_df[combos_df["combo_id"].astype(str) == combo_id]
        if combo_rows.empty:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "combo_definition_missing",
                    "combo_id": combo_id,
                    "path": str(combo_path),
                },
            )

        level_value = combo_rows.iloc[0].get("description_level", "paragraph")
        level = str(level_value).strip() or "paragraph"

        concept_ids = [str(value).strip() for value in combo_rows["concept_id"].astype(str).tolist() if str(value).strip()]
        resolved_concepts: list = []
        missing_concepts: list[str] = []
        for concept_id in concept_ids:
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
                    "combo_id": combo_id,
                    "missing_concepts": missing_concepts,
                },
            )

        combos.append(
            ContentCombination.from_concepts(
                resolved_concepts,
                level=level,
                combo_id=combo_id,
            )
        )

    context.add_output_metadata({"count": MetadataValue.int(len(combos))})
    return combos
