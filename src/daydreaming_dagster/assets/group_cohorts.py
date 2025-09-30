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
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from dagster import MetadataValue
from ._decorators import asset_with_boundary

from ..utils import ids as ids_utils
from ..utils.ids import (
    reserve_gen_id,
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
from ..models import ContentCombination
from daydreaming_dagster.models.content_combination import generate_combo_id
from ..utils.cohorts import (
    get_env_cohort_id,
    compute_cohort_id,
    write_manifest,
)
from .partitions import (
    draft_gens_partitions,
    essay_gens_partitions,
    evaluation_gens_partitions,
)
from ..utils.generation import load_generation, write_gen_metadata
from ..utils.errors import DDError, Err
from ..data_layer.paths import Paths


@dataclass
class CuratedSelectionConfig:
    selection_type: Optional[str]  # 'essay', 'draft', or None
    ids: List[str]
    mode: str
    fill_up: bool


CURATED_MODE_REGENERATE = "regenerate"
CURATED_MODE_REUSE_DRAFTS = "reuse-drafts"
CURATED_MODE_REUSE_ESSAYS = "reuse-essays"
_CURATED_MODE_ALIASES = {
    "evaluation-only": CURATED_MODE_REUSE_ESSAYS,
    "evaluations-only": CURATED_MODE_REUSE_ESSAYS,
    "reuse_drafts": CURATED_MODE_REUSE_DRAFTS,
    "reuse_essays": CURATED_MODE_REUSE_ESSAYS,
}
_CURATED_ALLOWED_MODES = {
    CURATED_MODE_REGENERATE,
    CURATED_MODE_REUSE_DRAFTS,
    CURATED_MODE_REUSE_ESSAYS,
}


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


def _parse_selection_file(path: Path) -> tuple[str, List[str], bool, bool]:
    mode = CURATED_MODE_REGENERATE
    fill_up = False
    ids: List[str] = []
    explicit_mode = False

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            directive = line[1:].strip().lower()
            if directive.startswith("mode:"):
                value = directive.split(":", 1)[1].strip().lower()
                value = _CURATED_MODE_ALIASES.get(value, value)
                if value not in _CURATED_ALLOWED_MODES:
                    raise DDError(
                        Err.INVALID_CONFIG,
                        ctx={
                            "reason": "unsupported_curated_mode",
                            "mode": value,
                        },
                    )
                mode = value
                explicit_mode = True
            elif directive in {"skip-existing-evaluations", "fill-up", "fillup", "include-existing-evaluations", "backfill-mode"}:
                fill_up = True
            continue
        ids.append(line)

    return mode, ids, fill_up, explicit_mode


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
        mode, ids, fill_up, _ = _parse_selection_file(essays_path)
        return CuratedSelectionConfig(
            selection_type="essay",
            ids=ids,
            mode=mode,
            fill_up=fill_up,
        )

    if drafts_exists:
        mode, ids, fill_up, explicit_mode = _parse_selection_file(drafts_path)
        if not explicit_mode and mode == CURATED_MODE_REGENERATE:
            mode = CURATED_MODE_REUSE_DRAFTS
        if mode == CURATED_MODE_REUSE_ESSAYS:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "reuse_essays_requires_selected_essays"},
            )
        if fill_up:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "fill_up_requires_selected_essays"},
            )
        return CuratedSelectionConfig(
            selection_type="draft",
            ids=ids,
            mode=mode,
            fill_up=False,
        )

    return CuratedSelectionConfig(
        selection_type=None,
        ids=[],
        mode=CURATED_MODE_REGENERATE,
        fill_up=False,
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


def _existing_evaluations_by_template_model(data_root: Path, essay_id: str) -> Dict[tuple[str, str], set[str]]:
    """Return existing evaluation gen_ids keyed by (template_id, llm_model_id)."""

    existing: Dict[tuple[str, str], set[str]] = defaultdict(set)
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
        existing[(tpl, model)].add(gen_dir.name)
    return existing


def _prepare_curated_entries(data_root: Path, essay_ids: Iterable[str]) -> List[CuratedEssay]:
    entries: List[CuratedEssay] = []
    for essay_src_id in essay_ids:
        essay_meta_path = data_root / "gens" / "essay" / essay_src_id / "metadata.json"
        if not essay_meta_path.exists():
            draft_meta_path = data_root / "gens" / "draft" / essay_src_id / "metadata.json"
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

        essay_meta = load_generation(data_root / "gens", "essay", essay_src_id).get("metadata") or {}
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

        draft_meta_path = data_root / "gens" / "draft" / draft_parent_src / "metadata.json"
        if not draft_meta_path.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "draft_parent_metadata_missing",
                    "draft_gen_id": draft_parent_src,
                    "essay_gen_id": essay_src_id,
                },
            )
        draft_meta = load_generation(data_root / "gens", "draft", draft_parent_src).get("metadata") or {}
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
    for draft_id in draft_ids:
        meta_path = data_root / "gens" / "draft" / draft_id / "metadata.json"
        if not meta_path.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "draft_metadata_missing",
                    "draft_gen_id": draft_id,
                },
            )
        draft_meta = load_generation(data_root / "gens", "draft", draft_id).get("metadata") or {}
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


def _seed_generation_metadata(
    data_root: Path,
    cohort_id: str,
    membership: pd.DataFrame,
    template_modes: Dict[str, Dict[str, str]],
) -> None:
    """Ensure metadata.json exists for each cohort generation prior to running stage assets."""

    if membership is None or membership.empty:
        return

    paths = Paths.from_str(data_root)
    gens_root = paths.gens_root

    for _, row in membership.iterrows():
        stage = _normalize_str(row.get("stage"))
        gen_id = _normalize_str(row.get("gen_id"))
        if stage not in {"draft", "essay", "evaluation"} or not gen_id:
            continue

        target_dir = gens_root / stage / gen_id
        meta_path = target_dir / "metadata.json"
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

        write_gen_metadata(gens_root, stage, gen_id, metadata)


@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    required_resource_keys={"data_root"},
)
def cohort_membership(
    context,
    cohort_id: str,
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

    data_root = Paths.from_context(context).data_root
    cohort_dir = data_root / "cohorts" / str(cohort_id)
    cohort_dir.mkdir(parents=True, exist_ok=True)
    out_path = cohort_dir / "membership.csv"
    # No partition pruning here; see note in docstring

    selection_cfg = _load_curated_selection_config(data_root)
    selected_ids = selection_cfg.ids
    # Cohort membership depends on llm_model_id only (provider names omitted).

    # Normalized row schema (internal while building cohort):
    #   stage, gen_id, origin_cohort_id, parent_gen_id, combo_id, template_id, llm_model_id, replicate (int, optional; default 1)
    # The persisted membership.csv is slimmed down to just stage/gen_id for each cohort.
    rows: List[Dict] = []

    template_modes = {
        "draft": _template_mode_map(_read_templates_safe(data_root, "draft")),
        "essay": _template_mode_map(_read_templates_safe(data_root, "essay")),
        "evaluation": _template_mode_map(_read_templates_safe(data_root, "evaluation")),
    }

    active_essay_templates = read_templates(data_root, "essay", filter_active=True)

    # Replication config: required and authoritative
    rep_cfg = _require_replication_config(data_root)

    essay_seed_combo: Dict[str, str] = {}
    existing_eval_cache: Dict[str, Dict[tuple[str, str], set[str]]] = {}
    fill_up_fully_covered: set[str] = set()
    evaluation_fill_added = 0

    allocator = _ReplicateAllocator(Paths.from_str(data_root).gens_root)

    def _add_draft_row(gen_id: str, combo_id: str, template_id: str, llm_model_id: str, replicate: int) -> None:
        rows.append(
            {
                "stage": "draft",
                "gen_id": gen_id,
                "origin_cohort_id": str(cohort_id),
                "parent_gen_id": "",
                "combo_id": combo_id,
                "template_id": template_id,
                "llm_model_id": llm_model_id,
                "replicate": int(replicate),
            }
        )

    def _add_essay_row(gen_id: str, parent_gen_id: str, combo_id: str, template_id: str, llm_model_id: str, replicate: int) -> None:
        rows.append(
            {
                "stage": "essay",
                "gen_id": gen_id,
                "origin_cohort_id": str(cohort_id),
                "parent_gen_id": parent_gen_id,
                "combo_id": combo_id,
                "template_id": template_id,
                "llm_model_id": llm_model_id,
                "replicate": int(replicate),
            }
        )

    def _build_curated_regenerate(entries: List[CuratedEssay]) -> None:
        for entry in entries:
            llm_model_id = entry.essay_llm_model_id or entry.draft_llm_model_id
            draft_base = (entry.combo_id, entry.draft_template_id, llm_model_id)
            draft_task_id = f"{entry.combo_id}__{entry.draft_template_id}__{llm_model_id}"
            draft_reps = int(rep_cfg.get("draft"))
            for draft_rep in allocator.allocate("draft", draft_base, draft_reps):
                draft_gen_id = _draft_gen_id(
                    combo_id=entry.combo_id,
                    draft_template_id=entry.draft_template_id,
                    generation_model_id=llm_model_id,
                    replicate_index=int(draft_rep),
                    cohort_id=str(cohort_id),
                )
                _add_draft_row(draft_gen_id, entry.combo_id, entry.draft_template_id, llm_model_id, draft_rep)

                essay_reps = int(rep_cfg.get("essay"))
                essay_base = (draft_gen_id, entry.essay_template_id)
                for essay_rep in allocator.allocate("essay", essay_base, essay_reps):
                    essay_task_id = f"{draft_task_id}__{entry.essay_template_id}"
                    essay_gen_id = _essay_gen_id(
                        draft_gen_id=draft_gen_id,
                        essay_template_id=entry.essay_template_id,
                        replicate_index=int(essay_rep),
                        cohort_id=str(cohort_id),
                        legacy_task_id=essay_task_id,
                    )
                    essay_llm_model_id = entry.essay_llm_model_id or llm_model_id
                    _add_essay_row(essay_gen_id, draft_gen_id, entry.combo_id, entry.essay_template_id, essay_llm_model_id, essay_rep)
                    essay_seed_combo[essay_gen_id] = entry.combo_id

    def _build_curated_reuse_drafts(entries: List[CuratedEssay]) -> None:
        for entry in entries:
            llm_model_id = entry.essay_llm_model_id or entry.draft_llm_model_id
            draft_llm_for_row = entry.draft_llm_model_id or llm_model_id
            _add_draft_row(entry.draft_gen_id, entry.combo_id, entry.draft_template_id, draft_llm_for_row, entry.draft_replicate)

            draft_task_id = f"{entry.combo_id}__{entry.draft_template_id}__{llm_model_id}"
            essay_reps = int(rep_cfg.get("essay"))
            essay_base = (entry.draft_gen_id, entry.essay_template_id)
            for essay_rep in allocator.allocate("essay", essay_base, essay_reps):
                essay_task_id = f"{draft_task_id}__{entry.essay_template_id}"
                essay_gen_id = _essay_gen_id(
                    draft_gen_id=entry.draft_gen_id,
                    essay_template_id=entry.essay_template_id,
                    replicate_index=int(essay_rep),
                    cohort_id=str(cohort_id),
                    legacy_task_id=essay_task_id,
                )
                essay_llm_model_id = entry.essay_llm_model_id or llm_model_id
                _add_essay_row(essay_gen_id, entry.draft_gen_id, entry.combo_id, entry.essay_template_id, essay_llm_model_id, essay_rep)
                essay_seed_combo[essay_gen_id] = entry.combo_id

    def _build_curated_reuse_essays(entries: List[CuratedEssay]) -> None:
        for entry in entries:
            llm_model_id = entry.essay_llm_model_id or entry.draft_llm_model_id
            draft_llm_for_row = entry.draft_llm_model_id or llm_model_id
            _add_draft_row(entry.draft_gen_id, entry.combo_id, entry.draft_template_id, draft_llm_for_row, entry.draft_replicate)
            essay_llm_model_id = entry.essay_llm_model_id or llm_model_id
            _add_essay_row(entry.essay_gen_id, entry.draft_gen_id, entry.combo_id, entry.essay_template_id, essay_llm_model_id, entry.essay_replicate)
            essay_seed_combo[entry.essay_gen_id] = entry.combo_id
            existing_eval_cache[entry.essay_gen_id] = _existing_evaluations_by_template_model(data_root, entry.essay_gen_id)

    def _build_from_drafts_regenerate(entries: List[CuratedDraft]) -> None:
        essay_tpl_ids = active_essay_templates["template_id"].astype(str).tolist()
        draft_reps = int(rep_cfg.get("draft"))
        essay_reps = int(rep_cfg.get("essay"))
        for entry in entries:
            draft_base = (entry.combo_id, entry.draft_template_id, entry.draft_llm_model_id)
            draft_task_id = f"{entry.combo_id}__{entry.draft_template_id}__{entry.draft_llm_model_id}"
            for draft_rep in allocator.allocate("draft", draft_base, draft_reps):
                draft_gen_id = _draft_gen_id(
                    combo_id=entry.combo_id,
                    draft_template_id=entry.draft_template_id,
                    generation_model_id=entry.draft_llm_model_id,
                    replicate_index=int(draft_rep),
                    cohort_id=str(cohort_id),
                )
                _add_draft_row(
                    draft_gen_id,
                    entry.combo_id,
                    entry.draft_template_id,
                    entry.draft_llm_model_id,
                    draft_rep,
                )

                for essay_tpl in essay_tpl_ids:
                    essay_base = (draft_gen_id, essay_tpl)
                    for essay_rep in allocator.allocate("essay", essay_base, essay_reps):
                        essay_task_id = f"{draft_task_id}__{essay_tpl}"
                        essay_gen_id = _essay_gen_id(
                            draft_gen_id=draft_gen_id,
                            essay_template_id=essay_tpl,
                            replicate_index=int(essay_rep),
                            cohort_id=str(cohort_id),
                            legacy_task_id=essay_task_id,
                        )
                        _add_essay_row(
                            essay_gen_id,
                            draft_gen_id,
                            entry.combo_id,
                            essay_tpl,
                            entry.draft_llm_model_id,
                            essay_rep,
                        )
                        essay_seed_combo[essay_gen_id] = entry.combo_id

    def _build_from_drafts_reuse_drafts(entries: List[CuratedDraft]) -> None:
        essay_tpl_ids = active_essay_templates["template_id"].astype(str).tolist()
        essay_reps = int(rep_cfg.get("essay"))
        for entry in entries:
            _add_draft_row(
                entry.draft_gen_id,
                entry.combo_id,
                entry.draft_template_id,
                entry.draft_llm_model_id,
                entry.draft_replicate,
            )
            for essay_tpl in essay_tpl_ids:
                essay_base = (entry.draft_gen_id, essay_tpl)
                for essay_rep in allocator.allocate("essay", essay_base, essay_reps):
                    draft_task_id = f"{entry.combo_id}__{entry.draft_template_id}__{entry.draft_llm_model_id}"
                    essay_task_id = f"{draft_task_id}__{essay_tpl}"
                    essay_gen_id = _essay_gen_id(
                        draft_gen_id=entry.draft_gen_id,
                        essay_template_id=essay_tpl,
                        replicate_index=int(essay_rep),
                        cohort_id=str(cohort_id),
                        legacy_task_id=essay_task_id,
                    )
                    _add_essay_row(
                        essay_gen_id,
                        entry.draft_gen_id,
                        entry.combo_id,
                        essay_tpl,
                        entry.draft_llm_model_id,
                        essay_rep,
                    )
                    essay_seed_combo[essay_gen_id] = entry.combo_id

    if selection_cfg.selection_type == "essay":
        curated_entries = _prepare_curated_entries(data_root, selected_ids)
        if selection_cfg.mode == CURATED_MODE_REUSE_ESSAYS:
            _build_curated_reuse_essays(curated_entries)
        elif selection_cfg.mode == CURATED_MODE_REUSE_DRAFTS:
            _build_curated_reuse_drafts(curated_entries)
        else:
            _build_curated_regenerate(curated_entries)
    elif selection_cfg.selection_type == "draft":
        curated_drafts = _prepare_curated_drafts(data_root, selected_ids)
        if selection_cfg.mode == CURATED_MODE_REUSE_DRAFTS:
            _build_from_drafts_reuse_drafts(curated_drafts)
        else:
            _build_from_drafts_regenerate(curated_drafts)

    else:
        # Cartesian mode — derive from active axes
        # Drafts: content_combinations × active draft templates × generation models
        dtpl_df = read_templates(data_root, "draft", filter_active=True)
        gen_models_df = read_llm_models(data_root)
        gen_models_df = gen_models_df[gen_models_df["for_generation"] == True]
        # Read selected combo ids from data/2_tasks/selected_combo_mappings.csv (raise on missing)
        sel_path = data_root / "2_tasks" / "selected_combo_mappings.csv"
        try:
            sel_df = pd.read_csv(sel_path)
        except FileNotFoundError as exc:
            raise DDError(
                Err.DATA_MISSING,
                ctx={"reason": "selected_combo_mappings_missing", "path": str(sel_path)},
                cause=exc,
            )
        combo_ids: List[str] = []
        if not sel_df.empty and "combo_id" in sel_df.columns:
            combo_ids = sel_df["combo_id"].astype(str).dropna().unique().tolist()

        draft_reps = int(rep_cfg.get("draft"))
        essay_reps = int(rep_cfg.get("essay"))
        for combo_id in combo_ids:
            for _, trow in dtpl_df.iterrows():
                draft_tpl = str(trow["template_id"])
                for _, mrow in gen_models_df.iterrows():
                    mid = str(mrow["id"])
                    # provider model name omitted
                    draft_task_id = f"{combo_id}__{draft_tpl}__{mid}"
                    for dr in range(1, draft_reps + 1):
                        # Preserve prior unsalted ids for first replicate
                        salt_d = None if dr == 1 else f"rep{dr}"
                        draft_cohort_gen = _draft_gen_id(
                            combo_id=combo_id,
                            draft_template_id=draft_tpl,
                            generation_model_id=mid,
                            replicate_index=dr,
                            cohort_id=str(cohort_id),
                            salt=salt_d,
                        )
                        rows.append(
                            {
                                "stage": "draft",
                                "gen_id": draft_cohort_gen,
                                "origin_cohort_id": str(cohort_id),
                                "parent_gen_id": "",
                                "combo_id": combo_id,
                                "template_id": draft_tpl,
                                "llm_model_id": mid,
                                "replicate": int(dr),
                            }
                        )

        # Essays: drafts × active essay templates
        essay_tpl_df = active_essay_templates
        draft_rows = [r for r in rows if r.get("stage") == "draft"]
        for d in draft_rows:
            draft_cohort_gen = str(d.get("gen_id"))
            # Normalized membership uses template_id + llm_model_id
            draft_tpl = str(d.get("template_id"))
            combo_id = str(d.get("combo_id"))
            mid = str(d.get("llm_model_id"))
            # provider model name omitted
            draft_task_id = f"{combo_id}__{draft_tpl}__{mid}"
            for _, et in essay_tpl_df.iterrows():
                essay_tpl = str(et["template_id"])
                essay_task_id = f"{draft_task_id}__{essay_tpl}"
                for er in range(1, essay_reps + 1):
                    # Preserve unsalted id for the primary path (dr==1 and er==1)
                    if int(d.get("replicate", 1)) == 1 and er == 1:
                        salt_e = None
                    else:
                        salt_e = f"rep{int(d.get('replicate', 1))}-{er}"
                    essay_cohort_gen = _essay_gen_id(
                        draft_gen_id=draft_cohort_gen,
                        essay_template_id=essay_tpl,
                        replicate_index=er,
                        cohort_id=str(cohort_id),
                        legacy_task_id=essay_task_id,
                        salt=salt_e,
                    )
                    rows.append(
                        {
                            "stage": "essay",
                            "gen_id": essay_cohort_gen,
                            "origin_cohort_id": str(cohort_id),
                            "parent_gen_id": draft_cohort_gen,
                            "combo_id": combo_id,
                            "template_id": essay_tpl,
                            # default: essay inherits generation model id
                            "llm_model_id": mid,
                            "replicate": int(er),
                        }
                    )
                    essay_seed_combo[str(essay_cohort_gen)] = combo_id

    # After drafts and essays are built, expand evaluations once using shared helper
    eval_tpl_ids, eval_model_ids = _eval_axes(data_root)
    if eval_tpl_ids and eval_model_ids:
        essay_ids = list(essay_seed_combo.keys())
        eval_reps = int(rep_cfg.get("evaluation"))
        for essay_gen_id in essay_ids:
            combo_ref = essay_seed_combo.get(essay_gen_id, "")
            existing_counts: Dict[tuple[str, str], set[str]] = {}
            if (
                selection_cfg.selection_type == "essay"
                and selection_cfg.mode == CURATED_MODE_REUSE_ESSAYS
                and selection_cfg.fill_up
            ):
                existing_counts = existing_eval_cache.get(essay_gen_id, {})

            created_for_essay = 0
            for tpl in eval_tpl_ids:
                for mid in eval_model_ids:
                    if (
                        selection_cfg.selection_type == "essay"
                        and selection_cfg.mode == CURATED_MODE_REUSE_ESSAYS
                        and selection_cfg.fill_up
                    ):
                        current = existing_counts.get((tpl, mid), set())
                        # Add existing evaluations to membership (up to eval_reps)
                        existing_to_include = sorted(current)[:eval_reps]
                        for existing_gen_id in existing_to_include:
                            rows.append(
                                {
                                    "stage": "evaluation",
                                    "gen_id": existing_gen_id,
                                    "origin_cohort_id": str(cohort_id),
                                    "parent_gen_id": essay_gen_id,
                                    "combo_id": combo_ref or "",
                                    "template_id": tpl,
                                    "llm_model_id": mid,
                                    "replicate": 0,  # Existing evals don't have cohort replicate numbers
                                }
                            )
                        needed = max(0, eval_reps - len(existing_to_include))
                        if needed == 0:
                            continue
                    else:
                        needed = eval_reps

                    replicate_indices = allocator.allocate(
                        "evaluation", (essay_gen_id, tpl, mid), needed
                    )
                    for replicate_index in replicate_indices:
                        legacy_task_id = f"{essay_gen_id}__{tpl}__{mid}"
                        eval_gen_id = _evaluation_gen_id(
                            essay_gen_id=essay_gen_id,
                            evaluation_template_id=tpl,
                            evaluation_model_id=mid,
                            replicate_index=int(replicate_index),
                            cohort_id=str(cohort_id),
                            legacy_task_id=legacy_task_id,
                        )
                        rows.append(
                            {
                                "stage": "evaluation",
                                "gen_id": eval_gen_id,
                                "origin_cohort_id": str(cohort_id),
                                "parent_gen_id": essay_gen_id,
                                "combo_id": combo_ref or "",
                                "template_id": tpl,
                                "llm_model_id": mid,
                                "replicate": int(replicate_index),
                            }
                        )
                        evaluation_fill_added += 1
                        created_for_essay += 1
            if (
                selection_cfg.selection_type == "essay"
                and selection_cfg.mode == CURATED_MODE_REUSE_ESSAYS
                and selection_cfg.fill_up
                and created_for_essay == 0
            ):
                fill_up_fully_covered.add(essay_gen_id)

    # Deduplicate by (stage, gen_id)
    df = pd.DataFrame(rows)
    if not df.empty:
        if "stage" in df.columns and "gen_id" in df.columns:
            df = df.drop_duplicates(subset=["stage", "gen_id"])  # authoritative id set per stage
        else:
            df = df.drop_duplicates()

    # Parent integrity validation
    stage_col = df.get("stage") if not df.empty else None
    if stage_col is not None:
        drafts = set(df[df["stage"] == "draft"]["gen_id"].astype(str).tolist())
        essays = set(df[df["stage"] == "essay"]["gen_id"].astype(str).tolist())
    else:
        drafts = set()
        essays = set()
    essay_parent_missing = []
    eval_parent_missing = []
    if "parent_gen_id" in df.columns:
        essay_parents = (
            df[(df["stage"] == "essay") & df["parent_gen_id"].notna()]["parent_gen_id"].astype(str)
        )
        for pid in essay_parents:
            if pid not in drafts:
                essay_parent_missing.append(pid)
        eval_parents = (
            df[(df["stage"] == "evaluation") & df["parent_gen_id"].notna()]["parent_gen_id"].astype(str)
        )
        for pid in eval_parents:
            if pid not in essays:
                try:
                    load_generation(data_root / "gens", "essay", pid)
                except Exception:
                    eval_parent_missing.append(pid)
    if essay_parent_missing or eval_parent_missing:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "cohort_parent_integrity_failed",
                "missing_draft_parents": essay_parent_missing,
                "missing_essay_parents": eval_parent_missing,
            },
        )

    # Seed metadata.json for cohort generations before materializing downstream assets
    _seed_generation_metadata(data_root, cohort_id, df, template_modes)

    # Persist slim membership (stage + gen_id only)
    slim_df = (
        df[["stage", "gen_id"]].drop_duplicates(subset=["stage", "gen_id"])
        if not df.empty
        else pd.DataFrame(columns=["stage", "gen_id"])
    )
    slim_df.to_csv(out_path, index=False)

    context.add_output_metadata(
        {
            "rows": MetadataValue.int(len(slim_df)),
            "drafts": MetadataValue.int(int((slim_df["stage"] == "draft").sum() if not slim_df.empty else 0)),
            "essays": MetadataValue.int(int((slim_df["stage"] == "essay").sum() if not slim_df.empty else 0)),
            "evaluations": MetadataValue.int(
                int((slim_df["stage"] == "evaluation").sum() if not slim_df.empty else 0)
            ),
            "origin_cohort_id": MetadataValue.text(str(cohort_id)),
            "membership_path": MetadataValue.path(str(out_path)),
            "mode": MetadataValue.text(selection_cfg.mode),
            "evaluation_fill_up": MetadataValue.bool(
                selection_cfg.selection_type == "essay"
                and selection_cfg.mode == CURATED_MODE_REUSE_ESSAYS
                and selection_cfg.fill_up
            ),
            "fill_up_added_evaluations": MetadataValue.int(evaluation_fill_added),
            "fill_up_fully_covered_essays": MetadataValue.int(len(fill_up_fully_covered)),
        }
    )

    return slim_df

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
    context.add_output_metadata({
        "origin_cohort_id": MetadataValue.text(cid),
        "manifest_path": MetadataValue.path(str((data_root / "cohorts" / cid / "manifest.json").resolve())),
    })
    return cid


@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
)
def selected_combo_mappings(context) -> pd.DataFrame:
    """Regenerate selected combo mappings from active concepts (deterministic ID)."""
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
def content_combinations(context) -> list[ContentCombination]:
    """Build combinations for generation. Preferred source: selected_combo_mappings.csv.

    Raises on missing/unreadable selected_combo_mappings.csv (let pandas exceptions bubble up).
    If the file is readable but yields no valid combos, fall back to one combo from active concepts.
    """
    data_root = Paths.from_context(context).data_root
    import pandas as _pd
    selected_path = data_root / "2_tasks" / "selected_combo_mappings.csv"
    try:
        sel = _pd.read_csv(selected_path)
    except FileNotFoundError as exc:
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "selected_combo_mappings_missing", "path": str(selected_path)},
            cause=exc,
        )

    if not sel.empty:
        all_concepts = {c.concept_id: c for c in read_concepts(data_root, filter_active=False)}
        combos: list[ContentCombination] = []
        for combo_id, group in sel.groupby("combo_id"):
            level = str(group.iloc[0]["description_level"]) if "description_level" in group.columns else "paragraph"
            concept_ids = [str(cid) for cid in group["concept_id"].astype(str).tolist()]
            concepts = [all_concepts[cid] for cid in concept_ids if cid in all_concepts]
            if len(concepts) != len(concept_ids):
                continue
            combos.append(ContentCombination.from_concepts(concepts, level=level, combo_id=str(combo_id)))
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
def _draft_gen_id(
    *,
    combo_id: str,
    draft_template_id: str,
    generation_model_id: str,
    replicate_index: int,
    cohort_id: str,
    salt: str | None = None,
) -> str:
    if ids_utils.DETERMINISTIC_GEN_IDS_ENABLED:
        signature = draft_signature(combo_id, draft_template_id, generation_model_id, replicate_index)
        return compute_deterministic_gen_id("draft", signature)
    task_id = f"{combo_id}__{draft_template_id}__{generation_model_id}"
    return reserve_gen_id("draft", task_id, run_id=cohort_id, salt=salt)


def _essay_gen_id(
    *,
    draft_gen_id: str,
    essay_template_id: str,
    replicate_index: int,
    cohort_id: str,
    legacy_task_id: str,
    salt: str | None = None,
) -> str:
    if ids_utils.DETERMINISTIC_GEN_IDS_ENABLED:
        signature = essay_signature(draft_gen_id, essay_template_id, replicate_index)
        return compute_deterministic_gen_id("essay", signature)
    return reserve_gen_id("essay", legacy_task_id, run_id=cohort_id, salt=salt)


def _evaluation_gen_id(
    *,
    essay_gen_id: str,
    evaluation_template_id: str,
    evaluation_model_id: str,
    replicate_index: int,
    cohort_id: str,
    legacy_task_id: str,
    salt: str | None = None,
) -> str:
    if ids_utils.DETERMINISTIC_GEN_IDS_ENABLED:
        signature = evaluation_signature(essay_gen_id, evaluation_template_id, evaluation_model_id, replicate_index)
        return compute_deterministic_gen_id("evaluation", signature)
    return reserve_gen_id("evaluation", legacy_task_id, run_id=cohort_id, salt=salt)
