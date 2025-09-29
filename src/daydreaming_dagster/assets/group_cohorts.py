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
from typing import Dict, Iterable, List, Tuple

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
from ..data_layer.paths import Paths
from ..data_layer.gens_data_layer import GensDataLayer


@dataclass
class SelectedEssaysConfig:
    mode: str
    essay_ids: List[str]
    fill_up: bool


def _load_selected_essays_config(data_root: Path) -> SelectedEssaysConfig:
    sel = data_root / "2_tasks" / "selected_essays.txt"
    if not sel.exists():
        return SelectedEssaysConfig(mode="cartesian", essay_ids=[], fill_up=False)

    mode = "curated"
    fill_up = False
    essays: List[str] = []
    for raw in sel.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            directive = line[1:].strip().lower()
            if directive.startswith("mode:"):
                value = directive.split(":", 1)[1].strip().lower()
                if value == "evaluation-only":
                    mode = "evaluation-only"
            elif directive in {"skip-existing-evaluations", "fill-up", "fillup"}:
                fill_up = True
            continue
        essays.append(line)
    if mode != "evaluation-only":
        fill_up = False
    return SelectedEssaysConfig(mode=mode, essay_ids=essays, fill_up=fill_up)


def _existing_evaluations_by_combo(data_root: Path, essay_id: str) -> Dict[tuple[str, str], set[str]]:
    combos: Dict[tuple[str, str], set[str]] = defaultdict(set)
    eval_root = data_root / "gens" / "evaluation"
    if not eval_root.exists():
        return combos
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
        combos[(tpl, model)].add(gen_dir.name)
    return combos


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
        if combo_id:
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

    selected_cfg = _load_selected_essays_config(data_root)
    selected_essays = selected_cfg.essay_ids
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

    # Replication config: required and authoritative
    rep_cfg = read_replication_config(data_root)
    if not isinstance(rep_cfg, dict):
        raise ValueError("replication_config.csv missing or unreadable; expected per-stage 'replicates' values")
    for _st in ("draft", "essay", "evaluation"):
        if _st not in rep_cfg or not isinstance(rep_cfg.get(_st), int) or rep_cfg.get(_st, 0) < 1:
            raise ValueError("replication_config.csv must define integer replicates>=1 for stages: draft, essay, evaluation")

    essay_seed_combo: Dict[str, str] = {}
    evaluation_only_essays: Dict[str, Dict[str, str]] = {}
    existing_eval_cache: Dict[str, Dict[tuple[str, str], set[str]]] = {}
    evaluation_only_fully_covered: set[str] = set()
    evaluation_fill_added = 0
    draft_combo_cache: Dict[str, str] = {}
    data_layer = GensDataLayer.from_root(data_root)

    if selected_essays and selected_cfg.mode == "evaluation-only":
        for essay_src_id in selected_essays:
            essay_meta_path = data_root / "gens" / "essay" / essay_src_id / "metadata.json"
            if not essay_meta_path.exists():
                # Detect mis-specified draft ids for clearer error
                draft_meta_path = data_root / "gens" / "draft" / essay_src_id / "metadata.json"
                if draft_meta_path.exists():
                    raise ValueError(
                        "selected_essays.txt (evaluation-only) requires essay gen_ids; "
                        f"'{essay_src_id}' is a draft gen_id."
                    )
                raise ValueError(
                    f"Essay metadata not found for '{essay_src_id}'. Ensure the essay exists before building the cohort."
                )

            essay_gen = load_generation(data_root / "gens", "essay", essay_src_id)
            essay_meta = essay_gen.get("metadata") or {}
            draft_parent_src = str(essay_meta.get("parent_gen_id") or "").strip()
            if not draft_parent_src:
                raise ValueError(
                    f"Essay '{essay_src_id}' is missing parent_gen_id; cannot derive evaluation tasks."
                )

            if draft_parent_src not in draft_combo_cache:
                draft_meta_path = data_root / "gens" / "draft" / draft_parent_src / "metadata.json"
                if not draft_meta_path.exists():
                    raise ValueError(
                        f"Draft parent '{draft_parent_src}' metadata not found for essay '{essay_src_id}'."
                    )
                draft_meta = load_generation(data_root / "gens", "draft", draft_parent_src).get("metadata") or {}
                draft_combo = str(draft_meta.get("combo_id") or "").strip()
                if not draft_combo:
                    raise ValueError(
                        f"Draft '{draft_parent_src}' is missing combo_id metadata; required for essay '{essay_src_id}'."
                    )
                draft_combo_cache[draft_parent_src] = draft_combo

            combo_id = draft_combo_cache[draft_parent_src]
            evaluation_only_essays[essay_src_id] = {
                "combo_id": combo_id,
                "template_id": str(essay_meta.get("template_id") or essay_meta.get("essay_template") or "").strip(),
            }
            essay_seed_combo[essay_src_id] = combo_id
            if selected_cfg.fill_up:
                existing_eval_cache[essay_src_id] = _existing_evaluations_by_combo(data_root, essay_src_id)

    elif selected_essays:
        # Curated mode — rebuild tasks from prior gens metadata and compute cohort-scoped ids
        for essay_src_id in selected_essays:
            # Load essay metadata
            essay_gen = load_generation(data_root / "gens", "essay", essay_src_id)
            essay_meta = essay_gen.get("metadata") or {}
            essay_tpl = str(essay_meta.get("template_id") or essay_meta.get("essay_template") or "").strip()
            draft_parent_src = str(essay_meta.get("parent_gen_id") or "").strip()
            # Model ids are taken directly from llm_model_id metadata
            essay_llm_model = str(essay_meta.get("llm_model_id") or "").strip()
            if not essay_tpl:
                raise ValueError("Selected essay is missing required template_id")

            # Load draft metadata
            if not draft_parent_src:
                raise ValueError("Selected essay is missing parent draft gen id")
            draft_gen = load_generation(data_root / "gens", "draft", draft_parent_src)
            draft_meta = draft_gen.get("metadata") or {}
            combo_id = str(draft_meta.get("combo_id") or "").strip()
            draft_tpl = str(draft_meta.get("template_id") or draft_meta.get("draft_template") or "").strip()
            draft_llm_model = str(draft_meta.get("llm_model_id") or "").strip()
            llm_model_id = essay_llm_model or draft_llm_model
            # Validate required fields for curated reconstruction
            missing_fields = []
            if not combo_id:
                missing_fields.append("draft.combo_id")
            if not draft_tpl:
                missing_fields.append("draft.template_id")
            if not llm_model_id:
                missing_fields.append("llm_model_id")
            if missing_fields:
                raise ValueError(
                    f"Missing required metadata to reconstruct tasks: {', '.join(missing_fields)}"
                )

            # Compose cohort task ids and gen ids
            draft_task_id = f"{combo_id}__{draft_tpl}__{llm_model_id}"
            draft_cohort_gen = _draft_gen_id(
                combo_id=combo_id,
                draft_template_id=draft_tpl,
                generation_model_id=llm_model_id,
                replicate_index=1,
                cohort_id=str(cohort_id),
            )

            # Draft row (no replicates in curated mode)
            rows.append(
                {
                    "stage": "draft",
                    "gen_id": draft_cohort_gen,
                    "origin_cohort_id": str(cohort_id),
                    "parent_gen_id": "",
                    "combo_id": combo_id,
                    "template_id": draft_tpl,
                    "llm_model_id": llm_model_id,
                    "replicate": 1,
                }
            )

            # Essay rows (replicates allowed in curated mode)
            essay_reps = int(rep_cfg.get("essay"))
            for er in range(1, essay_reps + 1):
                # Salt only when needed
                salt = f"rep{er}" if essay_reps > 1 else None
                essay_task_id = f"{draft_task_id}__{essay_tpl}"
                essay_cohort_gen = _essay_gen_id(
                    draft_gen_id=draft_cohort_gen,
                    essay_template_id=essay_tpl,
                    replicate_index=er,
                    cohort_id=str(cohort_id),
                    legacy_task_id=essay_task_id,
                    salt=salt,
                )
                rows.append(
                    {
                        "stage": "essay",
                        "gen_id": essay_cohort_gen,
                        "origin_cohort_id": str(cohort_id),
                        "parent_gen_id": draft_cohort_gen,
                        "combo_id": combo_id,
                        "template_id": essay_tpl,
                        # Prefer essay model if present; else inherit draft model
                        "llm_model_id": essay_llm_model or llm_model_id,
                        "replicate": int(er),
                    }
                )
                essay_seed_combo[str(essay_cohort_gen)] = combo_id

    else:
        # Cartesian mode — derive from active axes
        # Drafts: content_combinations × active draft templates × generation models
        dtpl_df = read_templates(data_root, "draft", filter_active=True)
        gen_models_df = read_llm_models(data_root)
        gen_models_df = gen_models_df[gen_models_df["for_generation"] == True]
        # Read selected combo ids from data/2_tasks/selected_combo_mappings.csv (raise on missing)
        sel_path = data_root / "2_tasks" / "selected_combo_mappings.csv"
        sel_df = pd.read_csv(sel_path)
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
        essay_tpl_df = read_templates(data_root, "essay", filter_active=True)
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
        if selected_cfg.mode == "evaluation-only":
            if not essay_ids:
                essay_ids = list(evaluation_only_essays.keys())

        # Replication factor for evaluations
        eval_reps = int(rep_cfg.get("evaluation"))
        for essay_gen_id in essay_ids:
            essay_missing = False
            existing_counts: Dict[tuple[str, str], set[str]] = {}
            if selected_cfg.mode == "evaluation-only" and selected_cfg.fill_up:
                existing_counts = existing_eval_cache.get(essay_gen_id, {})
            for tpl in eval_tpl_ids:
                for mid in eval_model_ids:
                    existing_ids = set()
                    if existing_counts:
                        existing_ids = set(existing_counts.get((tpl, mid), set()))
                    existing_count = len(existing_ids)
                    needed = eval_reps - existing_count if eval_reps > existing_count else 0
                    if selected_cfg.mode == "evaluation-only" and selected_cfg.fill_up:
                        if needed <= 0:
                            continue
                    else:
                        needed = eval_reps
                    if needed <= 0:
                        continue
                    eval_task_id = f"{essay_gen_id}__{tpl}__{mid}"
                    for idx in range(existing_count, existing_count + needed):
                        replicate_index = idx + 1
                        attempts = 0
                        salt_options = []
                        if replicate_index == 1:
                            salt_options.append(None)
                        else:
                            salt_options.append(f"rep{replicate_index}")
                        salt_options.append(f"fill{replicate_index}")

                        eval_gen_id = None
                        for base_salt in salt_options:
                            candidate = _evaluation_gen_id(
                                essay_gen_id=essay_gen_id,
                                evaluation_template_id=tpl,
                                evaluation_model_id=mid,
                                replicate_index=replicate_index,
                                cohort_id=str(cohort_id),
                                legacy_task_id=eval_task_id,
                                salt=base_salt,
                            )
                            if candidate not in existing_ids:
                                eval_gen_id = candidate
                                break
                        if eval_gen_id is None:
                            attempt = 0
                            while eval_gen_id is None:
                                attempt += 1
                                candidate = _evaluation_gen_id(
                                    essay_gen_id=essay_gen_id,
                                    evaluation_template_id=tpl,
                                    evaluation_model_id=mid,
                                    replicate_index=replicate_index,
                                    cohort_id=str(cohort_id),
                                    legacy_task_id=eval_task_id,
                                    salt=f"fill{replicate_index}-{attempt}",
                                )
                                if candidate not in existing_ids:
                                    eval_gen_id = candidate
                                    break
                                if attempt > 20:
                                    raise ValueError(
                                        "Unable to generate unique evaluation gen_id during fill-up"
                                    )
                        existing_ids.add(eval_gen_id)
                        combo_id = essay_seed_combo.get(essay_gen_id)
                        if combo_id is None and essay_gen_id in evaluation_only_essays:
                            combo_id = evaluation_only_essays[essay_gen_id]["combo_id"]
                        rows.append(
                            {
                                "stage": "evaluation",
                                "gen_id": eval_gen_id,
                                "origin_cohort_id": str(cohort_id),
                                "parent_gen_id": essay_gen_id,
                                "combo_id": combo_id or "",
                                "template_id": tpl,
                                "llm_model_id": mid,
                                "replicate": int(replicate_index),
                            }
                        )
                        meta_payload = {
                            "stage": "evaluation",
                            "gen_id": eval_gen_id,
                            "template_id": tpl,
                            "llm_model_id": mid,
                            "parent_gen_id": essay_gen_id,
                            "combo_id": combo_id or "",
                            "origin_cohort_id": str(cohort_id),
                            "mode": "llm",
                            "replicate": int(replicate_index),
                        }
                        data_layer.write_main_metadata("evaluation", eval_gen_id, meta_payload)
                        evaluation_fill_added += 1
                        essay_missing = True
                    if selected_cfg.mode == "evaluation-only" and selected_cfg.fill_up:
                        existing_counts[(tpl, mid)] = existing_ids
            if selected_cfg.mode == "evaluation-only" and selected_cfg.fill_up and not essay_missing:
                evaluation_only_fully_covered.add(essay_gen_id)

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
        raise ValueError(
            "Cohort membership parent integrity check failed: "
            f"missing_draft_parents={len(essay_parent_missing)}, "
            f"missing_essay_parents={len(eval_parent_missing)}"
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
            "mode": MetadataValue.text(selected_cfg.mode),
            "evaluation_fill_up": MetadataValue.bool(selected_cfg.mode == "evaluation-only" and selected_cfg.fill_up),
            "fill_up_added_evaluations": MetadataValue.int(evaluation_fill_added),
            "fill_up_fully_covered_essays": MetadataValue.int(len(evaluation_only_fully_covered)),
        }
    )

    return slim_df

@asset_with_boundary(
    stage="cohort",
    group_name="cohort",
    required_resource_keys={"data_root"},
    io_manager_key="io_manager",
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
    manifest = {
        "combos": combos,
        "templates": {"draft": drafts, "essay": essays, "evaluation": evals},
        "llms": {"generation": gen_models, "evaluation": eval_models},
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
    sel = _pd.read_csv(selected_path)

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
