"""
Group: cohort

Assets for building cohort membership (authoritative, wide rows) and
registering dynamic partitions based on cohort membership.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from dagster import Failure, MetadataValue, asset

from ..utils.ids import reserve_gen_id
from ..utils.raw_readers import (
    read_concepts,
    read_templates,
    read_llm_models,
)
from ..models import ContentCombination
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
from ..utils.generation import load_generation


def _load_selected_essays_list(data_root: Path) -> List[str]:
    sel = data_root / "2_tasks" / "selected_essays.txt"
    if not sel.exists():
        return []
    out: List[str] = []
    for line in sel.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


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


@asset(
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

    data_root = Path(getattr(context.resources, "data_root", "data"))
    cohort_dir = data_root / "cohorts" / str(cohort_id)
    cohort_dir.mkdir(parents=True, exist_ok=True)
    out_path = cohort_dir / "membership.csv"
    # No partition pruning here; see note in docstring

    selected_essays = _load_selected_essays_list(data_root)
    # Cohort membership depends on model_id only (provider names omitted).

    # Normalized row schema across all stages:
    #   stage, gen_id, cohort_id, parent_gen_id, combo_id, template_id, llm_model_id
    rows: List[Dict] = []

    if selected_essays:
        # Curated mode — rebuild tasks from prior gens metadata and compute cohort-scoped ids
        for essay_src_id in selected_essays:
            # Load essay metadata
            try:
                essay_gen = load_generation(data_root / "gens", "essay", essay_src_id)
            except Exception as e:
                raise Failure(
                    description="Selected essay document not found",
                    metadata={
                        "function": MetadataValue.text("cohort_membership"),
                        "essay_gen_id": MetadataValue.text(str(essay_src_id)),
                        "error": MetadataValue.text(str(e)),
                    },
                ) from e
            essay_meta = essay_gen.get("metadata") or {}
            essay_tpl = str(essay_meta.get("template_id") or essay_meta.get("essay_template") or "").strip()
            draft_parent_src = str(essay_meta.get("parent_gen_id") or "").strip()
            # Model id/name: prefer essay metadata; fall back to draft metadata
            essay_model_id = str(essay_meta.get("model_id") or "").strip()
            if not essay_tpl:
                raise Failure(
                    description="Selected essay is missing required template_id",
                    metadata={
                        "function": MetadataValue.text("cohort_membership"),
                        "essay_gen_id": MetadataValue.text(str(essay_src_id)),
                    },
                )

            # Load draft metadata
            if not draft_parent_src:
                raise Failure(
                    description="Selected essay is missing parent draft gen id",
                    metadata={
                        "function": MetadataValue.text("cohort_membership"),
                        "essay_gen_id": MetadataValue.text(str(essay_src_id)),
                    },
                )
            try:
                draft_gen = load_generation(data_root / "gens", "draft", draft_parent_src)
            except Exception as e:
                raise Failure(
                    description="Parent draft document not found for selected essay",
                    metadata={
                        "function": MetadataValue.text("cohort_membership"),
                        "essay_gen_id": MetadataValue.text(str(essay_src_id)),
                        "draft_gen_id": MetadataValue.text(str(draft_parent_src)),
                        "error": MetadataValue.text(str(e)),
                    },
                ) from e
            draft_meta = draft_gen.get("metadata") or {}
            combo_id = str(draft_meta.get("combo_id") or "").strip()
            draft_tpl = str(draft_meta.get("template_id") or draft_meta.get("draft_template") or "").strip()
            draft_model_id = str(draft_meta.get("model_id") or "").strip()
            model_id = essay_model_id or draft_model_id
            # Validate required fields for curated reconstruction
            missing_fields = []
            if not combo_id:
                missing_fields.append("draft.combo_id")
            if not draft_tpl:
                missing_fields.append("draft.template_id")
            if not model_id:
                missing_fields.append("model_id")
            if missing_fields:
                raise Failure(
                    description="Missing required metadata to reconstruct draft/essay tasks for curated cohort",
                    metadata={
                        "function": MetadataValue.text("cohort_membership"),
                        "essay_gen_id": MetadataValue.text(str(essay_src_id)),
                        "draft_parent_src": MetadataValue.text(str(draft_parent_src)),
                        "missing_fields": MetadataValue.json(missing_fields),
                    },
                )

            # Compose cohort task ids and gen ids
            draft_task_id = f"{combo_id}__{draft_tpl}__{model_id}"
            draft_cohort_gen = reserve_gen_id("draft", draft_task_id, run_id=cohort_id)

            essay_task_id = f"{draft_task_id}__{essay_tpl}"
            essay_cohort_gen = reserve_gen_id("essay", essay_task_id, run_id=cohort_id)

            # Draft row
            rows.append(
                {
                    "stage": "draft",
                    "gen_id": draft_cohort_gen,
                    "cohort_id": str(cohort_id),
                    "parent_gen_id": "",
                    "combo_id": combo_id,
                    "template_id": draft_tpl,
                    "llm_model_id": model_id,
                }
            )

            # Essay row
            rows.append(
                {
                    "stage": "essay",
                    "gen_id": essay_cohort_gen,
                    "cohort_id": str(cohort_id),
                    "parent_gen_id": draft_cohort_gen,
                    "combo_id": combo_id,
                    "template_id": essay_tpl,
                    # Prefer essay model if present; else inherit draft model
                    "llm_model_id": essay_model_id or model_id,
                }
            )

    else:
        # Cartesian mode — derive from active axes
        # Drafts: content_combinations × active draft templates × generation models
        dtpl_df = read_templates(data_root, "draft", filter_active=True)
        gen_models_df = read_llm_models(data_root)
        gen_models_df = gen_models_df[gen_models_df["for_generation"] == True]
        # Read selected combo ids from data/2_tasks/selected_combo_mappings.csv
        combo_ids: List[str] = []
        try:
            import pandas as _pd
            sel_df = _pd.read_csv(data_root / "2_tasks" / "selected_combo_mappings.csv")
            if not sel_df.empty and "combo_id" in sel_df.columns:
                combo_ids = sel_df["combo_id"].astype(str).dropna().unique().tolist()
        except FileNotFoundError:
            combo_ids = []

        for combo_id in combo_ids:
            for _, trow in dtpl_df.iterrows():
                draft_tpl = str(trow["template_id"])
                for _, mrow in gen_models_df.iterrows():
                    mid = str(mrow["id"])
                    # provider model name omitted
                    draft_task_id = f"{combo_id}__{draft_tpl}__{mid}"
                    draft_cohort_gen = reserve_gen_id("draft", draft_task_id, run_id=cohort_id)
                    rows.append(
                        {
                            "stage": "draft",
                            "gen_id": draft_cohort_gen,
                            "cohort_id": str(cohort_id),
                            "parent_gen_id": "",
                            "combo_id": combo_id,
                            "template_id": draft_tpl,
                            "llm_model_id": mid,
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
                essay_cohort_gen = reserve_gen_id("essay", essay_task_id, run_id=cohort_id)
                rows.append(
                    {
                        "stage": "essay",
                        "gen_id": essay_cohort_gen,
                        "cohort_id": str(cohort_id),
                        "parent_gen_id": draft_cohort_gen,
                        "combo_id": combo_id,
                        "template_id": essay_tpl,
                        # default: essay inherits generation model id
                        "llm_model_id": mid,
                    }
                )

    # After drafts and essays are built, expand evaluations once using shared helper
    eval_tpl_ids, eval_model_ids = _eval_axes(data_root)
    if eval_tpl_ids and eval_model_ids:
        essay_ids = [str(r["gen_id"]) for r in rows if r.get("stage") == "essay"]
        essay_combo_map = {
            str(r["gen_id"]): str(r.get("combo_id") or "") for r in rows if r.get("stage") == "essay"
        }
        for essay_gen_id in essay_ids:
            for tpl in eval_tpl_ids:
                for mid in eval_model_ids:
                    eval_task_id = f"{essay_gen_id}__{tpl}__{mid}"
                    eval_gen_id = reserve_gen_id("evaluation", eval_task_id, run_id=cohort_id)
                    rows.append(
                        {
                            "stage": "evaluation",
                            "gen_id": eval_gen_id,
                            "cohort_id": str(cohort_id),
                            "parent_gen_id": essay_gen_id,
                            "combo_id": essay_combo_map.get(essay_gen_id, ""),
                            "template_id": tpl,
                            "llm_model_id": mid,
                        }
                    )

    # Deduplicate by (stage, gen_id)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["stage", "gen_id"])  # authoritative id set per stage

    # Parent integrity validation
    drafts = set(df[df["stage"] == "draft"]["gen_id"].astype(str).tolist())
    essays = set(df[df["stage"] == "essay"]["gen_id"].astype(str).tolist())
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
                eval_parent_missing.append(pid)
    if essay_parent_missing or eval_parent_missing:
        raise Failure(
            description="Cohort membership parent integrity check failed",
            metadata={
                "function": MetadataValue.text("cohort_membership"),
                "missing_draft_parents_count": MetadataValue.int(len(essay_parent_missing)),
                "missing_essay_parents_count": MetadataValue.int(len(eval_parent_missing)),
                "missing_draft_parents_sample": MetadataValue.json(essay_parent_missing[:5]),
                "missing_essay_parents_sample": MetadataValue.json(eval_parent_missing[:5]),
            },
        )

    # Write membership.csv
    df.to_csv(out_path, index=False)

    # Register dynamic partitions (add-only)
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

    added_draft = _add_only(draft_gens_partitions.name, df[df["stage"] == "draft"]["gen_id"].astype(str))
    added_essay = _add_only(essay_gens_partitions.name, df[df["stage"] == "essay"]["gen_id"].astype(str))
    added_eval = _add_only(
        evaluation_gens_partitions.name, df[df["stage"] == "evaluation"]["gen_id"].astype(str)
    )

    context.add_output_metadata(
        {
            "rows": MetadataValue.int(len(df)),
            "drafts": MetadataValue.int(int((df["stage"] == "draft").sum() if not df.empty else 0)),
            "essays": MetadataValue.int(int((df["stage"] == "essay").sum() if not df.empty else 0)),
            "evaluations": MetadataValue.int(
                int((df["stage"] == "evaluation").sum() if not df.empty else 0)
            ),
            "cohort_id": MetadataValue.text(str(cohort_id)),
            "membership_path": MetadataValue.path(str(out_path)),
            "partitions_added_draft": MetadataValue.int(added_draft),
            "partitions_added_essay": MetadataValue.int(added_essay),
            "partitions_added_evaluation": MetadataValue.int(added_eval),
        }
    )

    return df
@asset(
    group_name="task_definitions",
    io_manager_key="io_manager",
    required_resource_keys={"data_root"},
)
def cohort_id(context, content_combinations: list[ContentCombination]) -> str:
    """Compute a deterministic cohort_id from the current manifest and persist it."""
    data_root = Path(getattr(context.resources, "data_root", "data"))
    # Build manifest from active axes
    try:
        ddf = read_templates(data_root, "draft", filter_active=True)
        if "active" in ddf.columns:
            ddf = ddf[ddf["active"] == True]
        drafts = sorted(ddf["template_id"].astype(str).tolist()) if not ddf.empty else []
    except Exception:
        drafts = []
    try:
        edf = read_templates(data_root, "essay", filter_active=True)
        if "active" in edf.columns:
            edf = edf[edf["active"] == True]
        essays = sorted(edf["template_id"].astype(str).tolist()) if not edf.empty else []
    except Exception:
        essays = []
    try:
        vdf = read_templates(data_root, "evaluation", filter_active=True)
        if "active" in vdf.columns:
            vdf = vdf[vdf["active"] == True]
        evals = sorted(vdf["template_id"].astype(str).tolist()) if not vdf.empty else []
    except Exception:
        evals = []
    try:
        mdf = read_llm_models(data_root)
        gen_models = sorted(mdf[mdf["for_generation"] == True]["id"].astype(str).tolist())
        eval_models = sorted(mdf[mdf["for_evaluation"] == True]["id"].astype(str).tolist())
    except Exception:
        gen_models, eval_models = [], []
    combos = sorted([str(c.combo_id) for c in (content_combinations or [])])
    manifest = {
        "combos": combos,
        "templates": {"draft": drafts, "essay": essays, "evaluation": evals},
        "llms": {"generation": gen_models, "evaluation": eval_models},
    }
    override = None
    try:
        if hasattr(context, "asset_config") and context.asset_config:
            override = context.asset_config.get("override")
        elif hasattr(context, "op_execution_context") and getattr(context.op_execution_context, "op_config", None):
            override = context.op_execution_context.op_config.get("override")
    except Exception:
        override = None
    env_override = get_env_cohort_id()
    cid = compute_cohort_id("cohort", manifest, explicit=(override or env_override))
    write_manifest(str(data_root), cid, manifest)
    context.add_output_metadata({
        "cohort_id": MetadataValue.text(cid),
        "manifest_path": MetadataValue.path(str((data_root / "cohorts" / cid / "manifest.json").resolve())),
    })
    return cid


@asset(
    group_name="cohort",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
)
def selected_combo_mappings(context) -> pd.DataFrame:
    """Regenerate selected combo mappings from active concepts (deterministic ID)."""
    from ..utils.combo_ids import ComboIDManager
    data_root = Path(getattr(context.resources, "data_root", "data"))
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


@asset(
    group_name="cohort",
    io_manager_key="io_manager",
    required_resource_keys={"experiment_config", "data_root"},
)
def content_combinations(context) -> list[ContentCombination]:
    """Build combinations for generation. Preferred source: selected_combo_mappings.csv."""
    data_root = Path(getattr(context.resources, "data_root", "data"))
    try:
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
                combos.append(ContentCombination(contents=[{"name": all_concepts[cid].name, "content": all_concepts[cid].content} for cid in concept_ids if cid in all_concepts], combo_id=str(combo_id), concept_ids=concept_ids))
            if combos:
                return combos
    except Exception:
        pass
    # Fallback: derive from active concepts using description_level/k_max
    cfg = context.resources.experiment_config
    level = getattr(cfg, "description_level", "paragraph")
    k_max = int(getattr(cfg, "k_max", 2))
    concepts = [c for c in read_concepts(data_root, filter_active=True)]
    concepts = concepts[: max(1, min(k_max, len(concepts)))]
    from daydreaming_dagster.models.content_combination import ContentCombination, generate_combo_id
    combo_id = generate_combo_id([c.concept_id for c in concepts], level, k_max)
    return [ContentCombination.from_concepts(concepts, level=level, combo_id=combo_id)]
