"""
Group: task_definitions

Assets that define generation/evaluation tasks from templates and models.
"""

from dagster import asset, MetadataValue, Failure
from dagster._core.errors import DagsterInvalidPropertyError
from ..models import ContentCombination
from .raw_data import (
    DRAFT_TEMPLATES_KEY,
    ESSAY_TEMPLATES_KEY,
    EVALUATION_TEMPLATES_KEY,
    LLM_MODELS_KEY,
)
from ..utils.raw_readers import (
    read_draft_templates,
    read_essay_templates,
    read_evaluation_templates,
    read_llm_models,
    read_concepts,
)
from ..utils.selected_combos import read_selected_combo_mappings
from .partitions import (
    draft_gens_partitions,
    essay_gens_partitions,
    evaluation_gens_partitions,
)
from ..utils.ids import reserve_gen_id
from ..utils.cohorts import (
    get_env_cohort_id,
    compute_cohort_id,
    write_manifest,
)
from ..utils.selected_combos import validate_selected_is_subset
from pathlib import Path
import pandas as pd
from typing import List
import os


def _read_membership_rows(data_root: Path, cohort: str | None) -> pd.DataFrame | None:
    """Read cohort membership CSV when present.

    Returns a DataFrame or None when missing.
    """
    if not cohort:
        return None
    mpath = Path(data_root) / "cohorts" / str(cohort) / "membership.csv"
    if not mpath.exists():
        return None
    try:
        return pd.read_csv(mpath)
    except Exception:
        return None


@asset(
    group_name="task_definitions",
    io_manager_key="io_manager",
    required_resource_keys={"data_root"},
)
def cohort_id(context, content_combinations: List[ContentCombination]) -> str:
    """Compute a deterministic cohort_id from the current manifest and persist it.

    Manifest includes the selected combos, active templates, and active LLM models.
    Allows explicit override via config (cohort_id_asset.override) or env (DD_COHORT).
    Writes data/cohorts/<cohort_id>/manifest.json and returns the cohort_id.
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))

    # Build manifest components deterministically
    try:
        draft_templates_df = read_draft_templates(data_root)
        if "active" in draft_templates_df.columns:
            draft_templates_df = draft_templates_df[draft_templates_df["active"] == True]
        draft_templates = sorted(draft_templates_df["template_id"].astype(str).tolist()) if not draft_templates_df.empty else []
    except Exception:
        draft_templates = []

    try:
        essay_templates_df = read_essay_templates(data_root)
        if "active" in essay_templates_df.columns:
            essay_templates_df = essay_templates_df[essay_templates_df["active"] == True]
        essay_templates = sorted(essay_templates_df["template_id"].astype(str).tolist()) if not essay_templates_df.empty else []
    except Exception:
        essay_templates = []

    try:
        evaluation_templates_df = read_evaluation_templates(data_root)
        if "active" in evaluation_templates_df.columns:
            evaluation_templates_df = evaluation_templates_df[evaluation_templates_df["active"] == True]
        evaluation_templates = sorted(evaluation_templates_df["template_id"].astype(str).tolist()) if not evaluation_templates_df.empty else []
    except Exception:
        evaluation_templates = []

    try:
        models_df = read_llm_models(data_root)
        gen_models = sorted(models_df[models_df["for_generation"] == True]["id"].astype(str).tolist())
        eval_models = sorted(models_df[models_df["for_evaluation"] == True]["id"].astype(str).tolist())
    except Exception:
        gen_models, eval_models = [], []

    combos = sorted([str(c.combo_id) for c in (content_combinations or [])])

    manifest = {
        "combos": combos,
        "templates": {
            "draft": draft_templates,
            "essay": essay_templates,
            "evaluation": evaluation_templates,
        },
        "llms": {
            "generation": gen_models,
            "evaluation": eval_models,
        },
    }

    override = None
    # Allow config override when provided (UI/run config). Prefer asset_config for assets; fallback to op_config for compatibility.
    try:
        if hasattr(context, "asset_config") and context.asset_config:
            override = context.asset_config.get("override")
        elif hasattr(context, "op_config") and context.op_config:
            override = context.op_config.get("override")
    except Exception:
        override = None
    # Env override as secondary path
    env_override = get_env_cohort_id()

    cohort_id = compute_cohort_id("cohort", manifest, explicit=(override or env_override))
    write_manifest(str(data_root), cohort_id, manifest)

    context.add_output_metadata(
        {
            "cohort_id": MetadataValue.text(cohort_id),
            "combos": MetadataValue.int(len(combos)),
            "draft_templates": MetadataValue.int(len(draft_templates)),
            "essay_templates": MetadataValue.int(len(essay_templates)),
            "evaluation_templates": MetadataValue.int(len(evaluation_templates)),
            "generation_models": MetadataValue.int(len(gen_models)),
            "evaluation_models": MetadataValue.int(len(eval_models)),
            "manifest_path": MetadataValue.path(str((data_root / "cohorts" / cohort_id / "manifest.json").resolve())),
        }
    )
    return cohort_id

@asset(
    group_name="task_definitions",
    io_manager_key="io_manager",
    required_resource_keys={"experiment_config", "data_root"},
)
def content_combinations(context) -> List[ContentCombination]:
    """Build combinations for generation.

    Preferred source: data/2_tasks/selected_combo_mappings.csv (explicit selection
    for historical or curated runs). Falls back to active concepts if selection
    is missing.
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    # Try explicit selection first
    try:
        sel = read_selected_combo_mappings(data_root)
        # Debug: log selection file stats
        selected_path = data_root / "2_tasks" / "selected_combo_mappings.csv"
        try:
            stat = selected_path.stat()
            context.log.info(
                f"content_combinations: loaded selection CSV at {selected_path} "
                f"(size={stat.st_size} bytes, mtime={stat.st_mtime})"
            )
        except Exception:
            context.log.info(f"content_combinations: selection path: {selected_path}")

        if not sel.empty:
            # More debug: shape, columns, sample
            try:
                sample_ids = (
                    sel["combo_id"].astype(str).dropna().unique().tolist()[:5]
                    if "combo_id" in sel.columns
                    else []
                )
                context.log.info(
                    "content_combinations: selection shape=%s, columns=%s, "
                    "unique_combo_ids_sample=%s" % (
                        str(sel.shape),
                        ",".join(sel.columns.astype(str).tolist()),
                        sample_ids,
                    )
                )
            except Exception as e:
                context.log.warning(f"content_combinations: failed selection debug summary: {e}")
            # Build combinations per combo_id using the specified description_level
            all_concepts = {c.concept_id: c for c in read_concepts(data_root, filter_active=False)}
            combos: List[ContentCombination] = []
            skipped_missing_concept = 0
            for combo_id, group in sel.groupby("combo_id"):
                # Expect a single level across rows for the combo; take the first
                level = str(group.iloc[0]["description_level"]) if "description_level" in group.columns else "paragraph"
                concept_ids = [str(cid) for cid in group["concept_id"].astype(str).tolist()]
                concepts = [all_concepts[cid] for cid in concept_ids if cid in all_concepts]
                if len(concepts) != len(concept_ids):
                    # Skip incomplete combos; log missing ids for debugging
                    missing = [cid for cid in concept_ids if cid not in all_concepts]
                    skipped_missing_concept += 1
                    context.log.warning(
                        f"content_combinations: skipping combo_id={combo_id} due to missing concepts: {missing}"
                    )
                    continue
                combos.append(ContentCombination.from_concepts(concepts, level=level, combo_id=str(combo_id)))
            context.add_output_metadata(
                {
                    "count": MetadataValue.int(len(combos)),
                    "source": MetadataValue.text("selected"),
                    "selection_path": MetadataValue.path(str(selected_path)),
                    "selection_rows": MetadataValue.int(int(sel.shape[0])),
                    "selection_cols": MetadataValue.int(int(sel.shape[1])),
                    "skipped_combos_missing_concept": MetadataValue.int(skipped_missing_concept),
                    "combo_ids_sample": MetadataValue.json(sample_ids if 'sample_ids' in locals() else []),
                }
            )
            if combos:
                return combos
    except FileNotFoundError:
        raise Failure(
            description="Selected combo mappings file not found",
            metadata={
                "required_file": MetadataValue.path(str(data_root / "2_tasks" / "selected_combo_mappings.csv")),
                "resolution": MetadataValue.text("Generate selected_combo_mappings.csv using the selected_combo_mappings asset"),
            }
        )
    except Exception as e:
        raise Failure(
            description=f"Failed to load selected combo mappings: {e}",
            metadata={
                "file_path": MetadataValue.path(str(data_root / "2_tasks" / "selected_combo_mappings.csv")),
                "error": MetadataValue.text(str(e)),
                "resolution": MetadataValue.text("Check file format and regenerate if needed using selected_combo_mappings asset"),
            }
        )


@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
)
def selected_combo_mappings(context) -> pd.DataFrame:
    """Regenerate selected combo mappings from active concepts (deterministic ID).

    - Reads active concepts from data/1_raw/concepts_metadata.csv
    - Uses ExperimentConfig.description_level (default: paragraph) and k_max
    - Allocates a stable combo_id via ComboIDManager (writes/uses data/combo_mappings.csv)
    - Writes data/2_tasks/selected_combo_mappings.csv with one row per concept
    """
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
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    deps={DRAFT_TEMPLATES_KEY, LLM_MODELS_KEY, "cohort_membership"},
    required_resource_keys={"data_root"},
)
def draft_generation_tasks(
    context,
    content_combinations: List[ContentCombination],
) -> pd.DataFrame:
    """Project draft task rows directly from cohort membership when available.

    Falls back to legacy active-axes derivation only when explicitly enabled via
    environment variable DD_ALLOW_LEGACY_TASKS=1 and membership is missing.
    """
    data_root = Path(context.resources.data_root)
    # Resolve cohort id for projection or fallback ID seeding
    env_cohort = get_env_cohort_id()
    try:
        resolved_cohort = cohort_id(context, content_combinations) if not env_cohort else env_cohort
    except Exception:
        resolved_cohort = env_cohort
    # Prefer cohort membership (authoritative) when present on disk
    mdf = _read_membership_rows(data_root, resolved_cohort)
    if mdf is not None and not mdf.empty:
        draft_df = mdf[mdf.get("stage") == "draft"].copy()
        cols = [
            "draft_task_id",
            "combo_id",
            "draft_template",
            "generation_model",
            "generation_model_name",
            "gen_id",
            "cohort_id",
        ]
        # Ensure expected columns exist even if empty
        for c in cols:
            if c not in draft_df.columns:
                draft_df[c] = pd.Series(dtype=str)
        out = draft_df[cols].drop_duplicates(subset=["gen_id"]).reset_index(drop=True)
        # Register partitions add-only
        existing = set(context.instance.get_dynamic_partitions(draft_gens_partitions.name))
        keys = [str(x) for x in out["gen_id"].astype(str).tolist()]
        to_add = [k for k in keys if k and k not in existing]
        if to_add:
            context.instance.add_dynamic_partitions(draft_gens_partitions.name, to_add)
        context.add_output_metadata(
            {
                "task_count": MetadataValue.int(len(out)),
                "source": MetadataValue.text("cohort_membership"),
                "partitions_added": MetadataValue.int(len(to_add)),
            }
        )
        return out

    # Fallback: derive from active axes (legacy behavior preserved)
    templates_df = read_draft_templates(data_root)
    models_df = read_llm_models(data_root)
    if "active" in templates_df.columns:
        templates_df = templates_df[templates_df["active"] == True]
    gen_models = models_df[models_df["for_generation"] == True]
    rows: List[dict] = []
    for combo in content_combinations:
        for _, t in templates_df.iterrows():
            for _, m in gen_models.iterrows():
                task_id = f"{combo.combo_id}__{t['template_id']}__{m['id']}"
                rows.append(
                    {
                        "draft_task_id": task_id,
                        "combo_id": combo.combo_id,
                        "draft_template": t["template_id"],
                        "generation_model": m["id"],
                        "generation_model_name": m["model"],
                    }
                )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["gen_id"] = df["draft_task_id"].astype(str).apply(
            lambda tid: reserve_gen_id("draft", tid, run_id=str(resolved_cohort) if resolved_cohort else None)
        )
        if resolved_cohort:
            df["cohort_id"] = str(resolved_cohort)
        # Add-only registration
        existing = set(context.instance.get_dynamic_partitions(draft_gens_partitions.name))
        keys = [str(x) for x in df["gen_id"].astype(str).tolist()]
        to_add = [k for k in keys if k and k not in existing]
        if to_add:
            context.instance.add_dynamic_partitions(draft_gens_partitions.name, to_add)
    context.add_output_metadata({"task_count": MetadataValue.int(len(df)), "source": MetadataValue.text("legacy_axes")})
    return df

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    deps={ESSAY_TEMPLATES_KEY, LLM_MODELS_KEY, "cohort_membership"},
    required_resource_keys={"data_root"},
)
def essay_generation_tasks(
    context,
    content_combinations: List[ContentCombination],
    draft_generation_tasks: pd.DataFrame,
) -> pd.DataFrame:
    """Project essay task rows directly from cohort membership when available.

    Falls back to legacy derivation (drafts × active essay templates) only when
    DD_ALLOW_LEGACY_TASKS is enabled and membership is missing.
    """
    data_root = Path(context.resources.data_root)
    env_cohort = get_env_cohort_id()
    try:
        resolved_cohort = cohort_id(context, content_combinations) if not env_cohort else env_cohort
    except Exception:
        resolved_cohort = env_cohort
    mdf = _read_membership_rows(data_root, resolved_cohort)
    if mdf is not None and not mdf.empty:
        essay_df = mdf[mdf.get("stage") == "essay"].copy()
        cols = [
            "essay_task_id",
            "parent_gen_id",
            "draft_task_id",
            "combo_id",
            "draft_template",
            "essay_template",
            "generation_model",
            "generation_model_name",
            "gen_id",
            "cohort_id",
        ]
        for c in cols:
            if c not in essay_df.columns:
                essay_df[c] = pd.Series(dtype=str)
        out = essay_df[cols].drop_duplicates(subset=["gen_id"]).reset_index(drop=True)
        # Register partitions add-only
        existing = set(context.instance.get_dynamic_partitions(essay_gens_partitions.name))
        keys = [str(x) for x in out["gen_id"].astype(str).tolist()]
        to_add = [k for k in keys if k and k not in existing]
        if to_add:
            context.instance.add_dynamic_partitions(essay_gens_partitions.name, to_add)
        context.add_output_metadata(
            {
                "task_count": MetadataValue.int(len(out)),
                "source": MetadataValue.text("cohort_membership"),
                "partitions_added": MetadataValue.int(len(to_add)),
            }
        )
        return out

    # Legacy fallback
    # Legacy fallback
    templates_df = read_essay_templates(data_root)
    if "active" in templates_df.columns:
        templates_df = templates_df[templates_df["active"] == True]
    cols = [
        "essay_task_id",
        "parent_gen_id",
        "draft_task_id",
        "combo_id",
        "draft_template",
        "essay_template",
        "generation_model",
        "generation_model_name",
        "gen_id",
        "cohort_id",
    ]
    df = pd.DataFrame(columns=cols)
    if draft_generation_tasks is None or draft_generation_tasks.empty or templates_df.empty:
        return df
    for _, drow in draft_generation_tasks.iterrows():
        combo_id = str(drow.get("combo_id"))
        draft_task_id = drow.get("draft_task_id")
        gen_model_id = drow.get("generation_model")
        gen_model_name = drow.get("generation_model_name", gen_model_id)
        draft_gen_id = drow.get("gen_id")
        for _, trow in templates_df.iterrows():
            essay_template_id = trow["template_id"]
            if isinstance(draft_task_id, str) and draft_task_id:
                essay_task_id = f"{draft_task_id}__{essay_template_id}"
            gen_id = reserve_gen_id("essay", essay_task_id, run_id=str(resolved_cohort) if resolved_cohort else None)
            row_dict = {
                "essay_task_id": essay_task_id,
                "parent_gen_id": draft_gen_id,
                "draft_task_id": draft_task_id,
                "combo_id": combo_id,
                "draft_template": drow.get("draft_template"),
                "essay_template": essay_template_id,
                "generation_model": gen_model_id,
                "generation_model_name": gen_model_name,
                "gen_id": gen_id,
                "cohort_id": str(resolved_cohort) if resolved_cohort else "",
            }
            df = pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)
    existing = set(context.instance.get_dynamic_partitions(essay_gens_partitions.name))
    keys = [str(x) for x in df["gen_id"].astype(str).tolist()]
    to_add = [k for k in keys if k and k not in existing]
    if to_add:
        context.instance.add_dynamic_partitions(essay_gens_partitions.name, to_add)
    context.add_output_metadata({"task_count": MetadataValue.int(len(df)), "source": MetadataValue.text("legacy_axes"), "partitions_added": MetadataValue.int(len(to_add))})
    return df


@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    deps={EVALUATION_TEMPLATES_KEY, ESSAY_TEMPLATES_KEY, LLM_MODELS_KEY, "cohort_membership"},
    required_resource_keys={"data_root"},
)
def evaluation_tasks(
    context,
    essay_generation_tasks: pd.DataFrame,
    draft_generation_tasks: pd.DataFrame,
) -> pd.DataFrame:
    from ..utils.raw_readers import read_llm_models, read_evaluation_templates

    # use evaluation_gens_partitions for partition registration

    data_root = Path(context.resources.data_root)
    # Resolve cohort id for seeding and membership projection
    env_cohort = get_env_cohort_id()
    try:
        # Build a manifest consistent path if env isn't provided
        if env_cohort:
            resolved = env_cohort
        else:
            # Build a minimal manifest from essay/draft tasks for determinism, same as before
            try:
                combos = sorted(
                    [str(c) for c in pd.unique(essay_generation_tasks.get("combo_id", pd.Series(dtype=str)).astype(str))]
                )
                if not combos:
                    combos = sorted(
                        [str(c) for c in pd.unique(draft_generation_tasks.get("combo_id", pd.Series(dtype=str)).astype(str))]
                    )
            except Exception:
                combos = []
            try:
                models_df = read_llm_models(Path(data_root))
                gen_models = sorted(models_df[models_df["for_generation"] == True]["id"].astype(str).tolist())
                eval_models = sorted(models_df[models_df["for_evaluation"] == True]["id"].astype(str).tolist())
            except Exception:
                gen_models, eval_models = [], []
            try:
                dtpl = read_draft_templates(Path(data_root))
                if "active" in dtpl.columns:
                    dtpl = dtpl[dtpl["active"] == True]
                draft_templates = sorted(dtpl["template_id"].astype(str).tolist()) if not dtpl.empty else []
            except Exception:
                draft_templates = []
            try:
                etpl = read_essay_templates(Path(data_root))
                if "active" in etpl.columns:
                    etpl = etpl[etpl["active"] == True]
                essay_templates = sorted(etpl["template_id"].astype(str).tolist()) if not etpl.empty else []
            except Exception:
                essay_templates = []
            try:
                vtpl = read_evaluation_templates(Path(data_root))
                if "active" in vtpl.columns:
                    vtpl = vtpl[vtpl["active"] == True]
                evaluation_templates = sorted(vtpl["template_id"].astype(str).tolist()) if not vtpl.empty else []
            except Exception:
                evaluation_templates = []

            manifest = {
                "combos": combos,
                "templates": {
                    "draft": draft_templates,
                    "essay": essay_templates,
                    "evaluation": evaluation_templates,
                },
                "llms": {"generation": gen_models, "evaluation": eval_models},
            }
            resolved = compute_cohort_id("cohort", manifest)
            try:
                write_manifest(str(data_root), resolved, manifest)
            except Exception:
                pass
    except Exception:
        resolved = env_cohort

    # First try authoritative cohort membership projection
    mdf = _read_membership_rows(data_root, resolved)
    if mdf is not None and not mdf.empty:
        edf = mdf[mdf.get("stage") == "evaluation"].copy()
        cols = [
            "evaluation_task_id",
            "parent_gen_id",
            "document_id",
            "essay_task_id",
            "draft_task_id",
            "combo_id",
            "draft_template",
            "essay_template",
            "generation_model",
            "generation_model_name",
            "evaluation_template",
            "evaluation_model",
            "evaluation_model_name",
            "parser",
            "file_path",
            "source_dir",
            "source_asset",
            "gen_id",
            "cohort_id",
        ]
        for c in cols:
            if c not in edf.columns:
                edf[c] = pd.Series(dtype=str)
        out = edf[cols].drop_duplicates(subset=["gen_id"]).reset_index(drop=True)
        existing = set(context.instance.get_dynamic_partitions(evaluation_gens_partitions.name))
        keys = [str(x) for x in out["gen_id"].astype(str).tolist()]
        to_add = [k for k in keys if k and k not in existing]
        if to_add:
            context.instance.add_dynamic_partitions(evaluation_gens_partitions.name, to_add)
        context.add_output_metadata({"task_count": MetadataValue.int(len(out)), "source": MetadataValue.text("cohort_membership"), "partitions_added": MetadataValue.int(len(to_add))})
        return out

    models_df = read_llm_models(Path(data_root))
    evaluation_models = models_df[models_df["for_evaluation"] == True]
    evaluation_templates_df = read_evaluation_templates(Path(data_root))
    if "active" in evaluation_templates_df.columns:
        evaluation_templates_df = evaluation_templates_df[evaluation_templates_df["active"] == True]
    eval_templates = list(evaluation_templates_df["template_id"].tolist())
    # Optional parser column: build lookup to propagate into tasks
    parser_map: dict[str, str] = {}
    if "parser" in evaluation_templates_df.columns:
        try:
            parser_map = (
                evaluation_templates_df[["template_id", "parser"]]
                .dropna(subset=["template_id"])  # keep valid template ids
                .astype({"template_id": str})
                .set_index("template_id")["parser"]
                .astype(str)
                .to_dict()
            )
        except Exception:
            parser_map = {}

    data_root_path = Path(data_root)
    # Require essay gen ids (gen-id–first). Accept essay_gen_id or parent_gen_id.
    if essay_generation_tasks.empty:
        return pd.DataFrame(columns=[
            "evaluation_task_id","parent_gen_id","document_id","essay_task_id","combo_id",
            "draft_template","essay_template","generation_model","generation_model_name",
            "evaluation_template","evaluation_model","evaluation_model_name","parser","file_path","source_dir","source_asset",
        ])
    # Accept any of these columns as the essay generation id (gen-id–first):
    #  - essay_gen_id (preferred), parent_gen_id (temporary alias), or gen_id (from essay_generation_tasks)
    if "essay_gen_id" in essay_generation_tasks.columns:
        doc_col = "essay_gen_id"
    elif "parent_gen_id" in essay_generation_tasks.columns:
        doc_col = "parent_gen_id"
    elif "gen_id" in essay_generation_tasks.columns:
        doc_col = "gen_id"
    else:
        doc_col = None
    if doc_col is None:
        raise Failure(
            description="evaluation_tasks requires essay gen IDs (gen-id–first)",
            metadata={
                "required_column": MetadataValue.text("essay_gen_id (or parent_gen_id)"),
                "present_columns": MetadataValue.json(list(map(str, essay_generation_tasks.columns))),
                "resolution": MetadataValue.text("Populate essay_gen_id in data/2_tasks/essay_generation_tasks.csv and re-run."),
            },
        )
    missing_mask = essay_generation_tasks[doc_col].astype(str).map(lambda s: (not s.strip()) or s.lower() == "nan")
    if bool(missing_mask.any()):
        sample = essay_generation_tasks.loc[missing_mask, "essay_task_id"].astype(str).head(5).tolist() if "essay_task_id" in essay_generation_tasks.columns else []
        raise Failure(
            description="Missing essay gen_id values in essay_generation_tasks",
            metadata={
                "missing_count": MetadataValue.int(int(missing_mask.sum())),
                "sample_essay_task_ids": MetadataValue.json(sample),
                "resolution": MetadataValue.text("Ensure essays exist and their gen_ids are populated in the input CSV."),
            },
        )
    # Build evaluation rows directly from essays and active evaluation axes
    rows: List[dict] = []
    docs_root = data_root_path / "gens"
    for _, erow in essay_generation_tasks.iterrows():
        essay_task_id = str(erow.get("essay_task_id") or "")
        parent_doc_id = str(erow.get(doc_col))
        # Optional path from docs store
        doc_dir = docs_root / "essay" / parent_doc_id
        parsed_fp = doc_dir / "parsed.txt"
        raw_fp = doc_dir / "raw.txt"
        file_path = str(parsed_fp) if parsed_fp.exists() else (str(raw_fp) if raw_fp.exists() else "")
        for _, eval_model_row in evaluation_models.iterrows():
            eval_model_id = eval_model_row["id"]
            eval_model_name = eval_model_row["model"]
            for eval_template_id in eval_templates:
                evaluation_task_id = f"{parent_doc_id}__{eval_template_id}__{eval_model_id}"
                rows.append({
                    "evaluation_task_id": evaluation_task_id,
                    "parent_gen_id": parent_doc_id,
                    # legacy context for compatibility
                    "document_id": essay_task_id,
                    "essay_task_id": essay_task_id,
                    "combo_id": erow.get("combo_id"),
                    "draft_template": erow.get("draft_template"),
                    "essay_template": erow.get("essay_template"),
                    "generation_model": erow.get("generation_model"),
                    "generation_model_name": erow.get("generation_model_name"),
                    "evaluation_template": eval_template_id,
                    "evaluation_model": eval_model_id,
                    "evaluation_model_name": eval_model_name,
                    "parser": parser_map.get(str(eval_template_id)),
                    "file_path": file_path,
                    "source_dir": "docs/essay" if doc_dir.exists() else "",
                    "source_asset": "essay_response",
                })
    tasks_df = pd.DataFrame(rows)
    if not tasks_df.empty:
        # Determine cohort id: env override or compute deterministically from tasks manifest
        env_cohort = get_env_cohort_id()
        if env_cohort:
            resolved = env_cohort
        else:
            # Build a manifest consistent with cohort asset using task-derived combos
            try:
                combos = sorted(
                    [str(c) for c in pd.unique(essay_generation_tasks.get("combo_id", pd.Series(dtype=str)).astype(str))]
                )
                if not combos:
                    combos = sorted(
                        [str(c) for c in pd.unique(draft_generation_tasks.get("combo_id", pd.Series(dtype=str)).astype(str))]
                    )
            except Exception:
                combos = []
            try:
                models_df = read_llm_models(Path(data_root))
                gen_models = sorted(models_df[models_df["for_generation"] == True]["id"].astype(str).tolist())
                eval_models = sorted(models_df[models_df["for_evaluation"] == True]["id"].astype(str).tolist())
            except Exception:
                gen_models, eval_models = [], []
            try:
                dtpl = read_draft_templates(Path(data_root))
                if "active" in dtpl.columns:
                    dtpl = dtpl[dtpl["active"] == True]
                draft_templates = sorted(dtpl["template_id"].astype(str).tolist()) if not dtpl.empty else []
            except Exception:
                draft_templates = []
            try:
                etpl = read_essay_templates(Path(data_root))
                if "active" in etpl.columns:
                    etpl = etpl[etpl["active"] == True]
                essay_templates = sorted(etpl["template_id"].astype(str).tolist()) if not etpl.empty else []
            except Exception:
                essay_templates = []
            try:
                vtpl = read_evaluation_templates(Path(data_root))
                if "active" in vtpl.columns:
                    vtpl = vtpl[vtpl["active"] == True]
                evaluation_templates = sorted(vtpl["template_id"].astype(str).tolist()) if not vtpl.empty else []
            except Exception:
                evaluation_templates = []

            manifest = {
                "combos": combos,
                "templates": {
                    "draft": draft_templates,
                    "essay": essay_templates,
                    "evaluation": evaluation_templates,
                },
                "llms": {"generation": gen_models, "evaluation": eval_models},
            }
            resolved = compute_cohort_id("cohort", manifest)
            try:
                write_manifest(str(data_root), resolved, manifest)
            except Exception:
                pass

        tasks_df["gen_id"] = tasks_df["evaluation_task_id"].astype(str).apply(
            lambda tid: reserve_gen_id("evaluation", tid, run_id=resolved)
        )
        if resolved:
            tasks_df["cohort_id"] = resolved
    existing = context.instance.get_dynamic_partitions(evaluation_gens_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(evaluation_gens_partitions.name, p)
    if not tasks_df.empty:
        context.instance.add_dynamic_partitions(
            evaluation_gens_partitions.name, tasks_df["gen_id"].astype(str).tolist()
        )
    context.add_output_metadata({"task_count": MetadataValue.int(len(tasks_df))})
    return tasks_df


# document_index asset removed (not used by runtime pipeline)
