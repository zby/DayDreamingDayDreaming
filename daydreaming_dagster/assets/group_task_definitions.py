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
    draft_docs_partitions,
    essay_docs_partitions,
    evaluation_docs_partitions,
)
from ..utils.combo_ids import ComboIDManager
from ..utils.ids import reserve_doc_id
from ..utils.selected_combos import validate_selected_is_subset
from pathlib import Path
import pandas as pd
from typing import List


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
        # No selection file — fall through to active concept fallback
        pass
    except Exception as e:
        # Log and fall back
        context.log.warning(f"content_combinations: selection load failed: {e}; falling back to active concepts")

    # FALLBACK(OPS): derive a single combo from currently active concepts when no curated selection is present.
    # Prefer maintaining curated selected_combo_mappings.csv; keep this narrow, deterministic fallback for dev only.
    cfg = context.resources.experiment_config
    level = getattr(cfg, "description_level", "paragraph")
    k_max = int(getattr(cfg, "k_max", 2))
    concepts = read_concepts(data_root, filter_active=True)
    if not concepts:
        context.add_output_metadata({"count": MetadataValue.int(0), "source": MetadataValue.text("fallback")})
        return []
    selected = concepts[: max(1, min(k_max, len(concepts)))]
    manager = ComboIDManager(str(data_root / "combo_mappings.csv"))
    combo_id = manager.get_or_create_combo_id([c.concept_id for c in selected], level, k_max)
    combo = ContentCombination.from_concepts(selected, level, combo_id=combo_id)
    context.add_output_metadata({"count": MetadataValue.int(1), "source": MetadataValue.text("fallback")})
    return [combo]


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
    deps={DRAFT_TEMPLATES_KEY, LLM_MODELS_KEY},
    required_resource_keys={"data_root"},
)
def draft_generation_tasks(context, content_combinations: List[ContentCombination]) -> pd.DataFrame:
    data_root = Path(context.resources.data_root)
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
        # Stable doc ids per task_id across runs
        df["doc_id"] = df["draft_task_id"].astype(str).apply(
            lambda tid: reserve_doc_id("draft", tid)
        )
    # refresh dynamic partitions
    existing = context.instance.get_dynamic_partitions(draft_docs_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(draft_docs_partitions.name, p)
    if not df.empty:
        context.instance.add_dynamic_partitions(
            draft_docs_partitions.name, df["doc_id"].astype(str).tolist()
        )
    try:
        context.add_output_metadata({"task_count": MetadataValue.int(len(df))})
    except (AttributeError, DagsterInvalidPropertyError):
        # Direct invocation (unit tests) may not support attaching metadata
        pass
    return df

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    deps={ESSAY_TEMPLATES_KEY, LLM_MODELS_KEY},
    required_resource_keys={"data_root"},
)
def essay_generation_tasks(context, content_combinations: List[ContentCombination], draft_generation_tasks: pd.DataFrame) -> pd.DataFrame:
    """Build essay generation tasks strictly from existing draft tasks.

    For each draft_generation_task row and each active essay template, create a
    corresponding essay_generation_task. This ensures the UI materialization
    expands drafts × templates, rather than re-deriving from combinations.
    """
    data_root = Path(context.resources.data_root)
    templates_df = read_essay_templates(data_root)

    if "active" in templates_df.columns:
        templates_df = templates_df[templates_df["active"] == True]

    # Create DataFrame with expected columns (matching script output)
    cols = [
        "essay_task_id",
        "parent_doc_id", 
        "essay_doc_id",
        "draft_task_id",
        "combo_id",
        "draft_template",
        "essay_template",
        "generation_model",
        "generation_model_name",
        "doc_id",
    ]
    df = pd.DataFrame(columns=cols)
    
    # If no drafts or templates, return empty DataFrame
    if draft_generation_tasks is None or draft_generation_tasks.empty or templates_df.empty:
        return df

    # Cross product: each draft task × each active essay template
    for _, drow in draft_generation_tasks.iterrows():
        combo_id = str(drow.get("combo_id"))
        draft_task_id = drow.get("draft_task_id")
        gen_model_id = drow.get("generation_model")
        gen_model_name = drow.get("generation_model_name", gen_model_id)
        draft_doc_id = drow.get("doc_id")
        for _, trow in templates_df.iterrows():
            essay_template_id = trow["template_id"]
            # Compose essay_task_id from the upstream draft_task_id when present
            if isinstance(draft_task_id, str) and draft_task_id:
                essay_task_id = f"{draft_task_id}__{essay_template_id}"
            
            # Reserve essay doc_id
            essay_doc_id = reserve_doc_id("essay", essay_task_id)
            
            # Add row directly to DataFrame
            new_row = pd.DataFrame([{
                "essay_task_id": essay_task_id,
                "parent_doc_id": draft_doc_id,  # Points to draft doc_id
                "essay_doc_id": essay_doc_id,   # Essay's own doc_id
                "draft_task_id": draft_task_id,
                "combo_id": combo_id,
                "draft_template": drow.get("draft_template"),
                "essay_template": essay_template_id,
                "generation_model": gen_model_id,
                "generation_model_name": gen_model_name,
                "doc_id": essay_doc_id,  # Set doc_id to essay_doc_id
            }])
            df = pd.concat([df, new_row], ignore_index=True)

    # refresh dynamic partitions
    existing = context.instance.get_dynamic_partitions(essay_docs_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(essay_docs_partitions.name, p)
    if not df.empty:
        context.instance.add_dynamic_partitions(
            essay_docs_partitions.name, df["doc_id"].astype(str).tolist()
        )
    try:
        context.add_output_metadata({"task_count": MetadataValue.int(len(df))})
    except Exception:
        pass
    return df


@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    deps={EVALUATION_TEMPLATES_KEY, ESSAY_TEMPLATES_KEY, LLM_MODELS_KEY},
    required_resource_keys={"data_root"},
)
def evaluation_tasks(
    context,
    essay_generation_tasks: pd.DataFrame,
    draft_generation_tasks: pd.DataFrame,
) -> pd.DataFrame:
    from ..utils.raw_readers import read_llm_models, read_evaluation_templates

    # use evaluation_docs_partitions for partition registration

    data_root = Path(context.resources.data_root)
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
    # Require essay doc ids (doc-id–first). Accept essay_doc_id or parent_doc_id.
    if essay_generation_tasks.empty:
        return pd.DataFrame(columns=[
            "evaluation_task_id","parent_doc_id","document_id","essay_task_id","combo_id",
            "draft_template","essay_template","generation_model","generation_model_name",
            "evaluation_template","evaluation_model","evaluation_model_name","parser","file_path","source_dir","source_asset",
        ])
    # Accept any of these columns as the essay document id (doc-id–first):
    #  - essay_doc_id (preferred), parent_doc_id (temporary alias), or doc_id (from essay_generation_tasks)
    if "essay_doc_id" in essay_generation_tasks.columns:
        doc_col = "essay_doc_id"
    elif "parent_doc_id" in essay_generation_tasks.columns:
        doc_col = "parent_doc_id"
    elif "doc_id" in essay_generation_tasks.columns:
        doc_col = "doc_id"
    else:
        doc_col = None
    if doc_col is None:
        raise Failure(
            description="evaluation_tasks requires essay doc IDs (doc-id–first)",
            metadata={
                "required_column": MetadataValue.text("essay_doc_id (or parent_doc_id)"),
                "present_columns": MetadataValue.json(list(map(str, essay_generation_tasks.columns))),
                "resolution": MetadataValue.text("Populate essay_doc_id in data/2_tasks/essay_generation_tasks.csv and re-run."),
            },
        )
    missing_mask = essay_generation_tasks[doc_col].astype(str).map(lambda s: (not s.strip()) or s.lower() == "nan")
    if bool(missing_mask.any()):
        sample = essay_generation_tasks.loc[missing_mask, "essay_task_id"].astype(str).head(5).tolist() if "essay_task_id" in essay_generation_tasks.columns else []
        raise Failure(
            description="Missing essay doc_id values in essay_generation_tasks",
            metadata={
                "missing_count": MetadataValue.int(int(missing_mask.sum())),
                "sample_essay_task_ids": MetadataValue.json(sample),
                "resolution": MetadataValue.text("Ensure essays exist and their doc_ids are populated in the input CSV."),
            },
        )
    # Build evaluation rows directly from essays and active evaluation axes
    rows: List[dict] = []
    docs_root = data_root_path / "docs"
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
                    "parent_doc_id": parent_doc_id,
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
        run_id = getattr(getattr(context, "run", object()), "run_id", None) or getattr(context, "run_id", None)
        tasks_df["doc_id"] = tasks_df["evaluation_task_id"].astype(str).apply(
            lambda tid: reserve_doc_id("evaluation", tid, run_id=str(run_id) if run_id else None)
        )
    existing = context.instance.get_dynamic_partitions(evaluation_docs_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(evaluation_docs_partitions.name, p)
    if not tasks_df.empty:
        context.instance.add_dynamic_partitions(
            evaluation_docs_partitions.name, tasks_df["doc_id"].astype(str).tolist()
        )
    context.add_output_metadata({"task_count": MetadataValue.int(len(tasks_df))})
    return tasks_df


@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
)
def document_index(context, essay_generation_tasks: pd.DataFrame, draft_generation_tasks: pd.DataFrame) -> pd.DataFrame:
    """Unified document axis skeleton.

    Provides a normalized view of documents discoverable by the pipeline.
    For now, builds a lightweight index from the task tables if present.
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    rows: list[dict] = []
    # Two-phase essays
    if not essay_generation_tasks.empty:
        essay_dir = data_root / "3_generation" / "essay_responses"
        for _, row in essay_generation_tasks.iterrows():
            essay_task_id = row.get("essay_task_id")
            rows.append(
                {
                    "document_id": essay_task_id,
                    "stage": "essay2p",
                    "origin": "two_phase",
                    "file_path": str(essay_dir / f"{essay_task_id}.txt"),
                    "combo_id": row.get("combo_id"),
                    "draft_template": row.get("draft_template"),
                    "essay_template": row.get("essay_template"),
                    "generation_model_id": row.get("generation_model"),
                    "generation_model_name": row.get("generation_model_name"),
                    "draft_task_id": row.get("draft_task_id"),
                    "essay_task_id": essay_task_id,
                    "source_asset": "essay_response",
                    "source_dir": "essay_responses",
                }
            )
    # OPS-ONLY: include drafts for ops browsing; evaluations do not consume drafts directly.
    if not draft_generation_tasks.empty:
        draft_dir = data_root / "3_generation" / "draft_responses"
        for _, row in draft_generation_tasks.iterrows():
            draft_task_id = row.get("draft_task_id")
            rows.append(
                {
                    "document_id": draft_task_id,
                    "stage": "essay1p",
                    "origin": "draft",
                    "file_path": str(draft_dir / f"{draft_task_id}.txt"),
                    "combo_id": row.get("combo_id"),
                    "draft_template": row.get("draft_template"),
                    "essay_template": row.get("draft_template"),
                    "generation_model_id": row.get("generation_model"),
                    "generation_model_name": row.get("generation_model_name"),
                    "draft_task_id": draft_task_id,
                    "essay_task_id": None,
                    "source_asset": "draft_response",
                    "source_dir": "draft_responses",
                }
            )
    df = pd.DataFrame(rows)
    context.add_output_metadata({"rows": MetadataValue.int(len(df))})
    return df
