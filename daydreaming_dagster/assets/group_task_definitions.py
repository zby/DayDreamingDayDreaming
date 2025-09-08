"""
Group: task_definitions

Assets that define generation/evaluation tasks from templates and models.
"""

from dagster import asset, MetadataValue
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
    draft_tasks_partitions,
    essay_tasks_partitions,
    evaluation_tasks_partitions,
)
from ..utils.combo_ids import ComboIDManager
from ..utils.ids import reserve_doc_id
from ..utils.selected_combos import validate_selected_is_subset
from ..utils.document_locator import find_document_path
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
)
def selected_combo_mappings(context) -> pd.DataFrame:
    """Write curated selected combo mappings CSV.

    - If `data/combo_mappings.csv` exists, copy all rows to `data/2_tasks/selected_combo_mappings.csv`.
    - Otherwise, write a minimal non-empty CSV with required columns.
    Returns the output path as a string.
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    superset = data_root / "combo_mappings.csv"
    out = data_root / "2_tasks" / "selected_combo_mappings.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        import pandas as pd
        if superset.exists():
            df = pd.read_csv(superset)
        else:
            df = pd.DataFrame([
                {"combo_id": "example", "concept_id": "concept-x", "description_level": "paragraph", "k_max": 2}
            ])
        context.add_output_metadata({"path": MetadataValue.path(str(out))})
        return df
    except Exception as e:
        import pandas as pd
        context.log.info(f"Failed to derive from superset: {e}; writing placeholder selection")
        return pd.DataFrame([
            {"combo_id": "example", "concept_id": "concept-x", "description_level": "paragraph", "k_max": 2}
        ])


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
        run_id = getattr(getattr(context, "run", object()), "run_id", None) or getattr(context, "run_id", None)
        df["doc_id"] = df["draft_task_id"].astype(str).apply(lambda tid: reserve_doc_id("draft", tid, run_id=str(run_id) if run_id else None))
    # refresh dynamic partitions
    existing = context.instance.get_dynamic_partitions(draft_tasks_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(draft_tasks_partitions.name, p)
    if not df.empty:
        context.instance.add_dynamic_partitions(
            draft_tasks_partitions.name, df["draft_task_id"].tolist()
        )
    context.add_output_metadata({"task_count": MetadataValue.int(len(df))})
    return df


@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    deps={ESSAY_TEMPLATES_KEY, LLM_MODELS_KEY},
    required_resource_keys={"data_root"},
)
def essay_generation_tasks(context, content_combinations: List[ContentCombination], draft_generation_tasks: pd.DataFrame) -> pd.DataFrame:
    data_root = Path(context.resources.data_root)
    templates_df = read_essay_templates(data_root)
    models_df = read_llm_models(data_root)

    if "active" in templates_df.columns:
        templates_df = templates_df[templates_df["active"] == True]
    gen_models = models_df[models_df["for_generation"] == True]

    rows: List[dict] = []
    # For two-phase pairing, attempt to link draft task for same combo
    draft_index = None
    if not draft_generation_tasks.empty:
        draft_index = draft_generation_tasks.set_index(["combo_id", "draft_template", "generation_model"])  # type: ignore

    for combo in content_combinations:
        for _, t in templates_df.iterrows():
            for _, m in gen_models.iterrows():
                draft_template = t.get("from_draft_template") or t.get("draft_template")
                draft_task_id = None
                if draft_template and draft_index is not None:
                    key = (combo.combo_id, draft_template, m["id"])
                    if key in draft_index.index:
                        draft_task_id = draft_index.loc[key]["draft_task_id"]  # type: ignore
                # Construct essay task id: prefer to derive from draft_task_id when available
                if draft_task_id:
                    essay_task_id = f"{draft_task_id}__{t['template_id']}"
                else:
                    essay_task_id = f"{combo.combo_id}__{t['template_id']}__{m['id']}"
                rows.append(
                    {
                        "essay_task_id": essay_task_id,
                        "combo_id": combo.combo_id,
                        "draft_template": draft_template,
                        "essay_template": t["template_id"],
                        "generation_model": m["id"],
                        "generation_model_name": m["model"],
                        "draft_task_id": draft_task_id,
                    }
                )
    df = pd.DataFrame(rows)
    if not df.empty:
        run_id = getattr(getattr(context, "run", object()), "run_id", None) or getattr(context, "run_id", None)
        df["doc_id"] = df["essay_task_id"].astype(str).apply(lambda tid: reserve_doc_id("essay", tid, run_id=str(run_id) if run_id else None))
    # refresh dynamic partitions
    existing = context.instance.get_dynamic_partitions(essay_tasks_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(essay_tasks_partitions.name, p)
    if not df.empty:
        context.instance.add_dynamic_partitions(
            essay_tasks_partitions.name, df["essay_task_id"].tolist()
        )
    context.add_output_metadata({"task_count": MetadataValue.int(len(df))})
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

    from .partitions import evaluation_tasks_partitions

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
    docs: List[dict] = []
    essay_dir = data_root_path / "3_generation" / "essay_responses"
    if not essay_generation_tasks.empty:
        for _, row in essay_generation_tasks.iterrows():
            essay_task_id = row["essay_task_id"]
            fp, src = find_document_path(essay_task_id, data_root_path)
            docs.append(
                {
                    "document_id": essay_task_id,
                    "stage": "essay2p",
                    "origin": "two_phase",
                    "file_path": str(fp) if fp else str(essay_dir / f"{essay_task_id}.txt"),
                    "combo_id": row["combo_id"],
                    "draft_template": row.get("draft_template"),
                    "essay_template": row["essay_template"],
                    "generation_model_id": row["generation_model"],
                    "generation_model_name": row["generation_model_name"],
                    "draft_task_id": row.get("draft_task_id"),
                    "essay_task_id": essay_task_id,
                    "source_asset": "essay_response",
                    "source_dir": src or "essay_responses",
                }
            )
    # Evaluations target essays only. Drafts are not included here; if you need
    # to evaluate a draft-like artifact, materialize an essay with generator=copy
    # and use the essay doc id.
    document_index = pd.DataFrame(docs)
    rows: List[dict] = []
    for _, doc in document_index.iterrows():
        document_id = doc["document_id"]
        for _, eval_model_row in evaluation_models.iterrows():
            eval_model_id = eval_model_row["id"]
            eval_model_name = eval_model_row["model"]
            for eval_template_id in eval_templates:
                evaluation_task_id = f"{document_id}__{eval_template_id}__{eval_model_id}"
                rows.append(
                    {
                        "evaluation_task_id": evaluation_task_id,
                        "document_id": document_id,
                        "stage": doc.get("stage"),
                        "origin": doc.get("origin"),
                        "file_path": doc.get("file_path"),
                        "combo_id": doc.get("combo_id"),
                        "draft_template": doc.get("draft_template"),
                        "essay_template": doc.get("essay_template"),
                        "generation_model": doc.get("generation_model_id"),
                        "generation_model_name": doc.get("generation_model_name"),
                        "draft_task_id": doc.get("draft_task_id"),
                        "essay_task_id": doc.get("essay_task_id"),
                        "source_asset": doc.get("source_asset"),
                        "source_dir": doc.get("source_dir"),
                        "evaluation_template": eval_template_id,
                        "evaluation_model": eval_model_id,
                        "evaluation_model_name": eval_model_name,
                        "parser": parser_map.get(str(eval_template_id)),
                    }
                )
    tasks_df = pd.DataFrame(rows)
    if not tasks_df.empty:
        run_id = getattr(getattr(context, "run", object()), "run_id", None) or getattr(context, "run_id", None)
        tasks_df["doc_id"] = tasks_df["evaluation_task_id"].astype(str).apply(lambda tid: reserve_doc_id("evaluation", tid, run_id=str(run_id) if run_id else None))
        # For doc-id-first migration, parent_doc_id represents the essay document id
        # Here, document_id is the essay_task_id; we expose it as parent_doc_id for convenience
        if "document_id" in tasks_df.columns:
            tasks_df["parent_doc_id"] = tasks_df["document_id"].astype(str)
    existing = context.instance.get_dynamic_partitions(evaluation_tasks_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(evaluation_tasks_partitions.name, p)
    if not tasks_df.empty:
        context.instance.add_dynamic_partitions(
            evaluation_tasks_partitions.name, tasks_df["evaluation_task_id"].tolist()
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
