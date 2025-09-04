from dagster import asset, MetadataValue, AutomationCondition, Failure, AssetKey
from typing import Dict, Tuple, List
from pathlib import Path
import pandas as pd
from itertools import combinations
from ..models import ContentCombination
import pandas as pd
from .raw_data import (
    CONCEPTS_METADATA_KEY,
    LLM_MODELS_KEY,
    LINK_TEMPLATES_KEY,
    DRAFT_TEMPLATES_KEY,
    ESSAY_TEMPLATES_KEY,
    EVALUATION_TEMPLATES_KEY,
)
from ..utils.raw_readers import (
    read_concepts,
    read_llm_models,
    read_draft_templates,
    read_essay_templates,
    read_evaluation_templates,
)
from .partitions import (
    generation_tasks_partitions,
    evaluation_tasks_partitions,
    draft_tasks_partitions,
    essay_tasks_partitions,
)
from ..utils.combo_ids import ComboIDManager
from ..utils.selected_combos import read_selected_combo_mappings, validate_selected_is_subset
from pathlib import Path as _Path
from ..utils.document_locator import find_document_path
from typing import List as _List

@asset(
    group_name="task_definitions",
    required_resource_keys={"data_root"},
    automation_condition=AutomationCondition.eager(),
    # Soft dependency on the selected CSV; file must exist
    deps={AssetKey(["task_sources", "selected_combo_mappings_csv"])},
)
def content_combinations(
    context,
) -> List[ContentCombination]:
    """Build ContentCombination objects from a single source: selected CSV.

    Contract: data/2_tasks/selected_combo_mappings.csv must exist and be a strict
    row-subset of data/combo_mappings.csv (identical schema). No k-max fallback.
    """
    data_root = Path(context.resources.data_root)
    # Load selected mapping (strict subset of superset). No fallback.
    try:
        selected_df = read_selected_combo_mappings(data_root)
    except FileNotFoundError as e:
        raise Failure(
            description="Selected combo mappings file not found",
            metadata={
                "error": MetadataValue.text(str(e)),
                "selected_path": MetadataValue.path(data_root / "2_tasks" / "selected_combo_mappings.csv"),
                "resolution": MetadataValue.text(
                    "Create selected_combo_mappings.csv (subset of data/combo_mappings.csv) via curated scripts or the optional asset."
                ),
            },
        )
    superset_path = data_root / "combo_mappings.csv"
    if superset_path.exists():
        try:
            validate_selected_is_subset(selected_df, superset_path)
        except Exception as e:
            raise Failure(
                description="Selected combo mappings are invalid (not a strict subset of data/combo_mappings.csv)",
                metadata={
                    "error": MetadataValue.text(str(e)),
                    "selected_path": MetadataValue.path(data_root / "2_tasks" / "selected_combo_mappings.csv"),
                    "superset_path": MetadataValue.path(superset_path),
                    "resolution": MetadataValue.text(
                        "Ensure selected file is created by filtering rows from data/combo_mappings.csv"
                    ),
                },
            )

    if selected_df.empty:
        raise Failure(
            description="No selected combinations found (selected_combo_mappings.csv is empty)",
            metadata={
                "selected_path": MetadataValue.path(data_root / "2_tasks" / "selected_combo_mappings.csv"),
                "resolution": MetadataValue.text(
                    "Populate selected_combo_mappings.csv with a subset of rows from data/combo_mappings.csv"
                ),
            },
        )

    # Build lookup for concepts (allow inactive concepts)
    all_concepts = read_concepts(data_root, filter_active=False)
    by_id = {c.concept_id: c for c in all_concepts}

    # Convert selected rows to ContentCombination objects
    required = {"combo_id", "concept_id", "description_level"}
    if not required.issubset(set(selected_df.columns)):
        missing = sorted(required - set(selected_df.columns))
        raise Failure(
            description="selected_combo_mappings.csv missing required columns",
            metadata={
                "missing": MetadataValue.text(", ".join(missing)),
                "required": MetadataValue.text(", ".join(sorted(required))),
            },
        )

    combos: List[ContentCombination] = []
    for combo_id, grp in selected_df.groupby("combo_id"):
        # Enforce consistent description_level per combo
        levels = [str(x) for x in grp["description_level"].dropna().astype(str).unique().tolist()]
        desc_level = (
            levels[0]
            if levels
            else str(getattr(getattr(context.resources, "experiment_config", None), "description_level", "paragraph"))
        )
        # Deterministic order: sort concept IDs
        concept_ids = sorted(grp["concept_id"].astype(str).unique().tolist())
        missing = [cid for cid in concept_ids if cid not in by_id]
        if missing:
            raise Failure(
                description=f"Missing concept(s) for combo {combo_id}",
                metadata={
                    "combo_id": MetadataValue.text(str(combo_id)),
                    "missing_concepts": MetadataValue.text(
                        ", ".join(missing[:10]) + ("..." if len(missing) > 10 else "")
                    ),
                    "resolution": MetadataValue.text(
                        "Ensure all concept_ids exist in data/1_raw/concepts_metadata.csv"
                    ),
                },
            )
        ordered_concepts = [by_id[cid] for cid in concept_ids]
        combos.append(
            ContentCombination.from_concepts(ordered_concepts, desc_level, combo_id=str(combo_id))
        )

    context.add_output_metadata(
        {
            "combination_count": MetadataValue.int(len(combos)),
            "stable_id_format": MetadataValue.text("combo_v1_<12-hex>"),
            "selection_source": MetadataValue.path(data_root / "2_tasks" / "selected_combo_mappings.csv"),
        }
    )
    return combos

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
    automation_condition=AutomationCondition.eager(),
)
def selected_combo_mappings(context) -> pd.DataFrame:
    """Generate selected combo mappings from active concepts (k_max) and write CSV.

    Produces a DataFrame with the same schema as data/combo_mappings.csv filtered to the
    generated combo_ids. This asset always rewrites data/2_tasks/selected_combo_mappings.csv
    via the CSV IO manager when materialized.
    """
    data_root = Path(context.resources.data_root)
    exp_cfg = context.resources.experiment_config
    k_max = int(getattr(exp_cfg, "k_max", 2))
    desc_level = str(getattr(exp_cfg, "description_level", "paragraph"))

    concepts = read_concepts(data_root, filter_active=True)
    if len(concepts) < k_max:
        raise Failure(
            description=f"Not enough active concepts for k_max={k_max}",
            metadata={
                "active_concepts": MetadataValue.int(len(concepts)),
                "k_max": MetadataValue.int(k_max),
                "resolution": MetadataValue.text("Increase active concepts or lower k_max in ExperimentConfig"),
            },
        )

    # Deterministic order
    ordered = sorted(concepts, key=lambda c: c.concept_id)
    ids: List[str] = []
    manager = ComboIDManager(mappings_path=str(data_root / "combo_mappings.csv"))
    for group in combinations(ordered, k_max):
        cid_list = [c.concept_id for c in group]
        combo_id = manager.get_or_create_combo_id(cid_list, desc_level, k_max)
        ids.append(combo_id)

    superset = manager.load_mappings()
    selected = superset[superset["combo_id"].astype(str).isin(set(map(str, ids)))].copy()

    context.add_output_metadata(
        {
            "k_max": MetadataValue.int(k_max),
            "description_level": MetadataValue.text(desc_level),
            "selected_combos": MetadataValue.int(len(set(ids))),
            "rows": MetadataValue.int(len(selected)),
        }
    )
    return selected

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    automation_condition=AutomationCondition.eager(),
)
def content_combinations_csv(
    context,
    content_combinations: List[ContentCombination],
) -> pd.DataFrame:
    """Export content combinations as normalized relational table with combo_id and concept_id columns."""
    # Fail fast if no combinations are available
    if not content_combinations:
        raise Failure(
            description=(
                "Content combinations input is empty. Upstream generation did not create any combinations."
            ),
            metadata={
                "resolution_1": MetadataValue.text(
                    "Check active concepts and k_max in ExperimentConfig"
                ),
                "resolution_2": MetadataValue.text(
                    "Ensure data/1_raw/concepts_metadata.csv has enough active concepts"
                ),
            },
        )
    # Create normalized rows: one row per concept in each combination
    rows = []
    for combo in content_combinations:
        for concept_id in combo.concept_ids:
            rows.append({
                "combo_id": combo.combo_id,
                "concept_id": concept_id
            })
    
    # Ensure a stable schema even when there are no rows
    df = pd.DataFrame(rows, columns=["combo_id", "concept_id"]) if rows else pd.DataFrame(columns=["combo_id", "concept_id"])
    
    context.add_output_metadata({
        "total_rows": MetadataValue.int(len(df)),
        "unique_combinations": MetadataValue.int(len(content_combinations)),
        "unique_concepts": MetadataValue.int(df["concept_id"].nunique() if not df.empty else 0),
        "sample_rows": MetadataValue.text(str(df.head(5).to_dict("records")))
    })

    return df

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
    automation_condition=AutomationCondition.eager(),
    deps={LLM_MODELS_KEY, DRAFT_TEMPLATES_KEY},
)
def draft_generation_tasks(
    context,
    content_combinations: List[ContentCombination],
) -> pd.DataFrame:
    """Create draft-generation tasks (combo × draft_template × model)."""
    data_root = context.resources.data_root
    models_df = read_llm_models(Path(data_root))
    generation_models = models_df[models_df["for_generation"] == True]

    templates_df = read_draft_templates(Path(data_root), filter_active=True)
    active_templates = list(templates_df["template_id"].tolist())

    rows: List[dict] = []
    for combo in content_combinations:
        for template_id in active_templates:
            for _, model_row in generation_models.iterrows():
                model_id = model_row["id"]
                model_name = model_row["model"]
                draft_task_id = f"{combo.combo_id}_{template_id}_{model_id}"
                rows.append(
                    {
                        "draft_task_id": draft_task_id,
                        "combo_id": combo.combo_id,
                        "draft_template": template_id,
                        "generation_model": model_id,
                        "generation_model_name": model_name,
                    }
                )

    # Build DataFrame with stable schema even if there are no rows
    columns = [
        "draft_task_id",
        "combo_id",
        "draft_template",
        "generation_model",
        "generation_model_name",
    ]
    tasks_df = pd.DataFrame(rows, columns=columns)

    # Register dynamic partitions for drafts (guard empty)
    existing = context.instance.get_dynamic_partitions(draft_tasks_partitions.name)
    if not tasks_df.empty:
        if existing:
            for p in existing:
                context.instance.delete_dynamic_partition(draft_tasks_partitions.name, p)
        context.instance.add_dynamic_partitions(draft_tasks_partitions.name, tasks_df["draft_task_id"].tolist())
    else:
        context.log.warning(
            "No draft-generation tasks produced (check active concepts, k_max, active draft templates, generation models). Keeping existing dynamic partitions."
        )

    context.add_output_metadata(
        {
            "task_count": MetadataValue.int(len(tasks_df)),
            "unique_combinations": MetadataValue.int(tasks_df["combo_id"].nunique() if not tasks_df.empty else 0),
            "unique_draft_templates": MetadataValue.int(tasks_df["draft_template"].nunique() if not tasks_df.empty else 0),
            "unique_models": MetadataValue.int(tasks_df["generation_model"].nunique() if not tasks_df.empty else 0),
        }
    )
    return tasks_df


@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
    automation_condition=AutomationCondition.eager(),
    deps={ESSAY_TEMPLATES_KEY},
)
def essay_generation_tasks(
    context,
    draft_generation_tasks: pd.DataFrame,
) -> pd.DataFrame:
    """Create essay-generation tasks (FK to draft tasks × essay_template)."""
    data_root = context.resources.data_root
    essay_templates_df = read_essay_templates(Path(data_root), filter_active=True)
    essay_templates = list(essay_templates_df["template_id"].tolist())

    rows: List[dict] = []
    for _, drow in draft_generation_tasks.iterrows():
        draft_task_id = drow["draft_task_id"]
        combo_id = drow["combo_id"]
        model_id = drow["generation_model"]
        model_name = drow["generation_model_name"]
        for essay_template_id in essay_templates:
            essay_task_id = f"{draft_task_id}_{essay_template_id}"
            rows.append(
                {
                    "essay_task_id": essay_task_id,
                    "draft_task_id": draft_task_id,
                    "combo_id": combo_id,
                    # Canonical: draft_template
                    "draft_template": drow["draft_template"],
                    "essay_template": essay_template_id,
                    "generation_model": model_id,
                    "generation_model_name": model_name,
                }
            )

    columns = [
        "evaluation_task_id",
        "document_id",
        "stage",
        "origin",
        "file_path",
        "combo_id",
        "draft_template",
        "essay_template",
        "generation_model",
        "generation_model_name",
        "draft_task_id",
        "essay_task_id",
        "source_asset",
        "source_dir",
        "evaluation_template",
        "evaluation_model",
        "evaluation_model_name",
    ]
    tasks_df = pd.DataFrame(rows, columns=columns)

    # Register dynamic partitions for essays
    existing = context.instance.get_dynamic_partitions(essay_tasks_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(essay_tasks_partitions.name, p)
    context.instance.add_dynamic_partitions(essay_tasks_partitions.name, tasks_df["essay_task_id"].tolist())

    context.add_output_metadata(
        {
            "task_count": MetadataValue.int(len(tasks_df)),
            "unique_drafts": MetadataValue.int(tasks_df["draft_task_id"].nunique()),
            "unique_essay_templates": MetadataValue.int(tasks_df["essay_template"].nunique()),
        }
    )
    return tasks_df

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"data_root"},
    automation_condition=AutomationCondition.eager(),
)
def document_index(
    context,
    draft_generation_tasks: pd.DataFrame,
    essay_generation_tasks: pd.DataFrame,
) -> pd.DataFrame:
    """Unified index of documents to evaluate (hybrid document axis).

    Includes:
    - Drafts as effective one-phase (if draft file exists)
    - Two-phase essays (if essay file exists)
    - Curated historical docs are supported by writing standard generation task CSVs and placing files under canonical folders (no legacy scanning)
    """
    data_root = _Path(context.resources.data_root)

    rows: _List[dict] = []

    # Drafts (effective one-phase) — support both legacy links_responses and new draft_responses
    legacy_draft_dir = data_root / "3_generation" / "links_responses"
    new_draft_dir = data_root / "3_generation" / "draft_responses"
    if not draft_generation_tasks.empty and (legacy_draft_dir.exists() or new_draft_dir.exists()):
        for _, row in draft_generation_tasks.iterrows():
            draft_task_id = row["draft_task_id"]
            fp_legacy = legacy_draft_dir / f"{draft_task_id}.txt"
            fp_new = new_draft_dir / f"{draft_task_id}.txt"
            fp = fp_new if fp_new.exists() else fp_legacy
            if fp and fp.exists():
                rows.append(
                    {
                        "document_id": draft_task_id,
                        "stage": "essay1p",
                        "origin": "draft",
                        "file_path": str(fp),
                        "combo_id": row["combo_id"],
                        # Canonical: draft_template; no essay template at this stage
                        "draft_template": row["draft_template"],
                        "essay_template": None,
                        "generation_model_id": row["generation_model"],
                        "generation_model_name": row["generation_model_name"],
                        "draft_task_id": draft_task_id,
                        "essay_task_id": None,
                        "source_asset": "draft_response" if fp_new.exists() else "links_response",
                        "source_dir": "draft_responses" if fp_new.exists() else "links_responses",
                    }
                )

    # Two-phase essays
    essay_dir = data_root / "3_generation" / "essay_responses"
    if not essay_generation_tasks.empty and essay_dir.exists():
        for _, row in essay_generation_tasks.iterrows():
            essay_task_id = row["essay_task_id"]
            fp = essay_dir / f"{essay_task_id}.txt"
            if fp.exists():
                rows.append(
                    {
                        "document_id": essay_task_id,
                        "stage": "essay2p",
                        "origin": "two_phase",
                        "file_path": str(fp),
                        "combo_id": row["combo_id"],
                        "draft_template": row.get("draft_template"),
                        "essay_template": row["essay_template"],
                        "generation_model_id": row["generation_model"],
                        "generation_model_name": row["generation_model_name"],
                        "draft_task_id": row.get("draft_task_id"),
                        "essay_task_id": essay_task_id,
                        "source_asset": "essay_response",
                        "source_dir": "essay_responses",
                    }
                )

    columns = [
        "document_id",
        "stage",
        "origin",
        "file_path",
        "combo_id",
        "draft_template",
        "essay_template",
        "generation_model_id",
        "generation_model_name",
        "draft_task_id",
        "essay_task_id",
        "source_asset",
        "source_dir",
    ]
    df = pd.DataFrame(rows, columns=columns)

    context.add_output_metadata(
        {
            "documents": MetadataValue.int(len(df)),
            "by_stage": MetadataValue.text(str(df["stage"].value_counts().to_dict() if not df.empty else {})),
            "by_origin": MetadataValue.text(str(df["origin"].value_counts().to_dict() if not df.empty else {})),
        }
    )
    return df

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
    automation_condition=AutomationCondition.eager(),
    deps={LLM_MODELS_KEY, EVALUATION_TEMPLATES_KEY},
)
def evaluation_tasks(
    context,
    essay_generation_tasks: pd.DataFrame,
    draft_generation_tasks: pd.DataFrame,
) -> pd.DataFrame:
    """Create evaluation tasks (document × evaluation_template × evaluation_model)."""
    data_root = context.resources.data_root
    models_df = read_llm_models(Path(data_root))
    evaluation_models = models_df[models_df["for_evaluation"] == True]

    evaluation_templates_df = read_evaluation_templates(Path(data_root))
    if "active" in evaluation_templates_df.columns:
        evaluation_templates_df = evaluation_templates_df[evaluation_templates_df["active"] == True]
    eval_templates = list(evaluation_templates_df["template_id"].tolist())

    # Build an internal document view (avoid requiring document_index asset in all callers)
    data_root_path = _Path(data_root)
    docs: List[dict] = []

    # Two-phase essays
    essay_dir = data_root_path / "3_generation" / "essay_responses"
    if not essay_generation_tasks.empty:
        for _, row in essay_generation_tasks.iterrows():
            essay_task_id = row["essay_task_id"]
            # Find the essay anywhere across current/legacy locations
            fp, src = find_document_path(essay_task_id, data_root_path)
            # Include all planned essays (file may be generated later)
            docs.append(
                {
                    "document_id": essay_task_id,
                    "stage": "essay2p",
                    "origin": "two_phase",
                    "file_path": str(fp) if fp else str(essay_dir / f"{essay_task_id}.txt"),
                    "combo_id": row["combo_id"],
                    "draft_template": row.get("draft_template") or row.get("link_template"),
                    "essay_template": row["essay_template"],
                    "generation_model_id": row["generation_model"],
                    "generation_model_name": row["generation_model_name"],
                    "draft_task_id": row.get("draft_task_id", row.get("link_task_id")),
                    "essay_task_id": essay_task_id,
                    "source_asset": "essay_response",
                    "source_dir": src or "essay_responses",
                }
            )

    # Drafts as one-phase documents (optional)
    draft_dir = data_root_path / "3_generation" / "draft_responses"
    if not draft_generation_tasks.empty:
        for _, row in draft_generation_tasks.iterrows():
            draft_task_id = row.get("draft_task_id") or row.get("link_task_id")
            if not isinstance(draft_task_id, str) or not draft_task_id:
                continue
            fp, src = find_document_path(draft_task_id, data_root_path)
            docs.append(
                {
                    "document_id": draft_task_id,
                    "stage": "essay1p",
                    "origin": "draft",
                    "file_path": str(fp) if fp else str(draft_dir / f"{draft_task_id}.txt"),
                    "combo_id": row.get("combo_id"),
                    "draft_template": row.get("draft_template") or row.get("link_template"),
                    "essay_template": None,
                    "generation_model_id": row.get("generation_model"),
                    "generation_model_name": row.get("generation_model_name"),
                    "draft_task_id": draft_task_id,
                    "essay_task_id": None,
                    "source_asset": "draft_response",
                    "source_dir": src or "draft_responses",
                }
            )

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
                    }
                )

    tasks_df = pd.DataFrame(rows)

    # Register dynamic partitions for evaluations
    existing = context.instance.get_dynamic_partitions(evaluation_tasks_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(evaluation_tasks_partitions.name, p)
    if not tasks_df.empty:
        context.instance.add_dynamic_partitions(
            evaluation_tasks_partitions.name, tasks_df["evaluation_task_id"].tolist()
        )

    context.add_output_metadata(
        {
            "task_count": MetadataValue.int(len(tasks_df)),
            "unique_documents": MetadataValue.int(tasks_df["document_id"].nunique() if not tasks_df.empty else 0),
            "unique_eval_templates": MetadataValue.int(tasks_df["evaluation_template"].nunique() if not tasks_df.empty else 0),
            "unique_eval_models": MetadataValue.int(tasks_df["evaluation_model"].nunique() if not tasks_df.empty else 0),
        }
    )
    return tasks_df
