"""
Group: task_definitions

Asset definitions for the task definition stage (combinations, draft/essay tasks, document index, evaluation tasks).
"""

from dagster import asset, MetadataValue, AutomationCondition, Failure
from typing import List
from pathlib import Path
import pandas as pd
from itertools import combinations
from ...models import ContentCombination
from ..raw_data import (
    LLM_MODELS_KEY,
    DRAFT_TEMPLATES_KEY,
    ESSAY_TEMPLATES_KEY,
    EVALUATION_TEMPLATES_KEY,
)
from ...utils.raw_readers import (
    read_concepts,
    read_llm_models,
    read_draft_templates,
    read_essay_templates,
    read_evaluation_templates,
)
from ..partitions import (
    evaluation_tasks_partitions,
    draft_tasks_partitions,
    essay_tasks_partitions,
)
from ...utils.combo_ids import ComboIDManager
from ...utils.selected_combos import validate_selected_is_subset
from ...utils.document_locator import find_document_path


@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config", "data_root"},
    automation_condition=AutomationCondition.eager(),
)
def selected_combo_mappings(context) -> pd.DataFrame:
    """Provide the selected combo subset as a DataFrame.

    Behavior:
    - If a curated file exists at data/2_tasks/selected_combo_mappings.csv, load and validate it
      as a strict subset of data/combo_mappings.csv.
    - Otherwise, generate the selection deterministically from active concepts and k_max, and
      return that DataFrame (also written to CSV by the IO manager).
    """
    data_root = Path(context.resources.data_root)
    curated_path = data_root / "2_tasks" / "selected_combo_mappings.csv"
    superset_path = data_root / "combo_mappings.csv"

    # Curated selection path (preferred when present)
    if curated_path.exists():
        try:
            df = pd.read_csv(curated_path)
        except Exception as e:
            raise Failure(
                description="Failed to read curated selected_combo_mappings.csv",
                metadata={
                    "path": MetadataValue.path(str(curated_path)),
                    "error": MetadataValue.text(str(e)),
                },
            )
        if df.empty:
            raise Failure(
                description="Curated selected_combo_mappings.csv is empty",
                metadata={"path": MetadataValue.path(str(curated_path))},
            )
        if superset_path.exists():
            from ...utils.selected_combos import validate_selected_is_subset as _validate
            _validate(df, superset_path)
        context.add_output_metadata(
            {
                "mode": MetadataValue.text("curated_file"),
                "rows": MetadataValue.int(len(df)),
                "unique_combos": MetadataValue.int(df["combo_id"].nunique()),
            }
        )
        return df

    # Generated selection (fallback when no curated file present)
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
            },
        )

    ordered = sorted(concepts, key=lambda c: c.concept_id)
    ids: List[str] = []
    manager = ComboIDManager(mappings_path=str(superset_path))
    for group in combinations(ordered, k_max):
        cid_list = [c.concept_id for c in group]
        combo_id = manager.get_or_create_combo_id(cid_list, desc_level, k_max)
        ids.append(combo_id)

    superset = manager.load_mappings()
    selected = superset[superset["combo_id"].astype(str).isin(set(map(str, ids)))].copy()
    context.add_output_metadata(
        {
            "mode": MetadataValue.text("generated"),
            "k_max": MetadataValue.int(k_max),
            "description_level": MetadataValue.text(desc_level),
            "selected_combos": MetadataValue.int(len(set(ids))),
            "rows": MetadataValue.int(len(selected)),
        }
    )
    return selected


@asset(
    group_name="task_definitions",
    required_resource_keys={"data_root"},
    automation_condition=AutomationCondition.eager(),
)
def content_combinations(
    context,
    selected_combo_mappings: pd.DataFrame,
) -> List[ContentCombination]:
    """Build ContentCombination objects from selected subset mappings (no k‑max fallback)."""
    data_root = Path(context.resources.data_root)
    selected_df = selected_combo_mappings.copy()
    superset_path = data_root / "combo_mappings.csv"
    if superset_path.exists() and not selected_df.empty:
        try:
            validate_selected_is_subset(selected_df, superset_path)
        except Exception as e:
            raise Failure(
                description="Selected combo mappings are invalid (not a strict subset of data/combo_mappings.csv)",
                metadata={
                    "error": MetadataValue.text(str(e)),
                    "selected_path": MetadataValue.path(data_root / "2_tasks" / "selected_combo_mappings.csv"),
                    "superset_path": MetadataValue.path(superset_path),
                },
            )

    if selected_df.empty:
        raise Failure(
            description="No selected combinations found (selected_combo_mappings.csv is empty)",
            metadata={
                "selected_path": MetadataValue.path(data_root / "2_tasks" / "selected_combo_mappings.csv"),
            },
        )

    all_concepts = read_concepts(data_root, filter_active=False)
    by_id = {c.concept_id: c for c in all_concepts}
    required = {"combo_id", "concept_id", "description_level"}
    if not required.issubset(set(selected_df.columns)):
        missing = sorted(required - set(selected_df.columns))
        raise Failure(
            description="selected_combo_mappings.csv missing required columns",
            metadata={"missing": MetadataValue.text(", ".join(missing))},
        )

    combos: List[ContentCombination] = []
    for combo_id, grp in selected_df.groupby("combo_id"):
        levels = [str(x) for x in grp["description_level"].dropna().astype(str).unique().tolist()]
        desc_level = (
            levels[0]
            if levels
            else str(getattr(getattr(context.resources, "experiment_config", None), "description_level", "paragraph"))
        )
        concept_ids = sorted(grp["concept_id"].astype(str).unique().tolist())
        missing = [cid for cid in concept_ids if cid not in by_id]
        if missing:
            raise Failure(
                description=f"Missing concept(s) for combo {combo_id}",
                metadata={"missing_concepts": MetadataValue.text(", ".join(missing[:10]))},
            )
        ordered_concepts = [by_id[cid] for cid in concept_ids]
        combos.append(ContentCombination.from_concepts(ordered_concepts, desc_level, combo_id=str(combo_id)))

    context.add_output_metadata(
        {
            "combination_count": MetadataValue.int(len(combos)),
            "stable_id_format": MetadataValue.text("combo_v1_<12-hex>"),
        }
    )
    return combos


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
    columns = [
        "draft_task_id",
        "combo_id",
        "draft_template",
        "generation_model",
        "generation_model_name",
    ]
    tasks_df = pd.DataFrame(rows, columns=columns)
    existing = context.instance.get_dynamic_partitions(draft_tasks_partitions.name)
    if not tasks_df.empty:
        if existing:
            for p in existing:
                context.instance.delete_dynamic_partition(draft_tasks_partitions.name, p)
        context.instance.add_dynamic_partitions(draft_tasks_partitions.name, tasks_df["draft_task_id"].tolist())
    context.add_output_metadata({"task_count": MetadataValue.int(len(tasks_df))})
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
                    "draft_template": drow["draft_template"],
                    "essay_template": essay_template_id,
                    "generation_model": model_id,
                    "generation_model_name": model_name,
                }
            )
    columns = [
        "essay_task_id",
        "draft_task_id",
        "combo_id",
        "draft_template",
        "essay_template",
        "generation_model",
        "generation_model_name",
    ]
    tasks_df = pd.DataFrame(rows, columns=columns)
    existing = context.instance.get_dynamic_partitions(essay_tasks_partitions.name)
    if not tasks_df.empty:
        if existing:
            for p in existing:
                context.instance.delete_dynamic_partition(essay_tasks_partitions.name, p)
        context.instance.add_dynamic_partitions(essay_tasks_partitions.name, tasks_df["essay_task_id"].tolist())
    context.add_output_metadata({"task_count": MetadataValue.int(len(tasks_df))})
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
    data_root = Path(context.resources.data_root)
    rows: List[dict] = []
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
    return pd.DataFrame(rows)


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
    data_root = Path(context.resources.data_root)
    models_df = read_llm_models(Path(data_root))
    evaluation_models = models_df[models_df["for_evaluation"] == True]
    evaluation_templates_df = read_evaluation_templates(Path(data_root))
    if "active" in evaluation_templates_df.columns:
        evaluation_templates_df = evaluation_templates_df[evaluation_templates_df["active"] == True]
    eval_templates = list(evaluation_templates_df["template_id"].tolist())

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
    draft_dir = data_root_path / "3_generation" / "draft_responses"
    if not draft_generation_tasks.empty:
        for _, row in draft_generation_tasks.iterrows():
            draft_task_id = row.get("draft_task_id")
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
                    "draft_template": row.get("draft_template"),
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
