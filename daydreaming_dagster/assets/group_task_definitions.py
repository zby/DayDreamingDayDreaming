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
)
from .partitions import (
    draft_tasks_partitions,
    essay_tasks_partitions,
    evaluation_tasks_partitions,
)
from ..utils.combo_ids import ComboIDManager
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
    """Build a minimal set of combinations from active concepts.

    Reads active concepts under `data/1_raw/concepts*` and produces a single
    ContentCombination using up to `k_max` concepts at `description_level`.
    """
    from ..utils.raw_readers import read_concepts

    cfg = context.resources.experiment_config
    level = getattr(cfg, "description_level", "paragraph")
    k_max = int(getattr(cfg, "k_max", 2))
    data_root = Path(getattr(context.resources, "data_root", "data"))
    concepts = read_concepts(data_root, filter_active=True)
    if not concepts:
        context.add_output_metadata({"count": MetadataValue.int(0)})
        return []
    selected = concepts[: max(1, min(k_max, len(concepts)))]
    # Ensure combo_mappings.csv exists and get a stable combo_id
    manager = ComboIDManager(str(data_root / "combo_mappings.csv"))
    combo_id = manager.get_or_create_combo_id([c.concept_id for c in selected], level, k_max)
    combo = ContentCombination.from_concepts(selected, level, combo_id=combo_id)
    context.add_output_metadata({"count": MetadataValue.int(1)})
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
                        "parser": parser_map.get(str(eval_template_id)),
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
    # Drafts treated as one-phase
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
