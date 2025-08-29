from dagster import asset, MetadataValue, AutoMaterializePolicy, Failure
from typing import Dict, Tuple, List
import pandas as pd
from itertools import combinations
from ..models import Concept, ContentCombination
from .partitions import (
    generation_tasks_partitions,
    evaluation_tasks_partitions,
    link_tasks_partitions,
    essay_tasks_partitions,
)
from ..utils.combo_ids import ComboIDManager

@asset(
    group_name="task_definitions",
    required_resource_keys={"experiment_config"},
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def content_combinations(
    context,
    concepts: List[Concept],
) -> List[ContentCombination]:
    """Generate k-max combinations of concepts with resolved content using ContentCombination."""
    experiment_config = context.resources.experiment_config
    k_max = experiment_config.k_max
    
    context.log.info(f"Generating content combinations with k_max={k_max}")
    
    # Fail fast when there aren't enough active concepts to form any combination
    if len(concepts) < k_max:
        raise Failure(
            description=(
                "Insufficient active concepts to generate content combinations. "
                "Increase active concepts or lower k_max."
            ),
            metadata={
                "active_concepts": MetadataValue.int(len(concepts)),
                "k_max": MetadataValue.int(k_max),
                "additional_needed": MetadataValue.int(max(0, k_max - len(concepts))),
                "resolution_1": MetadataValue.text(
                    "Activate more concepts in data/1_raw/concepts/concepts_metadata.csv"
                ),
                "resolution_2": MetadataValue.text(
                    "Or lower k_max via ExperimentConfig in the Dagster Launchpad"
                ),
            },
        )
    
    # Generate all k-max combinations and create ContentCombination objects
    content_combos: List[ContentCombination] = []
    description_level = experiment_config.description_level
    manager = ComboIDManager()

    for combo in combinations(concepts, k_max):
        concept_ids = [c.concept_id for c in combo]
        stable_id = manager.get_or_create_combo_id(
            concept_ids, description_level, k_max
        )

        # Create ContentCombination with resolved content and stable combo_id
        content_combo = ContentCombination.from_concepts(
            list(combo),
            description_level,
            combo_id=stable_id,
        )
        content_combos.append(content_combo)
    
    context.log.info(f"Generated {len(content_combos)} content combinations")
    
    if not content_combos:
        # Defensive check: combinations() yielded zero results for the given inputs
        raise Failure(
            description=(
                "No content combinations were generated. This typically occurs when "
                "k_max is too large for the number of active concepts."
            ),
            metadata={
                "active_concepts": MetadataValue.int(len(concepts)),
                "k_max": MetadataValue.int(k_max),
                "description_level": MetadataValue.text(description_level),
                "resolution": MetadataValue.text(
                    "Increase active concepts or lower k_max in ExperimentConfig"
                ),
            },
        )
    
    # Add output metadata
    context.add_output_metadata({
        "combination_count": MetadataValue.int(len(content_combos)),
        "k_max_used": MetadataValue.int(k_max),
        "source_concepts": MetadataValue.int(len(concepts)),
        "description_level": MetadataValue.text(description_level),
        "stable_id_format": MetadataValue.text("combo_v1_<12-hex>"),
    })
    
    return content_combos

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
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
                    "Ensure data/1_raw/concepts/concepts_metadata.csv has enough active concepts"
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
    required_resource_keys={"experiment_config"},
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def link_generation_tasks(
    context,
    content_combinations: List[ContentCombination],
    llm_models: pd.DataFrame,
    link_templates: pd.DataFrame,
) -> pd.DataFrame:
    """Create link-generation tasks (combo × link_template × model)."""
    generation_models = llm_models[llm_models["for_generation"] == True]

    active_templates_df = link_templates[link_templates["active"] == True]
    active_templates = list(active_templates_df["template_id"].tolist())

    rows: List[dict] = []
    for combo in content_combinations:
        for template_id in active_templates:
            for _, model_row in generation_models.iterrows():
                model_id = model_row["id"]
                model_name = model_row["model"]
                link_task_id = f"{combo.combo_id}_{template_id}_{model_id}"
                rows.append(
                    {
                        "link_task_id": link_task_id,
                        "combo_id": combo.combo_id,
                        "link_template": template_id,
                        "generation_model": model_id,
                        "generation_model_name": model_name,
                    }
                )

    # Build DataFrame with stable schema even if there are no rows
    columns = [
        "link_task_id",
        "combo_id",
        "link_template",
        "generation_model",
        "generation_model_name",
    ]
    tasks_df = pd.DataFrame(rows, columns=columns)

    # Register dynamic partitions for links (guard empty)
    existing = context.instance.get_dynamic_partitions(link_tasks_partitions.name)
    if not tasks_df.empty:
        # Replace partitions atomically: clear then add
        if existing:
            for p in existing:
                context.instance.delete_dynamic_partition(link_tasks_partitions.name, p)
        context.instance.add_dynamic_partitions(link_tasks_partitions.name, tasks_df["link_task_id"].tolist())
    else:
        # Keep existing partitions if no new tasks are produced in this run
        context.log.warning(
            "No link-generation tasks produced (check active concepts, k_max, active link templates, generation models). Keeping existing dynamic partitions."
        )

    context.add_output_metadata(
        {
            "task_count": MetadataValue.int(len(tasks_df)),
            "unique_combinations": MetadataValue.int(tasks_df["combo_id"].nunique() if not tasks_df.empty else 0),
            "unique_link_templates": MetadataValue.int(tasks_df["link_template"].nunique() if not tasks_df.empty else 0),
            "unique_models": MetadataValue.int(tasks_df["generation_model"].nunique() if not tasks_df.empty else 0),
        }
    )
    return tasks_df


@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config"},
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def essay_generation_tasks(
    context,
    link_generation_tasks: pd.DataFrame,
    essay_templates: pd.DataFrame,
) -> pd.DataFrame:
    """Create essay-generation tasks (FK to link tasks × essay_template)."""
    active_templates_df = essay_templates[essay_templates["active"] == True]
    essay_templates = list(active_templates_df["template_id"].tolist())

    rows: List[dict] = []
    for _, link_row in link_generation_tasks.iterrows():
        link_task_id = link_row["link_task_id"]
        combo_id = link_row["combo_id"]
        model_id = link_row["generation_model"]
        model_name = link_row["generation_model_name"]
        for essay_template_id in essay_templates:
            essay_task_id = f"{link_task_id}_{essay_template_id}"
            rows.append(
                {
                    "essay_task_id": essay_task_id,
                    "link_task_id": link_task_id,
                    "combo_id": combo_id,
                    "link_template": link_row["link_template"],
                    "essay_template": essay_template_id,
                    "generation_model": model_id,
                    "generation_model_name": model_name,
                }
            )

    tasks_df = pd.DataFrame(rows)

    # Register dynamic partitions for essays
    existing = context.instance.get_dynamic_partitions(essay_tasks_partitions.name)
    if existing:
        for p in existing:
            context.instance.delete_dynamic_partition(essay_tasks_partitions.name, p)
    context.instance.add_dynamic_partitions(essay_tasks_partitions.name, tasks_df["essay_task_id"].tolist())

    context.add_output_metadata(
        {
            "task_count": MetadataValue.int(len(tasks_df)),
            "unique_links": MetadataValue.int(tasks_df["link_task_id"].nunique()),
            "unique_essay_templates": MetadataValue.int(tasks_df["essay_template"].nunique()),
        }
    )
    return tasks_df

@asset(
    group_name="task_definitions",
    io_manager_key="csv_io_manager",
    required_resource_keys={"experiment_config"},
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def evaluation_tasks(
    context,
    essay_generation_tasks: pd.DataFrame,
    llm_models: pd.DataFrame,
    evaluation_templates: pd.DataFrame,
) -> pd.DataFrame:
    """Create evaluation tasks referencing essay_task_id (one or more per essay)."""
    evaluation_models = llm_models[llm_models["for_evaluation"] == True]

    active_templates_df = evaluation_templates[evaluation_templates["active"] == True]
    eval_templates = list(active_templates_df["template_id"].tolist())

    rows: List[dict] = []
    for _, essay_row in essay_generation_tasks.iterrows():
        essay_task_id = essay_row["essay_task_id"]
        for _, eval_model_row in evaluation_models.iterrows():
            eval_model_id = eval_model_row["id"]
            eval_model_name = eval_model_row["model"]
            for eval_template_id in eval_templates:
                evaluation_task_id = f"{essay_task_id}_{eval_template_id}_{eval_model_id}"
                rows.append(
                    {
                        "evaluation_task_id": evaluation_task_id,
                        "essay_task_id": essay_task_id,
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
    context.instance.add_dynamic_partitions(
        evaluation_tasks_partitions.name, tasks_df["evaluation_task_id"].tolist()
    )

    context.add_output_metadata(
        {
            "task_count": MetadataValue.int(len(tasks_df)),
            "unique_essays": MetadataValue.int(tasks_df["essay_task_id"].nunique()),
            "unique_eval_templates": MetadataValue.int(tasks_df["evaluation_template"].nunique()),
            "unique_eval_models": MetadataValue.int(tasks_df["evaluation_model"].nunique()),
        }
    )
    return tasks_df
