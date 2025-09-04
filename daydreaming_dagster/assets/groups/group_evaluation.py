"""
Group: evaluation

Asset definitions for the evaluation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from ..partitions import evaluation_tasks_partitions
import pandas as pd
import logging
from jinja2 import Environment
from ...utils.dataframe_helpers import get_task_row
from ...utils.raw_readers import read_evaluation_templates
from ..raw_data import EVALUATION_TEMPLATES_KEY

logger = logging.getLogger(__name__)


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"data_root"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def evaluation_prompt(context, evaluation_tasks) -> str:
    task_id = context.partition_key
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    file_path = task_row.get("file_path")
    if not isinstance(file_path, str) or not len(file_path):
        raise Failure(
            description=f"Missing or invalid file_path for evaluation task '{task_id}'",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "file_path": MetadataValue.text(str(file_path)),
            },
        )
    expected_path = Path(file_path)
    try:
        doc = expected_path.read_text(encoding="utf-8")
        if not doc:
            raise ValueError(f"Empty document content loaded for {file_path}")
    except FileNotFoundError as e:
        raise Failure(
            description=f"Missing document for evaluation task '{task_id}'",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "expected_file_path": MetadataValue.path(str(expected_path)),
                "source_asset": MetadataValue.text(str(task_row.get("source_asset"))),
                "source_dir": MetadataValue.text(str(task_row.get("source_dir"))),
            },
        ) from e

    eval_df = read_evaluation_templates(Path(context.resources.data_root))
    evaluation_templates_dict: dict[str, str] = {}
    if "content" in eval_df.columns:
        evaluation_templates_dict = eval_df.set_index("template_id")["content"].to_dict()
    else:
        templates_base = Path(context.resources.data_root) / "1_raw" / "evaluation_templates"
        for _, row in eval_df.iterrows():
            template_id = row.get("template_id")
            if not isinstance(template_id, str) or not len(template_id):
                continue
            fp = templates_base / f"{template_id}.txt"
            try:
                evaluation_templates_dict[template_id] = fp.read_text(encoding="utf-8")
            except FileNotFoundError:
                context.log.warning(f"Evaluation template file not found: {fp}")

    template_id = task_row["evaluation_template"]
    template_content = evaluation_templates_dict[template_id]
    env = Environment()
    template = env.from_string(template_content)
    eval_prompt = template.render(response=doc)
    context.add_output_metadata(
        {
            "evaluation_task_id": MetadataValue.text(task_id),
            "source_stage": MetadataValue.text(str(task_row.get("stage"))),
            "document_id": MetadataValue.text(str(task_row.get("document_id"))),
            "document_content_length": MetadataValue.int(len(doc)),
            "evaluation_prompt_length": MetadataValue.int(len(eval_prompt)),
            "target_path": MetadataValue.path(str(expected_path)),
            "template_used": MetadataValue.text(task_row["evaluation_template"]),
            "model_planned": MetadataValue.text(task_row["evaluation_model_name"]),
        }
    )
    return eval_prompt


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["evaluation_prompt", "evaluation_tasks"],
)
def evaluation_response(context, evaluation_prompt, evaluation_tasks) -> str:
    task_id = context.partition_key
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    model_name = task_row["evaluation_model_name"]
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(evaluation_prompt, model=model_name)
    context.log.info(f"Generated evaluation response for task {task_id} using model {model_name}")
    return response

