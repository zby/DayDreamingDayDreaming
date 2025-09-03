from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import evaluation_tasks_partitions
import pandas as pd
import logging
from jinja2 import Environment
from ..utils.dataframe_helpers import get_task_row
from ..utils.raw_readers import read_evaluation_templates
from .raw_data import EVALUATION_TEMPLATES_KEY

logger = logging.getLogger(__name__)


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"data_root"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def evaluation_prompt(context, evaluation_tasks) -> str:
    """
    Generate one evaluation prompt per partition using FK-based data loading.
    
    This asset uses the document_index-provided file_path to read the target
    document (draft or essay) and render an evaluation prompt.
    
    Key aspects:
    - FK relationship: evaluation_tasks carries document metadata including file_path.
    - Manual IO: reads file_path directly; no cross-partition IO manager access required.
    
    Data Flow:
    evaluation_task (partition: eval_001)
    -> reads file_path from evaluation_tasks
    -> reads document text from disk
    -> renders evaluation template
    
    Args:
        context: Dagster asset context (provides partition_key and resources)
        evaluation_tasks: DataFrame with evaluation task definitions including FKs
        evaluation_templates: DataFrame with evaluation template content
        
    Returns:
        str: Generated evaluation prompt text for this partition
        
    Raises:
        Failure: If evaluation_task_id not found in DataFrame
        Failure: If file_path is missing or the document file cannot be found
        
    Dependencies:
        - evaluation_tasks: Must be materialized (provides FK relationships and file_path)
        
    See Also:
        - docs/evaluation_asset_architecture.md: Detailed architecture explanation
        - plans/pass_generation_response_via_pipeline_fk.md: Alternative approaches
    """
    task_id = context.partition_key

    # Get the specific task for this partition
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    file_path = task_row.get("file_path")
    if not isinstance(file_path, str) or not len(file_path):
        raise Failure(
            description=f"Missing or invalid file_path for evaluation task '{task_id}'",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "file_path": MetadataValue.text(str(file_path)),
            }
        )

    expected_path = Path(file_path)
    try:
        draft_content = expected_path.read_text(encoding="utf-8")
        if not draft_content:
            raise ValueError(f"Empty document content loaded for {file_path}")
        context.log.info(f"Loaded document content for {task_id}: {len(draft_content)} chars from {file_path}")
    except FileNotFoundError as e:
        raise Failure(
            description=f"Missing document for evaluation task '{task_id}'",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "expected_file_path": MetadataValue.path(str(expected_path)),
                "source_asset": MetadataValue.text(str(task_row.get("source_asset"))),
                "source_dir": MetadataValue.text(str(task_row.get("source_dir"))),
            }
        ) from e

    # Load evaluation templates directly from CSV and convert to dict
    eval_df = read_evaluation_templates(Path(context.resources.data_root))
    evaluation_templates_dict: dict[str, str] = {}
    if 'content' in eval_df.columns:
        evaluation_templates_dict = eval_df.set_index('template_id')['content'].to_dict()
    else:
        # Fallback: read template files from data/1_raw/evaluation_templates/<template_id>.txt
        templates_base = Path(context.resources.data_root) / "1_raw" / "evaluation_templates"
        for _, row in eval_df.iterrows():
            template_id = row.get('template_id')
            if not isinstance(template_id, str) or not len(template_id):
                continue
            fp = templates_base / f"{template_id}.txt"
            try:
                evaluation_templates_dict[template_id] = fp.read_text(encoding='utf-8')
            except FileNotFoundError:
                context.log.warning(f"Evaluation template file not found: {fp}")
    
    # Render evaluation template directly for this task
    template_id = task_row["evaluation_template"]
    template_content = evaluation_templates_dict[template_id]
    env = Environment()
    template = env.from_string(template_content)
    eval_prompt = template.render(response=draft_content)
    context.log.info(f"Generated evaluation prompt for task {task_id}")
    
    # Add output metadata for debugging and monitoring
    context.add_output_metadata({
        "evaluation_task_id": MetadataValue.text(task_id),
        "source_stage": MetadataValue.text(str(task_row.get("stage"))),
        "document_id": MetadataValue.text(str(task_row.get("document_id"))),
        "document_content_length": MetadataValue.int(len(draft_content)),
        "evaluation_prompt_length": MetadataValue.int(len(eval_prompt)),
        "target_path": MetadataValue.path(str(expected_path)),
        "template_used": MetadataValue.text(task_row["evaluation_template"]),
        "model_planned": MetadataValue.text(task_row["evaluation_model_name"])
    })
    
    return eval_prompt


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["evaluation_prompt", "evaluation_tasks"],
)
def evaluation_response(context, evaluation_prompt, evaluation_tasks) -> str:
    """Generate one evaluation response per partition."""
    task_id = context.partition_key
    
    # Debug: Check if task_id exists in evaluation_tasks
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    
    # Use the model name directly from the task
    model_name = task_row["evaluation_model_name"]
    
    # The prompt is already generated and saved as a file by evaluation_prompt asset
    eval_prompt = evaluation_prompt
    
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(eval_prompt, model=model_name)
    
    context.log.info(f"Generated evaluation response for task {task_id} using model {model_name}")
    return response
