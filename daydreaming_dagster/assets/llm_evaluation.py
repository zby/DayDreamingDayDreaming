from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import evaluation_tasks_partitions
import pandas as pd
import logging
from jinja2 import Environment
from ..utils.dataframe_helpers import get_task_row
from ..utils.shared_context import MockLoadContext
from ..utils.raw_readers import read_evaluation_templates
from .raw_data import EVALUATION_TEMPLATES_KEY

logger = logging.getLogger(__name__)


def generate_evaluation_prompts(
    draft_responses: dict[str, str],
    evaluation_tasks: pd.DataFrame,
    evaluation_templates: dict[str, str],
) -> dict[str, str]:
    """
    Generate evaluation prompts from draft (links) responses.

    Args:
        draft_responses: Dict mapping link_task_id to response text
        evaluation_tasks: DataFrame of evaluation tasks
        evaluation_templates: Dict of template_id -> template_content

    Returns:
        Dictionary mapping evaluation_task_id to evaluation prompt text
    """
    logger.info("Generating evaluation prompts")

    eval_prompts = {}
    env = Environment()

    for _, task_row in evaluation_tasks.iterrows():
        evaluation_task_id = task_row["evaluation_task_id"]
        link_task_id = task_row["link_task_id"]
        template_id = task_row["evaluation_template"]

        # Get the draft response
        if link_task_id not in draft_responses:
            logger.warning(
                f"No draft response found for {link_task_id}, skipping evaluation task {evaluation_task_id}"
            )
            continue

        draft_response = draft_responses[link_task_id]

        # Render evaluation template
        template_content = evaluation_templates[template_id]
        template = env.from_string(template_content)
        eval_prompt = template.render(response=draft_response)

        eval_prompts[evaluation_task_id] = eval_prompt

    logger.info(f"Generated {len(eval_prompts)} evaluation prompts")
    return eval_prompts


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"links_response_io_manager", "data_root"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def evaluation_prompt(context, evaluation_tasks) -> str:
    """
    Generate one evaluation prompt per partition using FK-based data loading.
    
    This asset demonstrates the "Manual IO with Foreign Keys" pattern documented
    in docs/evaluation_asset_architecture.md. Key aspects:
    
    1. FK Relationship: Uses link_task_id from evaluation_tasks to load
       the corresponding draft (links_response) from a different partition
    
    2. MockLoadContext Pattern: Creates a temporary context to interface with
       the IO manager for cross-partition data loading
    
    3. Intentional Manual IO: Bypasses normal Dagster input/output pattern
       to handle complex partition relationships with foreign keys
    
    Data Flow:
    evaluation_task (partition: eval_001) 
    -> reads link_task_id FK 
    -> loads links_response (partition: link_123) via IO manager
    -> combines with evaluation template 
    -> generates evaluation prompt
    
    Args:
        context: Dagster asset context (provides partition_key and resources)
        evaluation_tasks: DataFrame with evaluation task definitions including FKs
        evaluation_templates: DataFrame with evaluation template content
        
    Returns:
        str: Generated evaluation prompt text for this partition
        
    Raises:
        Failure: If evaluation_task_id not found in DataFrame
        Failure: If referenced link_task_id is invalid/empty
        Failure: If links_response file not found (FK broken)
        
    Resources Required:
        - links_response_io_manager: To load upstream draft responses
        
    Dependencies:
        - evaluation_tasks: Must be materialized (provides FK relationships)
        - links_response: Must be materialized for referenced partitions
        
    See Also:
        - docs/evaluation_asset_architecture.md: Detailed architecture explanation
        - plans/pass_generation_response_via_pipeline_fk.md: Alternative approaches
    """
    task_id = context.partition_key

    # Get the specific task for this partition with debugging
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    link_task_id = task_row["link_task_id"]

    # Log the foreign key relationship for debugging
    context.log.info(f"Loading draft response for FK relationship: {task_id} -> {link_task_id}")

    # Load the draft response from the correct partition using I/O manager
    # This uses the MockLoadContext pattern to load data from a different partition
    draft_io_manager = context.resources.links_response_io_manager

    # Validate that the referenced link_task_id looks valid
    if not link_task_id or link_task_id.strip() == "":
        raise Failure(
            description=f"Invalid link_task_id referenced by evaluation task '{task_id}'",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "link_task_id": MetadataValue.text(str(link_task_id)),
                "resolution_1": MetadataValue.text("Check evaluation_tasks CSV for data corruption"),
                "resolution_2": MetadataValue.text("Re-materialize evaluation_tasks asset"),
                "fk_validation_failed": MetadataValue.text("link_task_id is empty or invalid")
            }
        )

    # Create a mock context for the generation partition (documented pattern)
    mock_context = MockLoadContext(link_task_id)

    # Check if the file exists before attempting to load (better error context)
    expected_path = draft_io_manager.base_path / f"{link_task_id}.txt"

    try:
        # Load the draft content directly from the link responses
        draft_content = draft_io_manager.load_input(mock_context)

        if not draft_content:
            raise ValueError(f"Empty draft content loaded for {link_task_id}")

        context.log.info(f"Successfully loaded draft content: {len(draft_content)} characters from {link_task_id}")

    except FileNotFoundError as e:
        # Enhanced error with more debugging context
        available_files = list(draft_io_manager.base_path.glob("*.txt")) if draft_io_manager.base_path.exists() else []
        available_partitions = [f.stem for f in available_files[:10]]  # Show first 10

        context.log.error(f"Draft response not found for partition {link_task_id}")
        context.log.error(f"Expected path: {expected_path}")
        context.log.error(f"Base directory exists: {draft_io_manager.base_path.exists()}")
        context.log.error(f"Available partitions (first 10): {available_partitions}")

        raise Failure(
            description=f"Missing draft response required for evaluation task '{task_id}' (FK: {link_task_id})",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "link_task_id": MetadataValue.text(link_task_id),
                "expected_file_path": MetadataValue.path(str(expected_path)),
                "base_directory_exists": MetadataValue.text(str(draft_io_manager.base_path.exists())),
                "available_partitions_sample": MetadataValue.text(str(available_partitions)),
                "total_available_files": MetadataValue.int(len(available_files)),
                "fk_relationship": MetadataValue.text(f"evaluation_task '{task_id}' references link_task '{link_task_id}'"),
                "resolution_1": MetadataValue.text(f"Check if links_response was materialized for partition: {link_task_id}"),
                "resolution_2": MetadataValue.text(f"Run: dagster asset materialize --select links_response --partition {link_task_id}"),
                "resolution_3": MetadataValue.text("Or materialize all drafts: dagster asset materialize --select links_response"),
                "resolution_4": MetadataValue.text("Verify link_generation_tasks asset was materialized before evaluation_tasks"),
                "original_error": MetadataValue.text(str(e))
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
    
    # Generate evaluation prompt using existing logic
    response_dict = {link_task_id: draft_content}
    eval_prompts_dict = generate_evaluation_prompts(
        response_dict,
        evaluation_tasks.iloc[[task_row.name]],  # Single task
        evaluation_templates_dict
    )
    eval_prompt = eval_prompts_dict[task_id]
    context.log.info(f"Generated evaluation prompt for task {task_id}, using draft content from {link_task_id}")
    
    # Add output metadata for debugging and monitoring
    context.add_output_metadata({
        "evaluation_task_id": MetadataValue.text(task_id),
        "source_stage": MetadataValue.text("draft"),
        "link_task_id_used": MetadataValue.text(link_task_id),
        "draft_content_length": MetadataValue.int(len(draft_content)),
        "evaluation_prompt_length": MetadataValue.int(len(eval_prompt)),
        "fk_relationship": MetadataValue.text(f"eval:{task_id} -> link:{link_task_id}"),
        "io_manager_base_path": MetadataValue.path(str(draft_io_manager.base_path)),
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
