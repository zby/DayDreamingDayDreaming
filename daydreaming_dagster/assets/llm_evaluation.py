from dagster import asset, Failure, MetadataValue
from .partitions import evaluation_tasks_partitions
from ..utils.nodes_standalone import generate_evaluation_prompts


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="llm_evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    deps=["evaluation_tasks", "generation_response"],  # Declare dependency for UI/run order; still load via IO manager
    required_resource_keys={"generation_response_io_manager"},
    pool="llm_api"  # Pool-based concurrency control
)
def evaluation_prompt(context, evaluation_tasks, evaluation_templates) -> str:
    """
    Generate one evaluation prompt per partition and save as individual file.
    Each prompt is cached as a separate file for debugging.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition with debugging
    matching_tasks = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id]
    if len(matching_tasks) == 0:
        available_tasks = evaluation_tasks["evaluation_task_id"].tolist()[:5]  # Show first 5
        context.log.error(f"Evaluation task '{task_id}' not found in evaluation_tasks DataFrame")
        raise Failure(
            description=f"Evaluation task '{task_id}' not found in task database",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "available_tasks_sample": MetadataValue.text(str(available_tasks)),
                "total_tasks": MetadataValue.int(len(evaluation_tasks)),
                "resolution_1": MetadataValue.text("Check if evaluation_tasks asset was materialized recently"),
                "resolution_2": MetadataValue.text("Run: dagster asset materialize --select evaluation_tasks"),
                "resolution_3": MetadataValue.text("Verify partitions are up to date - stale partitions may reference old task IDs"),
                "resolution_4": MetadataValue.text("Ensure generation_tasks was materialized first (evaluation depends on it)")
            }
        )
    
    task_row = matching_tasks.iloc[0]
    generation_task_id = task_row["generation_task_id"]
    
    # Load the generation response from the correct partition using I/O manager
    # Get the generation response I/O manager from resources to respect test vs production paths
    gen_response_io_manager = context.resources.generation_response_io_manager
    
    # Create a mock context for the generation partition
    class MockLoadContext:
        def __init__(self, partition_key):
            self.partition_key = partition_key
    
    mock_context = MockLoadContext(generation_task_id)
    try:
        generation_response = gen_response_io_manager.load_input(mock_context)
    except FileNotFoundError as e:
        context.log.error(f"Generation response not found for partition {generation_task_id}")
        raise Failure(
            description=f"Missing generation response required for evaluation task '{task_id}'",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "generation_task_id": MetadataValue.text(generation_task_id),
                "expected_file_path": MetadataValue.path(f"{gen_response_io_manager.base_path}/{generation_task_id}.txt"),
                "resolution_1": MetadataValue.text(f"Check if generation_response was materialized for partition: {generation_task_id}"),
                "resolution_2": MetadataValue.text(f"Run: dagster asset materialize --select generation_response --partition {generation_task_id}"),
                "resolution_3": MetadataValue.text("Or materialize all generation responses: dagster asset materialize --select generation_response"),
                "original_error": MetadataValue.text(str(e))
            }
        ) from e
    
    # Convert DataFrame to dict format expected by generate_evaluation_prompts
    evaluation_templates_dict = evaluation_templates.set_index('template_id')['content'].to_dict()
    
    # Generate evaluation prompt using existing logic
    response_dict = {generation_task_id: generation_response}
    eval_prompts_dict = generate_evaluation_prompts(
        response_dict,
        evaluation_tasks.iloc[[task_row.name]],  # Single task
        evaluation_templates_dict
    )
    
    eval_prompt = eval_prompts_dict[task_id]
    context.log.info(f"Generated evaluation prompt for task {task_id}, using generation response from {generation_task_id}")
    return eval_prompt


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="llm_evaluation",
    io_manager_key="evaluation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["generation_tasks"],  # Remove evaluation_models dependency
    pool="llm_api"  # Pool-based concurrency control
)
def evaluation_response(context, evaluation_prompt, evaluation_tasks) -> str:
    """Generate one evaluation response per partition."""
    task_id = context.partition_key
    
    # Debug: Check if task_id exists in evaluation_tasks
    matching_tasks = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id]
    if len(matching_tasks) == 0:
        available_tasks = evaluation_tasks["evaluation_task_id"].tolist()[:5]  # Show first 5
        context.log.error(f"Evaluation task '{task_id}' not found in evaluation_tasks DataFrame")
        raise Failure(
            description=f"Evaluation task '{task_id}' not found in task database",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "available_tasks_sample": MetadataValue.text(str(available_tasks)),
                "total_tasks": MetadataValue.int(len(evaluation_tasks)),
                "resolution_1": MetadataValue.text("Check if evaluation_tasks asset was materialized recently"),
                "resolution_2": MetadataValue.text("Run: dagster asset materialize --select evaluation_tasks"),
                "resolution_3": MetadataValue.text("Verify partitions are up to date - stale partitions may reference old task IDs"),
                "resolution_4": MetadataValue.text("Ensure evaluation_prompt was materialized first (evaluation_response depends on it)")
            }
        )
    
    task_row = matching_tasks.iloc[0]
    
    # Use the model name directly from the task
    model_name = task_row["evaluation_model_name"]
    
    # The prompt is already generated and saved as a file by evaluation_prompt asset
    eval_prompt = evaluation_prompt
    
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(eval_prompt, model=model_name)
    
    context.log.info(f"Generated evaluation response for task {task_id} using model {model_name}")
    return response