from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import evaluation_tasks_partitions
from ..utils.nodes_standalone import generate_evaluation_prompts


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="llm_evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"parsing_results_io_manager"},
)
def evaluation_prompt(context, evaluation_tasks, evaluation_templates, parsed_generation_responses) -> str:
    """
    Generate one evaluation prompt per partition using FK-based data loading.
    
    This asset demonstrates the "Manual IO with Foreign Keys" pattern documented
    in docs/evaluation_asset_architecture.md. Key aspects:
    
    1. FK Relationship: Uses generation_task_id from evaluation_tasks to load
       the corresponding generation_response from a different partition
    
    2. MockLoadContext Pattern: Creates a temporary context to interface with
       the IO manager for cross-partition data loading
    
    3. Intentional Manual IO: Bypasses normal Dagster input/output pattern
       to handle complex partition relationships with foreign keys
    
    Data Flow:
    evaluation_task (partition: eval_001) 
    -> reads generation_task_id FK 
    -> loads generation_response (partition: gen_123) via IO manager
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
        Failure: If referenced generation_task_id is invalid/empty  
        Failure: If generation_response file not found (FK broken)
        
    Resources Required:
        - generation_response_io_manager: To load upstream generation responses
        
    Dependencies:
        - evaluation_tasks: Must be materialized (provides FK relationships)
        - generation_response: Must be materialized for referenced partitions
        
    See Also:
        - docs/evaluation_asset_architecture.md: Detailed architecture explanation
        - plans/pass_generation_response_via_pipeline_fk.md: Alternative approaches
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
    
    # Log the foreign key relationship for debugging
    context.log.info(f"Loading generation response for FK relationship: {task_id} -> {generation_task_id}")
    
    # Load the parsed generation response from the correct partition using I/O manager
    # This uses the MockLoadContext pattern to load data from a different partition
    # See docs/evaluation_asset_architecture.md for detailed explanation
    parsing_io_manager = context.resources.parsing_results_io_manager
    
    # Validate that the referenced generation_task_id looks valid
    if not generation_task_id or generation_task_id.strip() == "":
        raise Failure(
            description=f"Invalid generation_task_id referenced by evaluation task '{task_id}'",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "generation_task_id": MetadataValue.text(str(generation_task_id)),
                "resolution_1": MetadataValue.text("Check evaluation_tasks CSV for data corruption"),
                "resolution_2": MetadataValue.text("Re-materialize evaluation_tasks asset"),
                "fk_validation_failed": MetadataValue.text("generation_task_id is empty or invalid")
            }
        )
    
    # Create a mock context for the generation partition (documented pattern)
    # This allows us to load data from a different partition than the current asset's partition
    # The IO manager only needs the partition_key attribute to locate the correct file
    class MockLoadContext:
        """Minimal context object for cross-partition IO manager calls.
        
        This pattern is documented in docs/evaluation_asset_architecture.md as an
        intentional way to load data from foreign key referenced partitions.
        """
        def __init__(self, partition_key):
            self.partition_key = partition_key
    
    mock_context = MockLoadContext(generation_task_id)
    
    # Check if the file exists before attempting to load (better error context)
    expected_path = parsing_io_manager.base_path / f"{generation_task_id}.csv"
    
    try:
        # Load the parsed generation response from the parsing results
        parsed_response = parsing_io_manager.load_input(mock_context)
        
        # Extract essay content from the parsed response
        if isinstance(parsed_response, dict) and 'essay_content' in parsed_response:
            essay_content = parsed_response['essay_content']
        elif isinstance(parsed_response, str):
            # If it's a string, assume it's the essay content directly
            essay_content = parsed_response
        else:
            # Fallback: try to load the original generation response
            gen_response_file = Path("data/3_generation/generation_responses") / f"{generation_task_id}.txt"
            if gen_response_file.exists():
                context.log.warning(f"Using fallback generation response for {generation_task_id}")
                essay_content = gen_response_file.read_text()
            else:
                raise FileNotFoundError(f"Neither parsed nor original generation response found for: {generation_task_id}")
        
        context.log.info(f"Successfully loaded essay content: {len(essay_content)} characters from {generation_task_id}")
        
    except FileNotFoundError as e:
        # Enhanced error with more debugging context
        available_files = list(parsing_io_manager.base_path.glob("*.csv")) if parsing_io_manager.base_path.exists() else []
        available_partitions = [f.stem for f in available_files[:10]]  # Show first 10
        
        context.log.error(f"Parsed generation response not found for partition {generation_task_id}")
        context.log.error(f"Expected path: {expected_path}")
        context.log.error(f"Base directory exists: {parsing_io_manager.base_path.exists()}")
        context.log.error(f"Available partitions (first 10): {available_partitions}")
        
        raise Failure(
            description=f"Missing parsed generation response required for evaluation task '{task_id}' (FK: {generation_task_id})",
            metadata={
                "evaluation_task_id": MetadataValue.text(task_id),
                "generation_task_id": MetadataValue.text(generation_task_id),
                "expected_file_path": MetadataValue.path(str(expected_path)),
                "base_directory_exists": MetadataValue.text(str(parsing_io_manager.base_path.exists())),
                "available_partitions_sample": MetadataValue.text(str(available_partitions)),
                "total_available_files": MetadataValue.int(len(available_files)),
                "fk_relationship": MetadataValue.text(f"evaluation_task '{task_id}' references generation_task '{generation_task_id}'"),
                "resolution_1": MetadataValue.text(f"Check if parsed_generation_responses was materialized for partition: {generation_task_id}"),
                "resolution_2": MetadataValue.text(f"Run: dagster asset materialize --select parsed_generation_responses --partition {generation_task_id}"),
                "resolution_3": MetadataValue.text("Or materialize all parsing results: dagster asset materialize --select parsed_generation_responses"),
                "resolution_4": MetadataValue.text("Verify generation_tasks asset was materialized before evaluation_tasks"),
                "original_error": MetadataValue.text(str(e))
            }
        ) from e
    
    # Convert DataFrame to dict format expected by generate_evaluation_prompts
    evaluation_templates_dict = evaluation_templates.set_index('template_id')['content'].to_dict()
    
    # Generate evaluation prompt using existing logic
    response_dict = {generation_task_id: essay_content}
    eval_prompts_dict = generate_evaluation_prompts(
        response_dict,
        evaluation_tasks.iloc[[task_row.name]],  # Single task
        evaluation_templates_dict
    )
    
    eval_prompt = eval_prompts_dict[task_id]
    context.log.info(f"Generated evaluation prompt for task {task_id}, using essay content from {generation_task_id}")
    
    # Add output metadata for debugging and monitoring
    context.add_output_metadata({
        "evaluation_task_id": MetadataValue.text(task_id),
        "generation_task_id_used": MetadataValue.text(generation_task_id),
        "essay_content_length": MetadataValue.int(len(essay_content)),
        "evaluation_prompt_length": MetadataValue.int(len(eval_prompt)),
        "fk_relationship": MetadataValue.text(f"eval:{task_id} -> gen:{generation_task_id}"),
        "io_manager_base_path": MetadataValue.path(str(parsing_io_manager.base_path)),
        "template_used": MetadataValue.text(task_row["evaluation_template"]),
        "model_planned": MetadataValue.text(task_row["evaluation_model_name"])
    })
    
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