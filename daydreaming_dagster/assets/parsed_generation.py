from dagster import asset, AutoMaterializePolicy
from .partitions import generation_tasks_partitions


@asset(
    partitions_def=generation_tasks_partitions,
    group_name="llm_generation",
    io_manager_key="parsed_generation_io_manager",
    description="Backward compatibility layer for parsed generation responses. "
                "This asset provides a direct passthrough from essay_response to "
                "the legacy parsed_generation_responses format.",
    compute_kind="python",
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def parsed_generation_responses(context, essay_response: str) -> str:
    """
    Backward compatibility layer for parsed generation responses.
    
    This asset provides a direct passthrough from essay_response to the legacy
    parsed_generation_responses format, ensuring existing downstream assets 
    continue to work unchanged.
    
    Args:
        context: Dagster asset context
        essay_response: Essay text from two-phase generation
        
    Returns:
        str: Essay text content for downstream consumption
    """
    task_id = context.partition_key
    
    context.log.info(f"Providing parsed generation response for task {task_id} directly from essay_response")
    context.add_output_metadata({
        "essay_word_count": len(essay_response.split()) if essay_response else 0,
        "source": "two_phase_generation.essay_response"
    })
    
    return essay_response