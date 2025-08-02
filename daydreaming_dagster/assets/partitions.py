from dagster import asset, DynamicPartitionsDefinition
from typing import Tuple
import pandas as pd

# Create dynamic partition definitions
generation_tasks_partitions = DynamicPartitionsDefinition(name="generation_tasks")
evaluation_tasks_partitions = DynamicPartitionsDefinition(name="evaluation_tasks")

@asset(group_name="daydreaming_experiment")
def task_definitions(
    context,
    generation_tasks: pd.DataFrame,
    evaluation_tasks: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Register task definitions as dynamic partitions after tasks are saved as CSV files.
    This enables downstream LLM response assets to be cached per task.
    """
    
    # Add partitions dynamically based on actual task data
    context.instance.add_dynamic_partitions(
        generation_tasks_partitions.name,
        generation_tasks["generation_task_id"].tolist()
    )
    context.instance.add_dynamic_partitions(
        evaluation_tasks_partitions.name,
        evaluation_tasks["evaluation_task_id"].tolist()
    )
    
    context.log.info(f"Created {len(generation_tasks)} generation task partitions")
    context.log.info(f"Created {len(evaluation_tasks)} evaluation task partitions")
    
    return generation_tasks, evaluation_tasks