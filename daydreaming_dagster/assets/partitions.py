from dagster import DynamicPartitionsDefinition

# Create dynamic partition definitions
generation_tasks_partitions = DynamicPartitionsDefinition(name="generation_tasks")
evaluation_tasks_partitions = DynamicPartitionsDefinition(name="evaluation_tasks")

# Two-phase architecture
essay_tasks_partitions = DynamicPartitionsDefinition(name="essay_tasks")
draft_tasks_partitions = DynamicPartitionsDefinition(name="draft_tasks")
