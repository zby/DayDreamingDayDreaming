from dagster import DynamicPartitionsDefinition

# Create dynamic partition definitions
generation_tasks_partitions = DynamicPartitionsDefinition(name="generation_tasks")
evaluation_tasks_partitions = DynamicPartitionsDefinition(name="evaluation_tasks")

# Split evaluation partitions by tier for cleaner UI selection
evaluation_tasks_free_partitions = DynamicPartitionsDefinition(name="evaluation_tasks_free")
evaluation_tasks_paid_partitions = DynamicPartitionsDefinition(name="evaluation_tasks_paid")