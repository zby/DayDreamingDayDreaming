from dagster import DynamicPartitionsDefinition

# Create dynamic partition definitions
generation_tasks_partitions = DynamicPartitionsDefinition(name="generation_tasks")
evaluation_tasks_partitions = DynamicPartitionsDefinition(name="evaluation_tasks")

# New partitions for simplified two-phase architecture
link_tasks_partitions = DynamicPartitionsDefinition(name="link_tasks")
essay_tasks_partitions = DynamicPartitionsDefinition(name="essay_tasks")
