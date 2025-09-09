from dagster import DynamicPartitionsDefinition

# Gen-id keyed dynamic partitions (one per generation)
draft_gens_partitions = DynamicPartitionsDefinition(name="draft_gens")
essay_gens_partitions = DynamicPartitionsDefinition(name="essay_gens")
evaluation_gens_partitions = DynamicPartitionsDefinition(name="evaluation_gens")
