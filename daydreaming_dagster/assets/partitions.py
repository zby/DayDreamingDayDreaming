from dagster import DynamicPartitionsDefinition

# Doc-id keyed dynamic partitions (one per document)
draft_docs_partitions = DynamicPartitionsDefinition(name="draft_docs")
essay_docs_partitions = DynamicPartitionsDefinition(name="essay_docs")
evaluation_docs_partitions = DynamicPartitionsDefinition(name="evaluation_docs")
