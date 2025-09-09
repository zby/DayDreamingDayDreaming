from dagster import DynamicPartitionsDefinition

from daydreaming_dagster.assets.partitions import (
    draft_docs_partitions,
    essay_docs_partitions,
    evaluation_docs_partitions,
)


def test_doc_id_partitions_exist_and_are_dynamic():
    assert isinstance(draft_docs_partitions, DynamicPartitionsDefinition)
    assert isinstance(essay_docs_partitions, DynamicPartitionsDefinition)
    assert isinstance(evaluation_docs_partitions, DynamicPartitionsDefinition)
