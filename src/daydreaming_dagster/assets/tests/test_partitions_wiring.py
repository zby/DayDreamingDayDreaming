from dagster import DynamicPartitionsDefinition

from daydreaming_dagster.assets.partitions import (
    draft_gens_partitions,
    essay_gens_partitions,
    evaluation_gens_partitions,
)


def test_gen_id_partitions_exist_and_are_dynamic():
    assert isinstance(draft_gens_partitions, DynamicPartitionsDefinition)
    assert isinstance(essay_gens_partitions, DynamicPartitionsDefinition)
    assert isinstance(evaluation_gens_partitions, DynamicPartitionsDefinition)
