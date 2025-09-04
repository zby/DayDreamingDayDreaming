import pytest
from dagster import MultiPartitionsDefinition, MultiToSingleDimensionPartitionMapping

from daydreaming_dagster.assets.partitions import (
    generation_tasks_partitions,
    evaluation_tasks_partitions,
)
try:
    # Optional import: only present once multi-partitions are implemented
    from daydreaming_dagster.assets.partitions import evaluation_multi_partitions  # type: ignore
    HAS_MULTI = True
except Exception:
    HAS_MULTI = False
from daydreaming_dagster.assets.groups.group_evaluation import evaluation_prompt
from daydreaming_dagster.assets.groups.group_generation_essays import essay_response as gen_response_asset


def test_evaluation_multi_partitions_definition():
    if not HAS_MULTI:
        pytest.skip("evaluation_multi_partitions not implemented yet")
    assert isinstance(evaluation_multi_partitions, MultiPartitionsDefinition)
    gen_def = evaluation_multi_partitions.get_partitions_def_for_dimension("generation_tasks")
    eval_def = evaluation_multi_partitions.get_partitions_def_for_dimension("evaluation_tasks")
    assert gen_def is generation_tasks_partitions
    assert eval_def is evaluation_tasks_partitions


def test_evaluation_prompt_uses_multi_partitions_and_mapping():
    if not HAS_MULTI:
        pytest.skip("evaluation_multi_partitions not implemented yet")
    # Partitions are multi-dimensional
    assert evaluation_prompt.partitions_def is evaluation_multi_partitions

    # The asset should declare an input mapping from the generation dimension
    # Access partition mapping via AssetsDefinition API
    # Query mapping for the upstream dependency asset key
    gen_key = next(iter(gen_response_asset.keys))
    mapping = evaluation_prompt.get_partition_mapping_for_dep(dep_key=gen_key)
    assert isinstance(
        mapping, MultiToSingleDimensionPartitionMapping
    ), "Input should use MultiToSingleDimensionPartitionMapping"
    assert getattr(mapping, "partition_dimension_name", None) == "generation_tasks"

    # Ensure it points at the generation dimension
    assert (
        getattr(mapping, "partition_dimension_name", None) == "generation_tasks"
    ), "Mapping should be on 'generation_tasks' dimension"
