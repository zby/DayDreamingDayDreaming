from __future__ import annotations

from dataclasses import dataclass

import pytest
from dagster import AssetKey, build_asset_context

from daydreaming_dagster.assets.group_draft import draft_parsed, draft_prompt, draft_raw
from daydreaming_dagster.assets.group_essay import essay_parsed, essay_prompt, essay_raw
from daydreaming_dagster.assets.group_evaluation import (
    evaluation_parsed,
    evaluation_prompt,
    evaluation_raw,
)
from daydreaming_dagster.assets.partitions import (
    draft_gens_partitions,
    essay_gens_partitions,
    evaluation_gens_partitions,
)
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.assets.raw_data import EVALUATION_TEMPLATES_KEY


@dataclass(frozen=True)
class _StageAssetExpectation:
    asset_def: object
    group_name: str
    partitions_def: object
    io_manager_key: str
    resources: set[str]
    deps: set[AssetKey]


_PROMPT_CASES = [
    _StageAssetExpectation(
        draft_prompt,
        "generation_draft",
        draft_gens_partitions,
        "draft_prompt_io_manager",
        {"data_root", "experiment_config", "draft_prompt_io_manager"},
        {AssetKey("content_combinations")},
    ),
    _StageAssetExpectation(
        essay_prompt,
        "generation_essays",
        essay_gens_partitions,
        "essay_prompt_io_manager",
        {"data_root", "experiment_config", "essay_prompt_io_manager"},
        set(),
    ),
    _StageAssetExpectation(
        evaluation_prompt,
        "evaluation",
        evaluation_gens_partitions,
        "evaluation_prompt_io_manager",
        {"data_root", "experiment_config", "evaluation_prompt_io_manager"},
        {EVALUATION_TEMPLATES_KEY},
    ),
]

_RAW_CASES = [
    _StageAssetExpectation(
        draft_raw,
        "generation_draft",
        draft_gens_partitions,
        "in_memory_io_manager",
        {"data_root", "experiment_config", "openrouter_client", "in_memory_io_manager"},
        {AssetKey("draft_prompt")},
    ),
    _StageAssetExpectation(
        essay_raw,
        "generation_essays",
        essay_gens_partitions,
        "in_memory_io_manager",
        {"data_root", "experiment_config", "openrouter_client", "in_memory_io_manager"},
        {AssetKey("essay_prompt")},
    ),
    _StageAssetExpectation(
        evaluation_raw,
        "evaluation",
        evaluation_gens_partitions,
        "in_memory_io_manager",
        {"data_root", "experiment_config", "openrouter_client", "in_memory_io_manager"},
        {AssetKey("evaluation_prompt")},
    ),
]

_PARSED_CASES = [
    _StageAssetExpectation(
        draft_parsed,
        "generation_draft",
        draft_gens_partitions,
        "in_memory_io_manager",
        {"data_root", "experiment_config", "in_memory_io_manager"},
        {AssetKey("draft_raw")},
    ),
    _StageAssetExpectation(
        essay_parsed,
        "generation_essays",
        essay_gens_partitions,
        "in_memory_io_manager",
        {"data_root", "experiment_config", "in_memory_io_manager"},
        {AssetKey("essay_raw")},
    ),
    _StageAssetExpectation(
        evaluation_parsed,
        "evaluation",
        evaluation_gens_partitions,
        "in_memory_io_manager",
        {"data_root", "experiment_config", "in_memory_io_manager"},
        {AssetKey("evaluation_raw")},
    ),
]


@pytest.mark.parametrize("case", _PROMPT_CASES + _RAW_CASES + _PARSED_CASES)
def test_stage_assets_share_expected_metadata(case: _StageAssetExpectation) -> None:
    asset_key = case.asset_def.key
    assert case.asset_def.group_names_by_key[asset_key] == case.group_name
    assert case.asset_def.partitions_def is case.partitions_def
    assert case.asset_def.get_io_manager_key_for_asset_key(asset_key) == case.io_manager_key
    assert case.asset_def.required_resource_keys == case.resources
    assert case.asset_def.asset_deps[asset_key] == case.deps


def test_evaluation_assets_skip_when_parsed_exists(tmp_path) -> None:
    gen_id = "G-001"
    data_layer = GensDataLayer.from_root(tmp_path)
    data_layer.reserve_generation("evaluation", gen_id, create=True)
    data_layer.write_input("evaluation", gen_id, "cached-input")
    data_layer.write_raw("evaluation", gen_id, "cached-raw")
    data_layer.write_parsed("evaluation", gen_id, "cached-parsed")

    @dataclass
    class _ExperimentConfig:
        stage_config: dict[str, object] = None

    def _context():
        return build_asset_context(
            partition_key=gen_id,
            resources={
                "data_root": str(tmp_path),
                "experiment_config": _ExperimentConfig(stage_config={}),
                "openrouter_client": object(),
            },
        )

    assert evaluation_prompt(_context()) == "cached-input"
    assert evaluation_raw(_context(), evaluation_prompt="cached-input") == "cached-raw"
    assert evaluation_parsed(_context()) == "cached-parsed"
