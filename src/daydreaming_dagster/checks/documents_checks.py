from __future__ import annotations

from dagster import asset_check, AssetCheckResult, MetadataValue
from pathlib import Path

from daydreaming_dagster.assets.group_draft import draft_parsed as draft_parsed_asset
from daydreaming_dagster.assets.group_essay import essay_parsed as essay_parsed_asset
from daydreaming_dagster.assets.group_evaluation import (
    evaluation_parsed as evaluation_parsed_asset,
)
from ..data_layer.paths import Paths
from ..types import Stage


"""Asset checks for gens-store outputs.

These checks assert that parsed.txt exists under data/gens/<stage>/<gen_id>.
"""


def _get_pk(context):
    return getattr(context, "partition_key", None) or getattr(
        context, "asset_partition_key", None
    )


def _files_exist_check_impl(context, stage: Stage) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(
            passed=True,
            metadata={"skipped": MetadataValue.text("no partition context")},
        )
    paths = Paths.from_context(context)
    base = paths.generation_dir(stage, str(pk))
    ok = paths.parsed_path(stage, str(pk)).exists()
    return AssetCheckResult(
        passed=bool(ok), metadata={"gen_dir": MetadataValue.path(str(base))}
    )


def _make_files_exist_check(stage: Stage):
    def _check(context) -> AssetCheckResult:
        return _files_exist_check_impl(context, stage)

    return _check


# Register checks for each stage using the generic implementation and expose mapping for definitions
_ASSET_BY_STAGE = {
    "draft": draft_parsed_asset,
    "essay": essay_parsed_asset,
    "evaluation": evaluation_parsed_asset,
}


CHECKS_BY_STAGE = {
    stage: asset_check(asset=asset, required_resource_keys={"data_root"})(
        _make_files_exist_check(stage)
    )
    for stage, asset in _ASSET_BY_STAGE.items()
}

draft_files_exist_check = CHECKS_BY_STAGE["draft"]
essay_files_exist_check = CHECKS_BY_STAGE["essay"]
evaluation_files_exist_check = CHECKS_BY_STAGE["evaluation"]


__all__ = [
    "CHECKS_BY_STAGE",
    "draft_files_exist_check",
    "essay_files_exist_check",
    "evaluation_files_exist_check",
    "_files_exist_check_impl",
]


# DB row-present checks removed under filesystem-only design
