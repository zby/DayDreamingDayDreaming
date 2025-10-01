from __future__ import annotations

"""Smoke tests ensuring the stage registry drives Definitions wiring."""

from daydreaming_dagster import definitions as defs_module
from daydreaming_dagster.data_layer.paths import Paths


def _asset_names_from_registry() -> set[str]:
    registry = defs_module.STAGES
    names: set[str] = set()
    for entry in registry.values():
        for asset_def in entry.assets:
            names.update(key.path[-1] for key in asset_def.keys)
    return names


def test_stage_assets_and_resources_align_with_registry():
    defs = defs_module.defs
    expected_asset_names = _asset_names_from_registry()
    actual_asset_names = {
        key.path[-1]
        for key in defs.resolve_all_asset_keys()
        if key.path[-1] in expected_asset_names
    }
    assert actual_asset_names == expected_asset_names

    expected_resource_keys = {
        resource_key
        for entry in defs_module.STAGES.values()
        for resource_key in entry.resource_factories
    }
    actual_resource_keys = {
        name for name in defs.resources if name in expected_resource_keys
    }
    assert actual_resource_keys == expected_resource_keys

    rebuilt = defs_module.build_definitions(
        paths=Paths.from_str(defs.resources["data_root"])
    )
    assert set(rebuilt.resolve_all_asset_keys()) == set(defs.resolve_all_asset_keys())
    assert set(rebuilt.resources) == set(defs.resources)
    for key in expected_resource_keys:
        assert type(rebuilt.resources[key]) is type(defs.resources[key])
