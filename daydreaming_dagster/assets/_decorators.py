from __future__ import annotations

import dagster as dg
from ._error_boundary import with_asset_error_boundary


def asset_with_boundary(*, stage: str, name: str | None = None, group_name: str | None = None, **asset_kwargs):
    """Compose @dagster.asset with a minimal error boundary.

    Usage mirrors @dagster.asset; pass through common kwargs such as
    partitions_def, io_manager_key, required_resource_keys, deps, etc.
    """

    def deco(fn):
        wrapped = with_asset_error_boundary(stage=stage)(fn)
        return dg.asset(name=name, group_name=group_name, **asset_kwargs)(wrapped)

    return deco
