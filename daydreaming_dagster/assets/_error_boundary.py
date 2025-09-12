from __future__ import annotations

from functools import wraps
from dagster import Failure


def with_asset_error_boundary(stage: str):
    """Wrap an asset so unexpected errors surface as dagster.Failure.

    This decorator preserves the wrapped function's signature via functools.wraps,
    which keeps Dagster input/resource binding unchanged.
    """

    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Failure:
                # Already a Dagster Failure — propagate as-is
                raise
            except Exception as e:  # noqa: BLE001 - convert all unexpected errors
                raise Failure(description=f"[{stage}] {e}") from e

        return wrapper

    return deco
