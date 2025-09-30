from __future__ import annotations

from functools import wraps
from typing import Any

from dagster import Failure, MetadataValue, get_dagster_logger

from daydreaming_dagster.utils.errors import DDError


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
            except DDError as err:
                logger = get_dagster_logger()
                description = f"[{stage}] {err}"
                logger.error(description)
                metadata: dict[str, MetadataValue] = {
                    "error_code": MetadataValue.text(err.code.name)
                }
                if err.ctx:
                    metadata.update(_ctx_to_metadata(err.ctx))
                raise Failure(description=description, metadata=metadata) from err
            except Exception as exc:  # pragma: no cover - safety net
                description = f"[{stage}] {exc}"
                get_dagster_logger().error(description)
                raise Failure(description=description) from exc

        return wrapper

    return deco


def _ctx_to_metadata(ctx: dict[str, Any]) -> dict[str, MetadataValue]:
    if not ctx:
        return {}
    try:
        return {"error_ctx": MetadataValue.json(ctx)}
    except TypeError:
        return {"error_ctx_repr": MetadataValue.text(repr(ctx))}
