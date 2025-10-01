"""Error types for the spec DSL module.

Keeping this module self-contained lets us evolve the error surface
without touching the global `DDError` contract until integration time.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Mapping


class SpecDslErrorCode(Enum):
    """Module-local error codes.

    We stick to a single failure mode for now until the compiler grows
    richer validation branches.
    """

    INVALID_SPEC = auto()


@dataclass(eq=False)
class SpecDslError(Exception):
    """Structured error raised by the DSL loader/compiler."""

    code: SpecDslErrorCode
    ctx: Mapping[str, Any] | None = None

    def __str__(self) -> str:  # pragma: no cover - convenience only
        if not self.ctx:
            return self.code.name
        parts = ", ".join(f"{k}={v!r}" for k, v in self.ctx.items())
        return f"{self.code.name}: {parts}"

