from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any


class Err(Enum):
    DATA_MISSING = auto()
    INVALID_CONFIG = auto()
    MISSING_TEMPLATE = auto()
    INVALID_MEMBERSHIP = auto()
    IO_ERROR = auto()
    PARSER_FAILURE = auto()
    UNKNOWN = auto()


@dataclass(eq=False)
class DDError(Exception):
    code: Err
    ctx: dict[str, Any] | None = None
    cause: Exception | None = None

    def __post_init__(self) -> None:
        if self.ctx is None:
            self.ctx = {}
        if self.cause is not None:
            self.__cause__ = self.cause
        super().__init__(self.code.name)

    def __str__(self) -> str:  # pragma: no cover - convenience for logging
        parts = [self.code.name]
        if self.ctx:
            parts.append(str(self.ctx))
        return ": ".join(parts)
