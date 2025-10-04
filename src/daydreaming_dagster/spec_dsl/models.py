"""Data structures backing the experiment DSL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class AxisSpec:
    """Declarative axis listing the raw levels prior to rule transforms."""

    name: str
    levels: Sequence[Any]


@dataclass(frozen=True)
class ExperimentSpec:
    """Top-level spec bundle loaded from disk."""

    axes: Mapping[str, AxisSpec]
    rules: Sequence[Mapping[str, Any]]
    output: Mapping[str, Any]
