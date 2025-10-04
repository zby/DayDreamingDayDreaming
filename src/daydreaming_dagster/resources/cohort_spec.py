from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from dagster import ConfigurableResource
from pydantic import PrivateAttr

from daydreaming_dagster.cohorts import CohortDefinition, compile_cohort_definition
from daydreaming_dagster.spec_dsl import ExperimentSpec, load_spec, parse_spec_mapping


class CohortSpecResource(ConfigurableResource):
    """Resource that loads and caches experiment specs for cohort assets."""

    default_seed: int | None = None
    cache_specs: bool = True

    _spec_cache: dict[str, ExperimentSpec] = PrivateAttr(default_factory=dict)

    def _normalize_path(self, candidate: Path) -> Path:
        if candidate.is_dir():
            candidate = candidate / "config.yaml"
        return candidate

    def _cache_key(self, path: Path) -> str:
        try:
            return str(path.resolve())
        except FileNotFoundError:
            # Resolve may fail for in-memory tests; fall back to absolute-like string.
            return str(path)

    def get_spec(self, path: str | Path) -> ExperimentSpec:
        spec_path = self._normalize_path(Path(path))
        key = self._cache_key(spec_path)
        if self.cache_specs and key in self._spec_cache:
            return self._spec_cache[key]

        spec = load_spec(spec_path)
        if self.cache_specs:
            self._spec_cache[key] = spec
        return spec

    def parse_mapping(
        self,
        mapping: Mapping[str, Any],
        *,
        source: str | Path | None = None,
        base_dir: str | Path | None = None,
    ) -> ExperimentSpec:
        source_path = Path(source) if source is not None else None
        resolved_base = Path(base_dir) if base_dir is not None else (
            source_path.parent if source_path is not None else Path(".")
        )
        spec = parse_spec_mapping(
            deepcopy(mapping),
            source=source_path,
            base_dir=resolved_base,
        )
        if self.cache_specs and source_path is not None:
            self._spec_cache[self._cache_key(source_path)] = spec
        return spec

    def compile_definition(
        self,
        *,
        spec: ExperimentSpec | None = None,
        path: str | Path | None = None,
        seed: int | None = None,
    ) -> CohortDefinition:
        if spec is None:
            if path is None:
                raise ValueError("Either 'spec' or 'path' must be provided to compile a definition")
            spec = self.get_spec(path)
        effective_seed = seed if seed is not None else self.default_seed
        return compile_cohort_definition(spec, seed=effective_seed)


__all__ = [
    "CohortSpecResource",
]
