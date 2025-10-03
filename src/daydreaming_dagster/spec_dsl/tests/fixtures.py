from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from daydreaming_dagster.spec_dsl import ExperimentSpec
from daydreaming_dagster.spec_dsl.loader import parse_spec_mapping


@dataclass
class ExperimentSpecFactory:
    """Utility for building in-memory specs anchored to a temp directory."""

    base_dir: Path
    source_name: str = "spec.json"

    def parse(self, payload: Mapping[str, Any]) -> ExperimentSpec:
        return parse_spec_mapping(
            deepcopy(payload),
            source=self.base_dir / self.source_name,
            base_dir=self.base_dir,
        )
