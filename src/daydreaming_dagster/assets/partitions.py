from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

from dagster import DynamicPartitionsDefinition, StaticPartitionsDefinition


def _discover_cohort_partition_keys(
    *, env_var: str = "DAYDREAMING_DATA_ROOT", default_root: str = "data"
) -> Sequence[str]:
    root = Path(os.environ.get(env_var, default_root)) / "cohorts"
    if not root.exists():
        return ()
    keys: list[str] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        spec_path = child / "spec" / "config.yaml"
        if spec_path.exists():
            keys.append(child.name)
    # Deduplicate while preserving order
    seen: set[str] = set()
    ordered: list[str] = []
    for key in sorted(keys):
        if key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return tuple(ordered)


def build_cohort_spec_partitions() -> StaticPartitionsDefinition:
    return StaticPartitionsDefinition(list(_discover_cohort_partition_keys()))


cohort_spec_partitions = build_cohort_spec_partitions()

# Gen-id keyed dynamic partitions (one per generation)
draft_gens_partitions = DynamicPartitionsDefinition(name="draft_gens")
essay_gens_partitions = DynamicPartitionsDefinition(name="essay_gens")
evaluation_gens_partitions = DynamicPartitionsDefinition(name="evaluation_gens")

# Cohort-scoped report partitions (one per cohort)
cohort_reports_partitions = DynamicPartitionsDefinition(name="cohort_reports")

__all__ = [
    "build_cohort_spec_partitions",
    "cohort_spec_partitions",
    "draft_gens_partitions",
    "essay_gens_partitions",
    "evaluation_gens_partitions",
    "cohort_reports_partitions",
]
