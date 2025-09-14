from __future__ import annotations

from dagster import MetadataValue
from ._decorators import asset_with_boundary
from pathlib import Path
import pandas as pd

from .partitions import (
    draft_gens_partitions,
    essay_gens_partitions,
    evaluation_gens_partitions,
)


def _desired_gen_ids(csv_path: Path) -> set[str]:
    try:
        if not csv_path.exists():
            return set()
        df = pd.read_csv(csv_path)
        if "gen_id" not in df.columns:
            return set()
        return set(df["gen_id"].astype(str).dropna().tolist())
    except Exception:
        return set()


@asset_with_boundary(
    stage="maintenance",
    group_name="cohort",
    description="Remove ALL registered dynamic partitions for draft/essay/evaluation (registry only).",
)
def prune_dynamic_partitions(context) -> dict:
    """Delete all dynamic partitions for draft/essay/evaluation.

    - Operates only on the Dagster partition registry, not on filesystem data.
    - Intended to run BEFORE regenerating task CSVs, so newly created tasks
      can register a clean set of partitions.
    """
    registry = context.instance
    stats: dict[str, dict] = {}

    for name, part_def in (
        ("draft", draft_gens_partitions),
        ("essay", essay_gens_partitions),
        ("evaluation", evaluation_gens_partitions),
    ):
        current = sorted(list(registry.get_dynamic_partitions(part_def.name)))
        deleted_count = 0
        for key in current:
            try:
                # DagsterInstance supports single-key deletion
                registry.delete_dynamic_partition(part_def.name, key)
                deleted_count += 1
            except Exception:
                # Continue best-effort
                pass
        stats[name] = {
            "deleted": deleted_count,
        }

    context.add_output_metadata(
        {f"{k}_deleted": MetadataValue.int(v["deleted"]) for k, v in stats.items()}
    )
    return stats
