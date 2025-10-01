#!/usr/bin/env python3
"""Report generation ID length distribution for draft/essay/evaluation stages."""

from __future__ import annotations

import argparse
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from daydreaming_dagster.types import Stage


@dataclass
class StageLengthStats:
    stage: Stage
    counts: Counter[int]

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    @property
    def deterministic_count(self) -> int:
        # Deterministic IDs are 18 characters (prefix + 16-char hash)
        return self.counts.get(18, 0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path(os.environ.get("DAYDREAMING_DATA_ROOT", "data")),
        help="Root directory that contains gens/, defaults to $DAYDREAMING_DATA_ROOT or data/",
    )
    return parser.parse_args()


def collect_stage_lengths(data_root: Path, stage: Stage) -> StageLengthStats:
    stage_dir = data_root / "gens" / stage
    counts: Counter[int] = Counter()
    if not stage_dir.exists():
        return StageLengthStats(stage=stage, counts=counts)

    for entry in stage_dir.iterdir():
        if entry.is_dir():
            counts[len(entry.name)] += 1
    return StageLengthStats(stage=stage, counts=counts)


def format_stats(stats: StageLengthStats) -> str:
    lines: list[str] = []
    lines.append(f"Stage: {stats.stage}")
    lines.append(f"  Total generations: {stats.total}")
    if stats.total == 0:
        return "\n".join(lines)

    for length in sorted(stats.counts):
        count = stats.counts[length]
        pct = (count / stats.total) * 100
        lines.append(f"  Length {length}: {count} ({pct:.1f}%)")

    deterministic = stats.deterministic_count
    pct_det = (deterministic / stats.total) * 100
    lines.append(f"  Deterministic (18 chars): {deterministic} ({pct_det:.1f}%)")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    stats = [
        collect_stage_lengths(args.data_root, "draft"),
        collect_stage_lengths(args.data_root, "essay"),
        collect_stage_lengths(args.data_root, "evaluation"),
    ]

    for stage_stats in stats:
        print(format_stats(stage_stats))
        print()

    return 0


if __name__ == "__main__":  # pragma: no cover - manual utility
    raise SystemExit(main())
