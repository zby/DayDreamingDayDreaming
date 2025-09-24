#!/usr/bin/env python3
"""CLI utility to migrate legacy 3_generation/4_evaluation data into gens store."""

from __future__ import annotations

import argparse
from pathlib import Path

from daydreaming_dagster.utils.legacy_gens_migration import (
    MigrationConfigError,
    migrate_legacy_gens,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--legacy-root",
        type=Path,
        default=Path("~/Downloads/data").expanduser(),
        help="Path to legacy data root containing 3_generation/4_evaluation directories.",
    )
    parser.add_argument(
        "--target-root",
        type=Path,
        default=Path("data"),
        help="Target data root where gens/ structure should be created.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report counts without writing files.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        stats = migrate_legacy_gens(
            legacy_root=args.legacy_root,
            target_root=args.target_root,
            dry_run=args.dry_run,
        )
    except MigrationConfigError as exc:
        parser.error(str(exc))
        return 2

    for stage, stage_stats in stats.items():
        print(
            f"{stage}: evaluated={stage_stats.evaluated} "
            f"converted={stage_stats.converted} skipped={stage_stats.skipped} errors={stage_stats.errors}"
        )

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
