from __future__ import annotations

import argparse
from pathlib import Path

from daydreaming_dagster.cohorts.spec_migration import generate_spec_bundle, SpecGenerationError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a DSL cohort spec from an existing membership.csv",
    )
    parser.add_argument("cohort_id", help="Cohort identifier (e.g., best_novelty_all_evals-v3)")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Project data root containing cohorts/<id>/membership.csv (default: ./data)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Optional output directory for the spec bundle (defaults to data/cohorts/<id>/spec)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing spec directory if it already exists.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        spec_dir = generate_spec_bundle(
            data_root=args.data_root,
            cohort_id=args.cohort_id,
            output_dir=args.out,
            overwrite=args.overwrite,
        )
    except SpecGenerationError as exc:
        parser.error(str(exc))
    print(f"Wrote cohort spec to {spec_dir}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
