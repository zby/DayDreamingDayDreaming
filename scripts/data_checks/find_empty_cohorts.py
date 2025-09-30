#!/usr/bin/env python3
"""Locate cohorts whose origin generations lack ``raw.txt`` artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

STAGES: tuple[str, ...] = ("draft", "essay", "evaluation")


@dataclass
class MembershipRecord:
    stage: str
    gen_id: str


@dataclass
class MissingArtifact:
    stage: str
    gen_id: str
    metadata_path: Path
    raw_path: Path


@dataclass
class CohortReport:
    cohort_id: str
    membership_path: Path
    origin_total: int
    missing_raw: list[MissingArtifact]
    metadata_missing: list[MembershipRecord]
    origin_by_stage: dict[str, int]

    @property
    def is_empty(self) -> bool:
        if self.origin_total == 0:
            return True
        return len(self.missing_raw) == self.origin_total

    def missing_raw_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for issue in self.missing_raw:
            counts[issue.stage] = counts.get(issue.stage, 0) + 1
        return counts

    def origin_counts(self) -> dict[str, int]:
        return dict(self.origin_by_stage)

    def metadata_missing_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for entry in self.metadata_missing:
            counts[entry.stage] = counts.get(entry.stage, 0) + 1
        return counts


def _load_metadata(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return {}


def _read_membership(path: Path) -> Iterator[MembershipRecord]:
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if "stage" not in reader.fieldnames or "gen_id" not in reader.fieldnames:
            raise ValueError(f"membership CSV missing required columns: {path}")
        seen: set[tuple[str, str]] = set()
        for row in reader:
            stage = str(row.get("stage") or "").strip()
            gen_id = str(row.get("gen_id") or "").strip()
            if not stage or not gen_id:
                continue
            key = (stage, gen_id)
            if key in seen:
                continue
            seen.add(key)
            yield MembershipRecord(stage=stage, gen_id=gen_id)


def _cohort_dirs(data_root: Path, filters: set[str] | None) -> Iterator[Path]:
    cohorts_root = data_root / "cohorts"
    if not cohorts_root.exists():
        return
    for cohort_dir in sorted(cohorts_root.iterdir()):
        if not cohort_dir.is_dir():
            continue
        cohort_id = cohort_dir.name
        if filters and cohort_id not in filters:
            continue
        membership_path = cohort_dir / "membership.csv"
        if membership_path.exists():
            yield cohort_dir


def _report_for_cohort(data_root: Path, cohort_dir: Path, stages: Iterable[str]) -> CohortReport:
    cohort_id = cohort_dir.name
    membership_path = cohort_dir / "membership.csv"
    origin_total = 0
    origin_by_stage: dict[str, int] = {}
    missing_raw: list[MissingArtifact] = []
    metadata_missing: list[MembershipRecord] = []

    stage_allowlist = set(stages)

    for entry in _read_membership(membership_path):
        if entry.stage not in stage_allowlist:
            continue
        gen_dir = data_root / "gens" / entry.stage / entry.gen_id
        meta_path = gen_dir / "metadata.json"
        metadata = _load_metadata(meta_path)
        if not metadata:
            metadata_missing.append(entry)
            continue
        origin = str(metadata.get("origin_cohort_id") or "").strip()
        if origin != cohort_id:
            continue
        origin_total += 1
        origin_by_stage[entry.stage] = origin_by_stage.get(entry.stage, 0) + 1
        raw_path = gen_dir / "raw.txt"
        if not raw_path.exists():
            missing_raw.append(
                MissingArtifact(
                    stage=entry.stage,
                    gen_id=entry.gen_id,
                    metadata_path=meta_path,
                    raw_path=raw_path,
                )
            )

    return CohortReport(
        cohort_id=cohort_id,
        membership_path=membership_path,
        origin_total=origin_total,
        missing_raw=missing_raw,
        metadata_missing=metadata_missing,
        origin_by_stage=origin_by_stage,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cohort",
        action="append",
        help="Restrict the check to one or more cohort ids (default: all cohorts)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data)",
    )
    parser.add_argument(
        "--stage",
        action="append",
        choices=[*STAGES, "all"],
        help="Limit to one or more stages (default: all stages)",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="List every missing artifact instead of summary counts",
    )
    args = parser.parse_args()

    if args.stage is None or "all" in args.stage:
        stages = STAGES
    else:
        stages = tuple(dict.fromkeys(args.stage))  # preserve order, drop dupes

    filters = set(args.cohort) if args.cohort else None

    reports = [
        _report_for_cohort(args.data_root, cohort_dir, stages)
        for cohort_dir in _cohort_dirs(args.data_root, filters)
    ]

    if not reports:
        print("No cohorts found to inspect.")
        return

    empty_reports = [report for report in reports if report.is_empty]

    print(f"Cohorts inspected: {len(reports)}")
    print(f"Cohorts flagged as empty: {len(empty_reports)}")

    for report in empty_reports:
        print(f"\nCohort: {report.cohort_id}")
        print(f" Membership: {report.membership_path}")
        if report.origin_total == 0:
            print(" Origin generations: 0 (no metadata rows matched this cohort)")
        else:
            print(
                f" Origin generations missing raw.txt: {len(report.missing_raw)}/{report.origin_total}"
            )
        missing_counts = report.missing_raw_counts()
        origin_counts = report.origin_counts()
        if missing_counts or origin_counts:
            stage_summaries: list[str] = []
            for stage in STAGES:
                total = origin_counts.get(stage)
                if total is None:
                    continue
                missing = missing_counts.get(stage, 0)
                stage_summaries.append(f"{stage}={missing}/{total}")
            if stage_summaries:
                suffix = "" if args.details else " (use --details to list each)"
                summary = ", ".join(stage_summaries)
                print(f" Missing raw.txt entries by stage: {summary}{suffix}")
            if args.details:
                for issue in report.missing_raw:
                    print(
                        f"  - {issue.stage}/{issue.gen_id}: missing {issue.raw_path}"
                        f" (metadata: {issue.metadata_path})"
                    )
        metadata_counts = report.metadata_missing_counts()
        if metadata_counts:
            summary = ", ".join(
                f"{stage}={metadata_counts[stage]}" for stage in STAGES if stage in metadata_counts
            )
            suffix = "" if args.details else " (use --details to list each)"
            total = sum(metadata_counts.values())
            print(
                f" Metadata missing for {total} entries ({summary}){suffix}"
            )
            if args.details:
                for entry in report.metadata_missing:
                    print(f"    - {entry.stage}/{entry.gen_id}")

    if empty_reports:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
