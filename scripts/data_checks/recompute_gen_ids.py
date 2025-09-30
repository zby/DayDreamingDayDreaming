#!/usr/bin/env python3
"""Compare stored generation IDs against deterministic recomputations."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, cast

from daydreaming_dagster.utils.errors import DDError
from daydreaming_dagster.utils.ids import (
    compute_deterministic_gen_id,
    signature_from_metadata,
)
from daydreaming_dagster.types import Stage, STAGES


@dataclass
class IdMismatch:
    stage: Stage
    gen_id: str
    metadata_path: str
    deterministic_id: str
    metadata_gen_id: str
    effect: str


@dataclass
class IdError:
    stage: Stage
    gen_id: str
    metadata_path: str
    error: str


@dataclass
class StageReport:
    stage: Stage
    total: int
    mismatches: List[IdMismatch]
    errors: List[IdError]

    @property
    def deterministic_count(self) -> int:
        return self.total - len(self.mismatches) - len(self.errors)


def _load_metadata(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return {}


def _effect_for_stage(stage: Stage) -> str:
    if stage == "draft":
        return "regen_draft_and_children"
    if stage == "essay":
        return "regen_evaluations"
    if stage == "evaluation":
        return "regen_evaluation"
    return "unknown"


def analyze_stage(data_root: Path, stage: Stage) -> StageReport:
    stage_dir = data_root / "gens" / stage
    mismatches: List[IdMismatch] = []
    errors: List[IdError] = []
    total = 0

    if not stage_dir.exists():
        return StageReport(stage=stage, total=0, mismatches=mismatches, errors=errors)

    for generation_dir in stage_dir.iterdir():
        if not generation_dir.is_dir():
            continue
        total += 1
        gen_id = generation_dir.name
        metadata_path = generation_dir / "metadata.json"
        metadata = _load_metadata(metadata_path)

        if metadata is None:
            errors.append(
                IdError(
                    stage=stage,
                    gen_id=gen_id,
                    metadata_path=str(metadata_path),
                    error="metadata_missing",
                )
            )
            continue
        if metadata == {}:
            errors.append(
                IdError(
                    stage=stage,
                    gen_id=gen_id,
                    metadata_path=str(metadata_path),
                    error="metadata_invalid_json",
                )
            )
            continue

        meta_gen_id = str(metadata.get("gen_id") or "").strip()
        if not meta_gen_id:
            errors.append(
                IdError(
                    stage=stage,
                    gen_id=gen_id,
                    metadata_path=str(metadata_path),
                    error="metadata_gen_id_missing",
                )
            )
        elif meta_gen_id != gen_id:
            errors.append(
                IdError(
                    stage=stage,
                    gen_id=gen_id,
                    metadata_path=str(metadata_path),
                    error=f"metadata_gen_id_mismatch:{meta_gen_id}",
                )
            )

        try:
            signature = signature_from_metadata(stage, metadata)
            deterministic_id = compute_deterministic_gen_id(stage, signature)
        except DDError as exc:
            errors.append(
                IdError(
                    stage=stage,
                    gen_id=gen_id,
                    metadata_path=str(metadata_path),
                    error=f"signature_error:{exc.code.name}:{exc.ctx}",
                )
            )
            continue

        if deterministic_id != gen_id:
            mismatches.append(
                IdMismatch(
                    stage=stage,
                    gen_id=gen_id,
                    metadata_path=str(metadata_path),
                    deterministic_id=deterministic_id,
                    metadata_gen_id=meta_gen_id,
                    effect=_effect_for_stage(stage),
                )
            )

    return StageReport(stage=stage, total=total, mismatches=mismatches, errors=errors)


def _print_report(report: StageReport, max_print: int) -> None:
    print(f"Stage: {report.stage}")
    print(f"  Total: {report.total}")
    print(f"  Deterministic matches: {report.deterministic_count}")
    print(f"  Mismatches: {len(report.mismatches)}")
    for mismatch in report.mismatches[:max_print]:
        meta_id = mismatch.metadata_gen_id or "<missing>"
        print(
            f"    - {mismatch.gen_id} -> {mismatch.deterministic_id}"
            f" (metadata: {meta_id}; {mismatch.effect})"
        )
    if len(report.mismatches) > max_print:
        print(f"    ... {len(report.mismatches) - max_print} more mismatches")
    print(f"  Errors: {len(report.errors)}")
    for error in report.errors[:max_print]:
        print(f"    - {error.gen_id}: {error.error}")
    if len(report.errors) > max_print:
        print(f"    ... {len(report.errors) - max_print} more errors")
    print()


def _write_csv(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_iter = list(rows)
    if not rows_iter:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows_iter[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_iter:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Path to the project data root (default: data)",
    )
    parser.add_argument(
        "--stage",
        choices=STAGES,
        default=None,
        help="Optional stage to limit analysis",
    )
    parser.add_argument(
        "--max-print",
        type=int,
        default=10,
        help="Limit number of mismatches/errors printed per stage",
    )
    parser.add_argument(
        "--mismatches-csv",
        type=Path,
        default=None,
        help="Optional CSV path to write mismatch details",
    )
    parser.add_argument(
        "--errors-csv",
        type=Path,
        default=None,
        help="Optional CSV path to write error details",
    )
    parser.add_argument(
        "--fail-on-mismatch",
        action="store_true",
        help="Exit with non-zero status if mismatches or errors are present",
    )
    args = parser.parse_args()

    stages: List[Stage]
    if args.stage:
        stages = [cast(Stage, args.stage)]
    else:
        stages = list(STAGES)
    data_root = args.data_root.resolve()

    reports = [analyze_stage(data_root, stage) for stage in stages]

    for report in reports:
        _print_report(report, args.max_print)

    if args.mismatches_csv:
        mismatch_rows = [asdict(m) for report in reports for m in report.mismatches]
        _write_csv(args.mismatches_csv, mismatch_rows)
    if args.errors_csv:
        error_rows = [asdict(e) for report in reports for e in report.errors]
        _write_csv(args.errors_csv, error_rows)

    if args.fail_on_mismatch and any(report.mismatches or report.errors for report in reports):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
