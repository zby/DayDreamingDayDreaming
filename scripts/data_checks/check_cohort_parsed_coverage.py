#!/usr/bin/env python3
"""Summarise parsed.txt coverage for a single cohort membership."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _ensure_src_on_path():
    # Allow running the script without installing the package
    repo_root = Path(__file__).resolve().parents[1]
    src = repo_root / "src"
    if src.exists():
        sys.path.insert(0, str(src))


_ensure_src_on_path()

import pandas as pd  # noqa: E402
from daydreaming_dagster.data_layer.paths import Paths  # noqa: E402
from daydreaming_dagster.utils.errors import DDError, Err  # noqa: E402


def compute_coverage(data_root: Path, cohort_id: str, stages: list[str] | None = None, list_missing: bool = False):
    paths = Paths.from_str(str(data_root))
    membership_csv = data_root / "cohorts" / str(cohort_id) / "membership.csv"
    if not membership_csv.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "cohort_membership_missing", "path": str(membership_csv)},
        )

    df = pd.read_csv(membership_csv)
    if df.empty or "stage" not in df.columns or "gen_id" not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "membership_missing_required_columns"},
        )

    target_stages = stages or sorted(df["stage"].astype(str).dropna().unique().tolist())
    stats: dict[str, dict] = {}
    overall_total = 0
    overall_present = 0
    missing_by_stage: dict[str, list[str]] = {}

    for stg in target_stages:
        sdf = df[df["stage"].astype(str) == stg]
        if sdf.empty:
            stats[stg] = {"total": 0, "present": 0, "missing": 0, "coverage_pct": 0.0}
            continue
        total = int(len(sdf))
        present = 0
        missing_ids: list[str] = []
        for _, row in sdf.iterrows():
            gid = str(row["gen_id"]).strip()
            p = paths.parsed_path(stg, gid)
            if p.exists():
                present += 1
            else:
                if list_missing:
                    missing_ids.append(gid)
        missing = total - present
        coverage = round((present / total) * 100, 1) if total > 0 else 0.0
        stats[stg] = {"total": total, "present": present, "missing": missing, "coverage_pct": coverage}
        if list_missing and missing_ids:
            missing_by_stage[stg] = missing_ids
        overall_total += total
        overall_present += present

    overall_coverage = round((overall_present / overall_total) * 100, 1) if overall_total > 0 else 0.0
    result = {
        "cohort_id": str(cohort_id),
        "data_root": str(data_root),
        "by_stage": stats,
        "overall": {"total": overall_total, "present": overall_present, "missing": overall_total - overall_present, "coverage_pct": overall_coverage},
    }
    if list_missing and missing_by_stage:
        result["missing_ids"] = missing_by_stage
    return result


def main():
    ap = argparse.ArgumentParser(description="Check parsed.txt coverage per stage for a cohort.")
    ap.add_argument("cohort_id", help="Cohort identifier (directory under data/cohorts/<cohort_id>)")
    ap.add_argument("--data-root", default="data", help="Path to data root (default: data)")
    ap.add_argument(
        "--stages",
        default=None,
        help="Comma-separated subset of stages to check (e.g., draft,essay,evaluation). Default: all in membership.csv",
    )
    ap.add_argument("--json", action="store_true", help="Output JSON instead of a text table")
    ap.add_argument("--list-missing", action="store_true", help="Include lists of missing gen_ids per stage in JSON output and print in text mode")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    stages = [s.strip() for s in args.stages.split(",")] if isinstance(args.stages, str) and args.stages else None

    try:
        result = compute_coverage(data_root, args.cohort_id, stages=stages, list_missing=args.list_missing)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    # Text output
    print(f"Cohort: {result['cohort_id']}")
    print(f"Data root: {result['data_root']}")
    print("")
    print("Stage          Total  Present  Missing  Coverage%")
    print("-------------------------------------------------")
    for stg, st in result["by_stage"].items():
        print(f"{stg:<13} {st['total']:>5}  {st['present']:>7}  {st['missing']:>7}  {st['coverage_pct']:>8.1f}")
        if args.list_missing and st.get("missing", 0) and "missing_ids" in result and stg in result["missing_ids"]:
            ids = ", ".join(result["missing_ids"][stg][:10])
            more = len(result["missing_ids"][stg]) - 10
            suffix = f" ... (+{more} more)" if more > 0 else ""
            print(f"  Missing sample: {ids}{suffix}")
    ov = result["overall"]
    print("-------------------------------------------------")
    print(f"Overall        {ov['total']:>5}  {ov['present']:>7}  {ov['missing']:>7}  {ov['coverage_pct']:>8.1f}")


if __name__ == "__main__":
    main()
