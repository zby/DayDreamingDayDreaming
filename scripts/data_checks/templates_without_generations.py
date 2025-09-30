#!/usr/bin/env python3
"""Report templates that have zero generations in the gens store.

Scans ``data/gens/<stage>/*/metadata.json`` and compares against template CSVs
under ``data/1_raw``. Supports optional stage filters and producing the same
draft-only listing that ``find_unused_draft_templates.py`` used to emit.

Usage examples::

  uv run python scripts/data_checks/templates_without_generations.py \
    --data-root data

  uv run python scripts/data_checks/templates_without_generations.py \
    --stage draft --list-unused --output unused_draft_templates.txt
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd


def _ensure_src_on_path() -> None:
    import sys

    repo_root = Path(__file__).resolve().parents[2]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))


_ensure_src_on_path()

from daydreaming_dagster.utils.errors import DDError, Err


STAGES = ("draft", "essay", "evaluation")


def _read_templates_table(stage: str, data_root: Path) -> pd.DataFrame:
    base = data_root / "1_raw"
    fname = {
        "draft": "draft_templates.csv",
        "essay": "essay_templates.csv",
        "evaluation": "evaluation_templates.csv",
    }[stage]
    fp = base / fname
    if not fp.exists():
        return pd.DataFrame(columns=["template_id", "active"]).astype({"template_id": str})
    df = pd.read_csv(fp)
    # Normalize minimal columns
    if "template_id" not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "templates_missing_template_id", "path": str(fp)},
        )
    if "active" not in df.columns:
        # default inactive when absent
        df["active"] = False
    df["template_id"] = df["template_id"].astype(str)
    df["active"] = df["active"].astype(bool)
    return df[["template_id", "active"]]


def _extract_template_id(meta: dict, stage: str) -> str | None:
    # Prefer canonical key
    tid = meta.get("template_id")
    if isinstance(tid, str) and tid.strip():
        return tid.strip()
    # Legacy fallbacks (best effort; safe to skip if absent)
    if stage == "essay":
        tid = meta.get("essay_template")
    elif stage == "draft":
        tid = meta.get("draft_template")
    else:
        tid = None
    return tid.strip() if isinstance(tid, str) and tid.strip() else None


def _scan_gens_templates(stage: str, data_root: Path) -> Counter:
    root = data_root / "gens" / stage
    counts: Counter = Counter()
    if not root.exists():
        return counts
    for d in root.iterdir():
        if not d.is_dir():
            continue
        mpath = d / "metadata.json"
        if not mpath.exists():
            continue
        try:
            meta = json.loads(mpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        tid = _extract_template_id(meta, stage)
        if isinstance(tid, str) and tid:
            counts[tid] += 1
    return counts


def _report_stage(
    stage: str,
    df: pd.DataFrame,
    used: Counter,
    *,
    active_only: bool,
    list_unused: bool,
    output_path: Path | None,
) -> Tuple[int, int]:
    template_ids = sorted(set(df["template_id"].astype(str))) if not df.empty else []
    active_ids = sorted(set(df[df["active"] == True]["template_id"].astype(str))) if not df.empty else []
    used_ids = sorted(used.keys())
    all_unused = sorted(set(template_ids) - set(used_ids))
    active_unused = sorted(set(active_ids) - set(used_ids))

    unused_display = active_unused if active_only else all_unused

    print(f"\n== {stage.upper()} ==")
    print(f"templates: total={len(template_ids)}, active={len(active_ids)}")
    print(f"templates with generations: {len(used_ids)}")

    target_label = "ACTIVE" if active_only else "ALL"
    if unused_display:
        print(f"{target_label} templates with 0 generations ({len(unused_display)}):")
        for tid in unused_display:
            print(f"  - {tid}")
    else:
        print(f"No {target_label.lower()} templates are unused.")

    if list_unused and unused_display:
        print("\nPlain list:")
        for tid in unused_display:
            print(tid)

    if output_path and unused_display:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("a", encoding="utf-8") as fp:
            for tid in unused_display:
                fp.write(f"{stage},{tid}\n")

    return (len(active_unused), len(all_unused))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root")
    ap.add_argument(
        "--stage",
        action="append",
        choices=[*STAGES, "all"],
        help="Limit report to one or more stages (default: all stages). Pass multiple times to combine.",
    )
    ap.add_argument(
        "--fail-on-active-unused",
        action="store_true",
        help="Exit non-zero if any ACTIVE templates have zero generations",
    )
    ap.add_argument(
        "--active-only",
        action="store_true",
        help="Report only active templates without generations (default: report all templates).",
    )
    ap.add_argument(
        "--list-unused",
        action="store_true",
        help="Print a plain list of unused template_ids for each stage.",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional file to append '<stage>,<template_id>' rows for unused templates.",
    )
    args = ap.parse_args()

    data_root: Path = args.data_root

    if args.stage is None or "all" in args.stage:
        stages = STAGES
    else:
        seen = []
        for item in args.stage:
            if item not in seen:
                seen.append(item)
        stages = tuple(seen)

    if args.output and args.output.exists():
        args.output.unlink()

    any_active_unused = False
    for stage in stages:
        df = _read_templates_table(stage, data_root)
        used = _scan_gens_templates(stage, data_root)
        a_unused, _ = _report_stage(
            stage,
            df,
            used,
            active_only=args.active_only,
            list_unused=args.list_unused,
            output_path=args.output,
        )
        if a_unused > 0:
            any_active_unused = True

    if args.fail_on_active_unused and any_active_unused:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
