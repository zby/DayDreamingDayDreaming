#!/usr/bin/env python3
"""Find templates that have zero generations in the gens store.

Scans data/gens/<stage>/*/metadata.json and compares against template CSVs:
- draft: data/1_raw/draft_templates.csv
- essay: data/1_raw/essay_templates.csv
- evaluation: data/1_raw/evaluation_templates.csv

By default, reports both active-unused and all-unused templates for each stage.

Usage:
  uv run python scripts/data_checks/templates_without_generations.py \
    [--data-root data] [--fail-on-active-unused]
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd


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
        raise ValueError(f"Missing 'template_id' in {fp}")
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


def _report_stage(stage: str, df: pd.DataFrame, used: Counter) -> Tuple[int, int]:
    template_ids = sorted(set(df["template_id"].astype(str))) if not df.empty else []
    active_ids = sorted(set(df[df["active"] == True]["template_id"].astype(str))) if not df.empty else []
    used_ids = sorted(used.keys())
    all_unused = sorted(set(template_ids) - set(used_ids))
    active_unused = sorted(set(active_ids) - set(used_ids))

    print(f"\n== {stage.upper()} ==")
    print(f"templates: total={len(template_ids)}, active={len(active_ids)}")
    print(f"templates with generations: {len(used_ids)}")
    if active_unused:
        print(f"ACTIVE templates with 0 generations ({len(active_unused)}):")
        for tid in active_unused:
            print(f"  - {tid}")
    else:
        print("No active templates are unused.")
    # Show all-unused if there are any beyond active
    if all_unused:
        print(f"ALL templates with 0 generations ({len(all_unused)}):")
        for tid in all_unused:
            print(f"  - {tid}")
    return (len(active_unused), len(all_unused))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root")
    ap.add_argument(
        "--fail-on-active-unused",
        action="store_true",
        help="Exit non-zero if any ACTIVE templates have zero generations",
    )
    args = ap.parse_args()

    data_root: Path = args.data_root
    any_active_unused = False
    for stage in STAGES:
        df = _read_templates_table(stage, data_root)
        used = _scan_gens_templates(stage, data_root)
        a_unused, _ = _report_stage(stage, df, used)
        if a_unused > 0:
            any_active_unused = True

    if args.fail_on_active_unused and any_active_unused:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

