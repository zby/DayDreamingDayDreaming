#!/usr/bin/env python3
"""Scan gens store and list draft templates with no generations.

Usage:
  uv run python scripts/data_checks/find_unused_draft_templates.py [--data-root DATA] [--active-only] [--out OUTFILE]

Behavior:
- Reads data/1_raw/draft_templates.csv to obtain the canonical list of draft template_ids
  (optionally filtered to active=True with --active-only).
- Scans data/gens/draft/*/metadata.json and collects the set of used draft template_ids
  (template_id preferred; FALLBACK to 'draft_template' when present).
- Prints a summary and, when --out is provided, writes the list to the given file (one per line).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Set, List

import pandas as pd


def _ensure_src_on_path() -> None:
    import sys

    repo_root = Path(__file__).resolve().parents[2]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))


_ensure_src_on_path()

from daydreaming_dagster.utils.errors import DDError, Err


def read_all_draft_templates(data_root: Path, active_only: bool) -> List[str]:
    csv_path = data_root / "1_raw" / "draft_templates.csv"
    if not csv_path.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "draft_templates_csv_missing", "path": str(csv_path)},
        )
    df = pd.read_csv(csv_path)
    if active_only and "active" in df.columns:
        df = df[df["active"] == True]
    if "template_id" not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "draft_templates_missing_template_id"},
        )
    return (
        df["template_id"]
        .astype(str)
        .dropna()
        .map(lambda s: s.strip())
        .loc[lambda s: s != ""]
        .unique()
        .tolist()
    )


def collect_used_draft_templates(data_root: Path) -> Set[str]:
    gens_root = data_root / "gens" / "draft"
    used: Set[str] = set()
    if not gens_root.exists():
        return used
    for doc_dir in gens_root.iterdir():
        if not doc_dir.is_dir():
            continue
        md = doc_dir / "metadata.json"
        if not md.exists():
            continue
        try:
            data = json.loads(md.read_text(encoding="utf-8")) or {}
            # Prefer canonical key; FALLBACK to legacy key for backcompat
            tpl = data.get("template_id") or data.get("draft_template")
            if isinstance(tpl, str) and tpl.strip():
                used.add(tpl.strip())
        except Exception:
            # Best-effort scan
            continue
    return used


def main() -> int:
    ap = argparse.ArgumentParser(description="List draft templates that have no generations in gens store")
    ap.add_argument("--data-root", default="data", help="Base data directory (default: data)")
    ap.add_argument("--active-only", action="store_true", help="Consider only active templates from CSV")
    ap.add_argument("--out", default=None, help="Optional path to write newline-separated list of unused templates")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    all_templates = set(read_all_draft_templates(data_root, active_only=args.active_only))
    used_templates = collect_used_draft_templates(data_root)
    unused = sorted(list(all_templates - used_templates))

    print(f"Data root: {data_root}")
    print(f"Draft templates (total{', active only' if args.active_only else ''}): {len(all_templates)}")
    print(f"Draft templates used in gens: {len(used_templates)}")
    print(f"Unused draft templates: {len(unused)}\n")
    for t in unused:
        print(t)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(unused) + ("\n" if unused else ""), encoding="utf-8")
        print(f"\nWritten list to: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
