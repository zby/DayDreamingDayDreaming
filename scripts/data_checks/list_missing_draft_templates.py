#!/usr/bin/env python3
"""
List draft prompt templates that have no docs-store results yet.

Scans data/docs/draft/*/metadata.json to collect used draft template_ids,
compares to data/1_raw/draft_templates.csv, and prints a summary grouped by
template family (link-style vs essay-like) with counts.

Usage:
  uv run python scripts/list_missing_draft_templates.py [--data-root data]

Options:
  --data-root PATH   Base project data directory (default: data)
  --csv PATH         Optional CSV file to write the missing templates summary
  --only-link        Show only link-style families (Turn 1 candidates)
  --only-essaylike   Show only essay-like families (Turn 2 candidates)
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import json
from typing import Dict, Tuple


def _ensure_src_on_path() -> None:
    import sys

    repo_root = Path(__file__).resolve().parents[2]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))


_ensure_src_on_path()

from daydreaming_dagster.utils.errors import DDError, Err


def classify_family(tpl: str) -> str:
    s = tpl.lower()
    # Link-style buckets
    if any(k in s for k in (
        "rolling-summary", "deliberate-rolling-thread", "connective-thread",
        "pair-explorer", "recursive-light", "recursive-precise", "links-v", "links_", "links-"
    )):
        return "link-style"
    # Two-phase draft (creative-synthesis link phase)
    if s.startswith("creative-synthesis-v9") or s.startswith("creative-synthesis-v10"):
        return "link-style"
    return "essay-like"


def load_templates(csv: Path) -> pd.DataFrame:
    if not csv.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "draft_templates_csv_missing", "path": str(csv)},
        )
    df = pd.read_csv(csv)
    if "template_id" not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "draft_templates_missing_template_id", "path": str(csv)},
        )
    if "active" not in df.columns:
        df["active"] = False
    df["family"] = df["template_id"].astype(str).map(classify_family)
    return df


def scan_used_templates(docs_root: Path) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    droot = docs_root / "draft"
    if not droot.exists():
        return counts
    for d in droot.iterdir():
        if not d.is_dir():
            continue
        mp = d / "metadata.json"
        if not mp.exists():
            continue
        try:
            m = json.loads(mp.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        tpl = str(m.get("template_id") or m.get("draft_template") or "").strip()
        if not tpl:
            continue
        counts[tpl] = counts.get(tpl, 0) + 1
    return counts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--csv", type=Path, default=None)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--only-link", action="store_true", help="Show only link-style templates")
    g.add_argument("--only-essaylike", action="store_true", help="Show only essay-like templates")
    args = ap.parse_args()

    data_root: Path = args.data_root
    draft_csv = data_root / "1_raw" / "draft_templates.csv"
    docs_root = data_root / "docs"

    df = load_templates(draft_csv)
    used = scan_used_templates(docs_root)
    df["used_count"] = df["template_id"].map(lambda t: used.get(str(t), 0))
    df["has_results"] = df["used_count"] > 0
    df_missing = df[~df["has_results"]].copy()
    if args.only_link:
        df_missing = df_missing[df_missing["family"] == "link-style"]
    if args.only_essaylike:
        df_missing = df_missing[df_missing["family"] == "essay-like"]

    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        df_missing.sort_values(["family", "template_id"]).to_csv(args.csv, index=False)
        print(f"Wrote missing draft templates to {args.csv} ({len(df_missing)} rows)")
    else:
        if df_missing.empty:
            print("All draft templates have docs-store results (no missing).")
            return 0
        print("Missing draft templates (no docs-store results):")
        fam_counts: Dict[str, int] = {}
        for _, r in df_missing.sort_values(["family", "template_id"]).iterrows():
            fam = str(r["family"]) ; fam_counts[fam] = fam_counts.get(fam, 0) + 1
            print(f" - {r['template_id']} (family={r['family']}, active={bool(r['active'])})")
        print("")
        print("Summary by family:")
        for fam, c in fam_counts.items():
            print(f" - {fam}: {c}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
