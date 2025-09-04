#!/usr/bin/env python3
"""
Write data/2_tasks/selected_combo_mappings.csv by filtering data/combo_mappings.csv
to the provided combo_ids. Accepts ids via --ids or from a text file with one id per line.

Usage:
  uv run python scripts/select_combos.py --ids combo_v1_abc123def456 combo_v1_deadbeefcafe
  uv run python scripts/select_combos.py --from-file data/2_tasks/selected_combo_ids.txt
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--ids", nargs="*", help="combo_id values to select")
    p.add_argument("--from-file", type=Path, default=None, help="Path to a text file with one combo_id per line")
    p.add_argument("--dry-run", action="store_true", help="Preview selection without writing")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    data_root = args.data_root
    ids: list[str] = []
    if args.ids:
        ids.extend([str(x) for x in args.ids])
    if args.from_file:
        if not args.from_file.exists():
            print(f"ERROR: id list not found: {args.from_file}", file=sys.stderr)
            return 2
        ids.extend([line.strip() for line in args.from_file.read_text(encoding="utf-8").splitlines() if line.strip()])

    ids = sorted({x for x in ids if isinstance(x, str) and x})
    if not ids:
        print("No combo_ids provided (use --ids or --from-file)", file=sys.stderr)
        return 2

    superset = data_root / "combo_mappings.csv"
    if not superset.exists():
        print(f"ERROR: superset not found: {superset}", file=sys.stderr)
        return 2
    try:
        mdf = pd.read_csv(superset)
    except Exception as e:
        print(f"ERROR: failed to read superset: {e}", file=sys.stderr)
        return 2

    subset = mdf[mdf["combo_id"].astype(str).isin(ids)].copy()
    if subset.empty:
        print("Warning: no matching combo_ids found in superset", file=sys.stderr)
    out = data_root / "2_tasks" / "selected_combo_mappings.csv"
    if args.dry_run:
        print(f"[dry-run] Would write {len(subset)} rows to {out}")
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    subset.to_csv(out, index=False)
    print(f"Wrote {len(subset)} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

