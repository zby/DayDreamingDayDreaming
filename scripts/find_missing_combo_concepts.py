#!/usr/bin/env python3
"""
Scan combo_mappings.csv for concept_ids missing from concepts_metadata.csv.

Usage:
  uv run python scripts/find_missing_combo_concepts.py \
    --data-root data \
    --mappings data/combo_mappings.csv \
    --concepts data/1_raw/concepts_metadata.csv \
    [--show-combos]

Exit codes:
  0  no missing concepts
  2  missing concept_ids found
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Set
import sys

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root")
    p.add_argument("--mappings", type=Path, default=Path("data/combo_mappings.csv"), help="Path to combo_mappings.csv (superset)")
    p.add_argument("--concepts", type=Path, default=Path("data/1_raw/concepts_metadata.csv"), help="Path to concepts_metadata.csv")
    p.add_argument("--show-combos", action="store_true", help="Also show combo_ids that reference missing concepts")
    return p.parse_args()


def load_mappings(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"ERROR: failed to read combo_mappings from {path}: {e}", file=sys.stderr)
        raise SystemExit(1)
    required = {"combo_id", "concept_id"}
    missing = required - set(df.columns)
    if missing:
        print(f"ERROR: {path} missing required columns: {sorted(missing)}", file=sys.stderr)
        raise SystemExit(1)
    return df


def load_concepts_metadata(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"ERROR: failed to read concepts_metadata from {path}: {e}", file=sys.stderr)
        raise SystemExit(1)
    if "concept_id" not in df.columns:
        print(f"ERROR: {path} missing required column 'concept_id'", file=sys.stderr)
        raise SystemExit(1)
    return df


def main() -> int:
    args = parse_args()
    mapping_df = load_mappings(args.mappings)
    concepts_df = load_concepts_metadata(args.concepts)

    mapped_ids: Set[str] = set(mapping_df["concept_id"].astype(str))
    known_ids: Set[str] = set(concepts_df["concept_id"].astype(str))

    missing_ids: List[str] = sorted(list(mapped_ids - known_ids))
    print(f"Mapped unique concept_ids: {len(mapped_ids)}")
    print(f"Known concepts in metadata: {len(known_ids)}")
    print(f"Missing concepts: {len(missing_ids)}")
    if missing_ids:
        print("Missing list (first 50):", missing_ids[:50])
        if args.show_combos:
            bad = (
                mapping_df[mapping_df["concept_id"].astype(str).isin(missing_ids)]["combo_id"]
                .astype(str)
                .dropna()
                .unique()
                .tolist()
            )
            bad.sort()
            print(f"Combos referencing missing concepts ({len(bad)}):", bad[:50])
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

