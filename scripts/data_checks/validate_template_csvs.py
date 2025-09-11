#!/usr/bin/env python3
"""
Validate strict template CSV schema:
- generator present in all three CSVs (draft, essay, evaluation)
- parser present in evaluation
- active is last column in each CSV
- essay generator must be 'llm' or 'copy'; draft/eval generator should be 'llm'

Usage:
  uv run python scripts/data_checks/validate_template_csvs.py [--data-root data]
  uv run python scripts/data_checks/validate_template_csvs.py --fail
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


def _check_csv(path: Path, *, require_parser: bool, stage: str) -> list[str]:
    errs: list[str] = []
    if not path.exists():
        errs.append(f"Missing CSV: {path}")
        return errs
    df = pd.read_csv(path)
    cols = list(df.columns)
    if "generator" not in cols:
        errs.append(f"{path}: missing required 'generator' column")
    if require_parser and "parser" not in cols:
        errs.append(f"{path}: missing required 'parser' column")
    if cols and cols[-1] != "active":
        errs.append(f"{path}: 'active' should be the last column (got '{cols[-1]}')")
    if "generator" in df.columns:
        gens = set(str(x).strip().lower() for x in df["generator"].dropna().tolist())
        if stage == "essay":
            bad = [g for g in gens if g not in {"llm", "copy"}]
            if bad:
                errs.append(f"{path}: invalid essay generator values: {sorted(set(bad))}")
        else:
            bad = [g for g in gens if g not in {"llm"}]
            if bad:
                errs.append(f"{path}: non-llm generator values for {stage}: {sorted(set(bad))}")
    return errs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--fail", action="store_true", help="Exit non-zero on any error")
    args = ap.parse_args()

    base = args.data_root / "1_raw"
    problems: list[str] = []
    problems += _check_csv(base / "draft_templates.csv", require_parser=False, stage="draft")
    problems += _check_csv(base / "essay_templates.csv", require_parser=False, stage="essay")
    problems += _check_csv(base / "evaluation_templates.csv", require_parser=True, stage="evaluation")

    if problems:
        print("Template CSV validation issues:")
        for p in problems:
            print(" -", p)
    else:
        print("All template CSVs look good.")

    return 1 if (problems and args.fail) else 0


if __name__ == "__main__":
    raise SystemExit(main())

