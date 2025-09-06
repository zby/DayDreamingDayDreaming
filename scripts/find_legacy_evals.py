#!/usr/bin/env python3
"""Scan evaluation responses to find legacy (complex) parsing templates.

This script helps determine whether we still need to keep the legacy
evaluation parsing strategy. It scans the evaluation response files and,
for each, determines the evaluation template and parsing strategy
('complex' legacy vs 'in_last_line' modern).

Usage:
  python scripts/find_legacy_evals.py --data-root data [--out outfile.tsv] [--fail-on-found]

Output:
  Prints a tab-separated list of legacy items: evaluation_task_id, template, strategy, path
  Also prints a summary line with totals.
  If --out is provided, writes the list to that file as well.
  If --fail-on-found is set, exits with code 1 when any legacy items are found.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


def _load_task_templates(tasks_csv: Path) -> dict[str, str]:
    try:
        df = pd.read_csv(tasks_csv)
        if "evaluation_task_id" in df.columns and "evaluation_template" in df.columns:
            return (
                df[["evaluation_task_id", "evaluation_template"]]
                .dropna()
                .set_index("evaluation_task_id")["evaluation_template"]
                .to_dict()
            )
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return {}


def _parse_template_from_filename(stem: str) -> str | None:
    # Heuristic fallback used in evaluation_processing; scan from right for known templates
    known_templates = {
        'creative-synthesis', 'creative-synthesis-v2', 'creative-synthesis-v3',
        'essay-inventive-synthesis', 'essay-inventive-synthesis-v3',
        'research-discovery', 'research-discovery-v2', 'research-discovery-v3',
        'systematic-analytical', 'systematic-analytical-v2',
        'problem-solving', 'problem-solving-v2',
        'application-implementation', 'application-implementation-v2',
        'gwern-original',
        'daydreaming-verification', 'daydreaming-verification-v2',
        'o3-prior-art-eval', 'gemini-prior-art-eval', 
        'style-coherence', 'style-coherence-v2', 'style-coherence-v3',
        'creativity-metrics', 'iterative-loops', 'scientific-rigor'
    }
    parts = stem.split("_")
    for i in range(len(parts) - 1, -1, -1):
        part = parts[i]
        if part in known_templates:
            return part
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="data", help="Project data root (default: data)")
    ap.add_argument("--responses-dir", default=None, help="Override evaluation responses dir")
    ap.add_argument("--tasks-csv", default=None, help="Override evaluation_tasks.csv path")
    ap.add_argument("--out", default=None, help="Optional path to write TSV list of legacy items")
    ap.add_argument("--fail-on-found", action="store_true", help="Exit with code 1 if any legacy items are found")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    responses_dir = Path(args.responses_dir) if args.responses_dir else (data_root / "4_evaluation" / "evaluation_responses")
    tasks_csv = Path(args.tasks_csv) if args.tasks_csv else (data_root / "2_tasks" / "evaluation_tasks.csv")

    # Import detection logic from project utility to avoid duplicated truth
    from daydreaming_dagster.utils.evaluation_processing import detect_parsing_strategy, load_evaluation_parsing_strategies

    # Map evaluation_task_id -> evaluation_template from tasks CSV if present
    task_templates = _load_task_templates(tasks_csv)
    strategy_map = load_evaluation_parsing_strategies(data_root)

    legacy_rows: list[tuple[str, str, str, str]] = []
    total = 0
    for fp in sorted(responses_dir.glob("*.txt")):
        total += 1
        stem = fp.stem
        template = task_templates.get(stem) or _parse_template_from_filename(stem)
        if not template:
            # Unable to determine template; skip
            continue
        strategy = detect_parsing_strategy(template, strategy_map)
        if strategy == "complex":
            legacy_rows.append((stem, template, strategy, str(fp)))

    if legacy_rows:
        print("evaluation_task_id\ttemplate\tstrategy\tpath")
        for row in legacy_rows:
            print("\t".join(row))
        print(f"\nFound {len(legacy_rows)} legacy evaluation responses out of {total} files in {responses_dir}.")
    else:
        print(f"No legacy evaluation responses found in {responses_dir}. Scanned {total} files.")

    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        with outp.open("w", encoding="utf-8") as f:
            if legacy_rows:
                f.write("evaluation_task_id\ttemplate\tstrategy\tpath\n")
                for row in legacy_rows:
                    f.write("\t".join(row) + "\n")
        print(f"Wrote details to {outp}")

    return 1 if (args.fail_on_found and legacy_rows) else 0


if __name__ == "__main__":
    sys.exit(main())
