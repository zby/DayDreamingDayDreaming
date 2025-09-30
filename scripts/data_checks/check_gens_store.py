#!/usr/bin/env python3
"""Validate gens-store artifacts and, optionally, prune incomplete runs.

By default the checker scans all stages and prints summary statistics. Use
``--verbose`` for per-generation details, ``--stage`` to scope the scan, and
``--delete-missing`` to remove incomplete evaluation runs (prompted unless
``--yes`` is supplied).

Usage:
    python scripts/data_checks/check_gens_store.py [--data-root PATH]
                                                  [--stage STAGE ...]
                                                  [--strict-prompt]
                                                  [--verbose]
                                                  [--delete-missing [--yes]]
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
import shutil
from pathlib import Path
from typing import Iterable

STAGES: tuple[str, ...] = ("draft", "essay", "evaluation")
REQUIRED_FILES: tuple[str, ...] = (
    "metadata.json",
    "parsed.txt",
    "parsed_metadata.json",
    "raw_metadata.json",
)
OPTIONAL_FILES: tuple[str, ...] = ("raw.txt",)
PROMPT_FILE = "prompt.txt"


def iter_gen_dirs(data_root: Path, stages: Iterable[str]) -> Iterable[tuple[str, Path | None]]:
    gens_root = data_root / "gens"
    for stage in stages:
        stage_dir = gens_root / stage
        if not stage_dir.exists():
            yield stage, None
            continue
        for gen_dir in sorted(p for p in stage_dir.iterdir() if p.is_dir()):
            yield stage, gen_dir


def check_generation_dir(gen_dir: Path, strict_prompt: bool):
    missing = [name for name in REQUIRED_FILES if not (gen_dir / name).exists()]
    missing_optional = [name for name in OPTIONAL_FILES if not (gen_dir / name).exists()]
    prompt_missing = not (gen_dir / PROMPT_FILE).exists()
    if strict_prompt and prompt_missing:
        missing.append(PROMPT_FILE)
    return missing, missing_optional, prompt_missing


def main() -> int:
    parser = argparse.ArgumentParser(description="Check gens-store artifacts")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Data root containing gens/ (default: %(default)s)",
    )
    parser.add_argument(
        "--stage",
        action="append",
        choices=[*STAGES, "all"],
        help="Limit the scan to one or more stages (default: all stages). Pass multiple times to combine.",
    )
    parser.add_argument(
        "--strict-prompt",
        action="store_true",
        help="Treat missing prompt.txt as an error instead of a warning.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show per-generation details when artifacts are missing.",
    )
    parser.add_argument(
        "--delete-missing",
        action="store_true",
        help="Delete evaluation runs missing metadata or raw artifacts (prompts before removal).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation when used with --delete-missing.",
    )
    args = parser.parse_args()

    data_root: Path = args.data_root
    strict_prompt: bool = args.strict_prompt
    verbose: bool = args.verbose

    if args.stage is None or "all" in args.stage:
        stages = STAGES
    else:
        seen = []
        for item in args.stage:
            if item not in seen:
                seen.append(item)
        stages = tuple(seen)

    missing_required_stats: Counter[str] = Counter()
    missing_optional_stats: Counter[str] = Counter()
    prompt_warnings = 0
    missing_stage_dirs: list[str] = []
    inspected = 0
    missing_metadata_dirs: list[tuple[str, Path]] = []
    missing_raw_dirs: list[tuple[str, Path]] = []

    for stage, gen_dir in iter_gen_dirs(data_root, stages):
        if gen_dir is None:
            missing_stage_dirs.append(stage)
            continue
        inspected += 1
        missing_required, missing_optional, prompt_missing = check_generation_dir(gen_dir, strict_prompt)
        if missing_required:
            missing_required_stats.update(missing_required)
            if verbose:
                rel = gen_dir.relative_to(data_root)
                print(f"ERROR: {rel} missing {', '.join(sorted(missing_required))}")
            if "metadata.json" in missing_required:
                missing_metadata_dirs.append((stage, gen_dir))
        if missing_optional:
            missing_optional_stats.update(missing_optional)
            if verbose:
                rel = gen_dir.relative_to(data_root)
                print(f"WARN: {rel} missing optional {', '.join(sorted(missing_optional))}")
            if "raw.txt" in missing_optional:
                missing_raw_dirs.append((stage, gen_dir))
        if prompt_missing and not strict_prompt:
            prompt_warnings += 1
            if verbose:
                rel = gen_dir.relative_to(data_root)
                print(f"WARN: {rel} missing {PROMPT_FILE}")

    gens_root = data_root / "gens"
    print(f"Checked {inspected} generation directories under {gens_root} ({', '.join(stages)})")
    if missing_stage_dirs:
        print("Missing stage directories:", ", ".join(missing_stage_dirs))

    if missing_required_stats:
        print("Required artifacts missing:")
        for name, count in sorted(missing_required_stats.items()):
            print(f"  {name}: {count}")
    else:
        print("Required artifacts missing: none")

    if missing_optional_stats:
        print("Optional artifacts missing:")
        for name, count in sorted(missing_optional_stats.items()):
            print(f"  {name}: {count}")
    else:
        print("Optional artifacts missing: none")

    if prompt_warnings and not strict_prompt:
        print(f"Prompts missing (warnings): {prompt_warnings}")

    exit_code = 0
    if missing_required_stats:
        print("FAIL: required artifacts missing")
        exit_code = 1
    else:
        print("OK: all required artifacts present")

    if args.delete_missing:
        deletion_candidates = []
        for stage, path in missing_metadata_dirs + missing_raw_dirs:
            if stage != "evaluation":
                continue
            if path not in deletion_candidates:
                deletion_candidates.append(path)
        if not deletion_candidates:
            print("No evaluation directories eligible for deletion.")
        else:
            print("\nEvaluation runs missing required artifacts:")
            for path in deletion_candidates:
                print(f"  - {path}")
            proceed = args.yes
            if not proceed:
                resp = input("Delete these directories? [y/N] ").strip().lower()
                proceed = resp.startswith("y")
            if proceed:
                for directory in deletion_candidates:
                    if directory.exists():
                        shutil.rmtree(directory)
                        print(f"Deleted {directory}")
            else:
                print("Deletion cancelled.")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
