#!/usr/bin/env python3
"""Validate gens-store artifacts for all stages and generation ids.

By default the checker prints summary statistics only. Use ``--verbose`` to
show per-generation details when artifacts are missing.

Usage:
    python scripts/data_checks/check_gens_store.py [--data-root PATH] [--strict-prompt] [--verbose]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

STAGES: tuple[str, ...] = ("draft", "essay", "evaluation")
REQUIRED_FILES: tuple[str, ...] = (
    "metadata.json",
    "raw.txt",
    "parsed.txt",
    "raw_metadata.json",
    "parsed_metadata.json",
)
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


def check_generation_dir(gen_dir: Path, strict_prompt: bool) -> tuple[list[str], bool]:
    missing = [name for name in REQUIRED_FILES if not (gen_dir / name).exists()]
    prompt_missing = not (gen_dir / PROMPT_FILE).exists()
    if strict_prompt and prompt_missing:
        missing.append(PROMPT_FILE)
    return missing, prompt_missing


def main() -> int:
    parser = argparse.ArgumentParser(description="Check gens-store artifacts")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Data root containing gens/ (default: %(default)s)",
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
    args = parser.parse_args()

    data_root: Path = args.data_root
    strict_prompt: bool = args.strict_prompt
    verbose: bool = args.verbose

    missing_required = 0
    prompt_warnings = 0
    missing_stage_dirs: list[str] = []
    inspected = 0

    for stage, gen_dir in iter_gen_dirs(data_root, STAGES):
        if gen_dir is None:
            missing_stage_dirs.append(stage)
            continue
        inspected += 1
        missing, prompt_missing = check_generation_dir(gen_dir, strict_prompt)
        if missing:
            missing_required += 1
            if verbose:
                rel = gen_dir.relative_to(data_root)
                print(f"ERROR: {rel} missing {', '.join(sorted(missing))}")
        elif prompt_missing:
            prompt_warnings += 1
            if verbose and not strict_prompt:
                rel = gen_dir.relative_to(data_root)
                print(f"WARN: {rel} missing {PROMPT_FILE}")

    gens_root = data_root / "gens"
    print(
        f"Checked {inspected} generation directories under {gens_root} ({len(STAGES)} stages)",
    )
    if missing_stage_dirs:
        print("Missing stage directories:", ", ".join(missing_stage_dirs))
    if prompt_warnings and not strict_prompt:
        print(f"Warnings: {prompt_warnings} generation(s) without {PROMPT_FILE}")

    if missing_required > 0:
        print(f"FAIL: {missing_required} generation(s) missing required artifacts")
        return 1

    print("OK: all required artifacts present")
    return 0


if __name__ == "__main__":
    sys.exit(main())
