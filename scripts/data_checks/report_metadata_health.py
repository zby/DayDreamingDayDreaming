#!/usr/bin/env python3
"""Report gens-store metadata health (missing/invalid metadata files).

This audit scans ``data/gens`` (or a custom ``--data-root``) and surfaces
missing or invalid ``raw_metadata.json`` / ``parsed_metadata.json`` entries.
It no longer reports reuse statistics now that the unified stages always emit
the same metadata shape for reused artifacts, but it retains counters for
"resume" flags so operators can spot forced restarts.

Usage:
    python scripts/data_checks/report_metadata_health.py [--data-root PATH]
                                                         [--stage STAGE ...]
                                                         [--verbose]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable


STAGES: tuple[str, ...] = ("draft", "essay", "evaluation")


def iter_gen_dirs(data_root: Path, stages: Iterable[str]) -> Iterable[tuple[str, Path | None]]:
    gens_root = data_root / "gens"
    for stage in stages:
        stage_dir = gens_root / stage
        if not stage_dir.exists():
            yield stage, None
            continue
        for gen_dir in sorted(p for p in stage_dir.iterdir() if p.is_dir()):
            yield stage, gen_dir


def load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Report gens metadata health")
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
        "--verbose",
        action="store_true",
        help="Print per-generation anomalies (missing/invalid metadata).",
    )
    args = parser.parse_args()

    if args.stage is None or "all" in args.stage:
        stages = STAGES
    else:
        seen: list[str] = []
        for value in args.stage:
            if value not in seen:
                seen.append(value)
        stages = tuple(seen)

    data_root: Path = args.data_root
    gens_root = data_root / "gens"
    verbose: bool = args.verbose

    if not gens_root.exists():
        print(f"ERROR: gens root not found at {gens_root}")
        return 1

    summary: dict[str, dict[str, int]] = {
        stage: {
            "generations": 0,
            "missing_raw_metadata": 0,
            "missing_parsed_metadata": 0,
            "invalid_raw_metadata": 0,
            "invalid_parsed_metadata": 0,
            "raw_resume": 0,
            "parsed_resume": 0,
        }
        for stage in stages
    }

    for stage, gen_dir in iter_gen_dirs(data_root, stages):
        if gen_dir is None:
            print(f"WARN: stage directory missing: {stage}")
            continue

        summary[stage]["generations"] += 1
        raw_md_path = gen_dir / "raw_metadata.json"
        parsed_md_path = gen_dir / "parsed_metadata.json"

        try:
            raw_md = load_json(raw_md_path)
        except ValueError:
            summary[stage]["invalid_raw_metadata"] += 1
            if verbose:
                rel = raw_md_path.relative_to(data_root)
                print(f"ERROR: invalid JSON in {rel}")
            raw_md = None

        try:
            parsed_md = load_json(parsed_md_path)
        except ValueError:
            summary[stage]["invalid_parsed_metadata"] += 1
            if verbose:
                rel = parsed_md_path.relative_to(data_root)
                print(f"ERROR: invalid JSON in {rel}")
            parsed_md = None

        raw_artifact_exists = (gen_dir / "raw.txt").exists()
        parsed_artifact_exists = (gen_dir / "parsed.txt").exists()

        if raw_md is None:
            if raw_artifact_exists:
                summary[stage]["missing_raw_metadata"] += 1
                if verbose and not raw_md_path.exists():
                    rel = raw_md_path.relative_to(data_root)
                    print(f"ERROR: missing {rel}")
        else:
            if raw_md.get("resume") or raw_md.get("resume_reason"):
                summary[stage]["raw_resume"] += 1

        if parsed_md is None:
            if parsed_artifact_exists:
                summary[stage]["missing_parsed_metadata"] += 1
                if verbose and not parsed_md_path.exists():
                    rel = parsed_md_path.relative_to(data_root)
                    print(f"ERROR: missing {rel}")
        else:
            if parsed_md.get("resume") or parsed_md.get("resume_reason"):
                summary[stage]["parsed_resume"] += 1

    print(f"Scanned gens at {gens_root}\n")
    header = (
        "Stage",
        "Generations",
        "Missing raw_md",
        "Missing parsed_md",
        "Invalid raw_md",
        "Invalid parsed_md",
        "raw resume",
        "parsed resume",
    )
    print("\t".join(header))
    for stage in stages:
        stats = summary[stage]
        row = [
            stage,
            str(stats["generations"]),
            str(stats["missing_raw_metadata"]),
            str(stats["missing_parsed_metadata"]),
            str(stats["invalid_raw_metadata"]),
            str(stats["invalid_parsed_metadata"]),
            str(stats["raw_resume"]),
            str(stats["parsed_resume"]),
        ]
        print("\t".join(row))

    # Non-zero missing/invalid stats should trigger a failing exit code.
    exit_code = 0
    for stats in summary.values():
        if (
            stats["missing_raw_metadata"]
            or stats["missing_parsed_metadata"]
            or stats["invalid_raw_metadata"]
            or stats["invalid_parsed_metadata"]
        ):
            exit_code = 1
            break
    if exit_code:
        print("\nFAIL: metadata gaps detected")
    else:
        print("\nOK: no missing/invalid metadata found")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
