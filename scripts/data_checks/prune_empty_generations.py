"""Report and prune empty gens-store directories.

A generation directory is considered *empty* when the primary artefacts
(`prompt.txt`, `raw.txt`, `parsed.txt`) are either missing or zero-length.
Empty generations are always reported. When ``--apply`` is specified, the
script deletes those directories whose ``origin_cohort_id`` no longer exists
under ``data/cohorts``.

Usage examples
--------------

Dry run (report only)::

    uv run python scripts/data_checks/prune_empty_generations.py

Delete empty generations whose cohorts were removed::

    uv run python scripts/data_checks/prune_empty_generations.py --apply
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


DEFAULT_STAGES = ("draft", "essay", "evaluation")
DATA_FILES = ("prompt.txt", "raw.txt", "parsed.txt")


@dataclass
class GenerationCandidate:
    stage: str
    gen_id: str
    prompt_bytes: int
    raw_bytes: int
    parsed_bytes: int
    origin_cohort_id: str | None
    cohort_exists: bool

    @property
    def any_payload(self) -> bool:
        return any(size > 0 for size in (self.prompt_bytes, self.raw_bytes, self.parsed_bytes))


def _load_metadata(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _iter_stage_generations(stage_dir: Path) -> Iterable[Path]:
    if not stage_dir.exists():
        return []
    return [p for p in stage_dir.iterdir() if p.is_dir()]


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def collect_candidates(
    *,
    data_root: Path,
    stages: Iterable[str],
) -> List[GenerationCandidate]:
    cohort_root = data_root / "cohorts"
    candidates: List[GenerationCandidate] = []

    for stage in stages:
        stage_dir = data_root / "gens" / stage
        for gen_dir in _iter_stage_generations(stage_dir):
            gen_id = gen_dir.name
            meta = _load_metadata(gen_dir / "metadata.json")
            origin = str(meta.get("origin_cohort_id") or "").strip() if meta else None

            prompt_bytes = _file_size(gen_dir / "prompt.txt")
            raw_bytes = _file_size(gen_dir / "raw.txt")
            parsed_bytes = _file_size(gen_dir / "parsed.txt")

            if prompt_bytes or raw_bytes or parsed_bytes:
                continue

            cohort_exists = False
            if origin and (cohort_root / origin).exists():
                membership_csv = cohort_root / origin / "membership.csv"
                if membership_csv.exists():
                    try:
                        with membership_csv.open("r", encoding="utf-8") as fh:
                            reader = csv.DictReader(fh)
                            for row in reader:
                                if str(row.get("stage")) == stage and str(row.get("gen_id")) == gen_id:
                                    cohort_exists = True
                                    break
                    except Exception:
                        cohort_exists = True
                else:
                    cohort_exists = True

            candidates.append(
                GenerationCandidate(
                    stage=stage,
                    gen_id=gen_id,
                    prompt_bytes=prompt_bytes,
                    raw_bytes=raw_bytes,
                    parsed_bytes=parsed_bytes,
                    origin_cohort_id=origin,
                    cohort_exists=cohort_exists,
                )
            )

    return candidates


def prune_candidates(
    *,
    candidates: List[GenerationCandidate],
    data_root: Path,
    apply: bool,
    limit: int | None,
) -> int:
    removed = 0
    kept = 0
    dryrun = 0
    for candidate in candidates:
        stage_dir = data_root / "gens" / candidate.stage
        gen_dir = stage_dir / candidate.gen_id

        base = (
            f"{candidate.stage}/{candidate.gen_id} origin={candidate.origin_cohort_id or 'N/A'}"
            f" cohort_exists={candidate.cohort_exists}"
        )

        if apply and not candidate.cohort_exists:
            print(f"REMOVE   {base}")
            shutil.rmtree(gen_dir, ignore_errors=True)
            removed += 1
            if limit is not None and removed >= limit:
                break
        else:
            status = "KEEP" if candidate.cohort_exists else "DRYRUN"
            print(f"{status:8s}{base}")
            if candidate.cohort_exists:
                kept += 1
            else:
                dryrun += 1

    if not apply:
        print(f"  total reported with cohorts: {kept}")
        print(f"  total reported without cohorts: {dryrun}")

    return removed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Path to the data root (default: data)",
    )
    parser.add_argument(
        "--stages",
        nargs="*",
        default=list(DEFAULT_STAGES),
        help="Stages to scan (default: draft essay evaluation)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete matching generation directories",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on removals",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stages = args.stages or list(DEFAULT_STAGES)
    candidates = collect_candidates(
        data_root=args.data_root,
        stages=stages,
    )

    if not candidates:
        print("No empty generations found.")
        return

    print("Found empty generations:")
    for cand in candidates:
        print(
            f"  {cand.stage}/{cand.gen_id}"
            f" origin={cand.origin_cohort_id or 'N/A'} cohort_exists={cand.cohort_exists}"
        )

    removed = prune_candidates(
        candidates=candidates,
        data_root=args.data_root,
        apply=args.apply,
        limit=args.limit,
    )

    if args.apply:
        print(f"Removed {removed} generation directories.")
    else:
        print("Dry run complete (use --apply to delete).")


if __name__ == "__main__":
    main()
