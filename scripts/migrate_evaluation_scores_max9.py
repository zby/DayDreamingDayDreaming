"""Migration script to reset evaluation generations with scores above the cap.

Steps performed per offending evaluation gen_id:
- ensure the evaluation metadata uses `mode: "llm"` so Dagster will re-run it
- delete raw/parsed artifacts so new runs are forced to regenerate

After running (without --dry-run), materialize `evaluation_prompt,evaluation_raw,evaluation_parsed`
for each listed gen_id to repopulate outputs, then rebuild aggregated scores/pivots.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable, Set

DEFAULT_MAX_SCORE = 9.0
AGGREGATED_CSV = Path("data/5_parsing/aggregated_scores.csv")
DATA_ROOT = Path("data")


def load_offending_gen_ids(aggregated_path: Path, max_score: float) -> Set[str]:
    offending: Set[str] = set()
    if not aggregated_path.exists():
        raise FileNotFoundError(f"Aggregated scores CSV not found: {aggregated_path}")

    with aggregated_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if "score" not in reader.fieldnames or "gen_id" not in reader.fieldnames:
            raise ValueError(
                "Aggregated scores CSV missing required columns 'score' and 'gen_id'"
            )

        for row in reader:
            try:
                value = float(row["score"])
            except (TypeError, ValueError):
                continue
            if value > max_score:
                gen_id = (row.get("gen_id") or "").strip()
                if gen_id:
                    offending.add(gen_id)
    return offending


def ensure_llm_mode(metadata_path: Path, *, dry_run: bool) -> str:
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing metadata.json for {metadata_path.parent}")

    with metadata_path.open(encoding="utf-8") as handle:
        metadata = json.load(handle)

    current_mode = metadata.get("mode")
    if (current_mode or "").strip().lower() == "llm":
        return "already_llm"

    metadata["mode"] = "llm"

    if dry_run:
        return "would_update"

    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    return "updated"


def remove_artifacts(gen_dir: Path, *, dry_run: bool) -> list[str]:
    removed: list[str] = []
    to_remove = [
        gen_dir / "raw.txt",
        gen_dir / "raw_metadata.json",
        gen_dir / "parsed.txt",
        gen_dir / "parsed_metadata.json",
    ]

    for path in to_remove:
        if path.exists():
            removed.append(path.name)
            if not dry_run:
                path.unlink()
    return removed


def iter_gen_dirs(data_root: Path, gen_ids: Iterable[str]) -> Iterable[Path]:
    for gen_id in gen_ids:
        gen_dir = data_root / "gens" / "evaluation" / gen_id
        yield gen_id, gen_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reset evaluation generations whose parsed scores exceed the allowed maximum."
        )
    )
    parser.add_argument(
        "--aggregated",
        type=Path,
        default=AGGREGATED_CSV,
        help="Path to aggregated_scores.csv (default: data/5_parsing/aggregated_scores.csv)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DATA_ROOT,
        help="Root data directory (default: data)",
    )
    parser.add_argument(
        "--max-score",
        type=float,
        default=DEFAULT_MAX_SCORE,
        help="Upper bound for valid scores (default: 9.0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report actions without modifying any files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    offending = load_offending_gen_ids(args.aggregated, args.max_score)
    if not offending:
        print(
            f"No evaluations above {args.max_score} detected in {args.aggregated}. "
            "Nothing to do."
        )
        return

    print(
        f"Found {len(offending)} evaluation generations with scores above {args.max_score}."
    )

    for gen_id, gen_dir in iter_gen_dirs(args.data_root, sorted(offending)):
        if not gen_dir.exists():
            print(f"- {gen_id}: MISSING directory {gen_dir}")
            continue

        metadata_path = gen_dir / "metadata.json"
        try:
            mode_result = ensure_llm_mode(metadata_path, dry_run=args.dry_run)
        except FileNotFoundError as exc:
            print(f"- {gen_id}: {exc}")
            continue

        removed = remove_artifacts(gen_dir, dry_run=args.dry_run)
        if mode_result == "already_llm":
            mode_note = "mode already llm"
        elif mode_result == "would_update":
            mode_note = "would set mode to llm"
        else:
            mode_note = "updated mode to llm"
        if removed:
            action = "would remove" if args.dry_run else "removed"
            removed_note = f", {action} " + ", ".join(removed)
        else:
            removed_note = ", nothing removed"
        print(f"- {gen_id}: {mode_note}{removed_note}")

    print("\nNext steps:")
    print("1. For each gen_id above, wipe Dagster assets if desired:")
    print(
        "   uv run dagster asset wipe --asset evaluation_raw --partition <gen_id> -f src/daydreaming_dagster/definitions.py"
    )
    print(
        "   uv run dagster asset wipe --asset evaluation_parsed --partition <gen_id> -f src/daydreaming_dagster/definitions.py"
    )
    print("2. Re-materialize the evaluations:")
    print(
        "   uv run dagster asset materialize --select \"evaluation_prompt,evaluation_raw,evaluation_parsed\" --partition <gen_id> -f src/daydreaming_dagster/definitions.py"
    )
    print("3. Rebuild aggregated scores and pivots:")
    print(
        "   uv run dagster asset materialize --select aggregated_scores -f src/daydreaming_dagster/definitions.py"
    )
    print(
        "   .venv/bin/python scripts/build_pivot_tables.py --limit-to-active-templates"
    )


if __name__ == "__main__":
    main()
