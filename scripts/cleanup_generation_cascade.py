#!/usr/bin/env python3
"""Delete a generation and all its descendants (children, grandchildren, etc.).

Usage:
    python scripts/cleanup_generation_cascade.py draft d_abc123
    python scripts/cleanup_generation_cascade.py essay e_xyz789

This removes all artifacts for the specified generation and recursively deletes
all downstream generations. After deletion, Dagster will regenerate them on the
next materialization (since raw.txt/parsed.txt will be missing).

Stage hierarchy:
    draft → essay → evaluation
"""
import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import List


def find_children(gens_root: Path, stage: str, parent_gen_id: str) -> List[str]:
    """Find all gen_ids in the child stage that have parent_gen_id as their parent."""
    child_stage = None
    if stage == "draft":
        child_stage = "essay"
    elif stage == "essay":
        child_stage = "evaluation"

    if child_stage is None:
        return []

    child_stage_dir = gens_root / child_stage
    if not child_stage_dir.exists():
        return []

    children = []
    for gen_dir in child_stage_dir.iterdir():
        if not gen_dir.is_dir():
            continue

        metadata_path = gen_dir / "metadata.json"
        if not metadata_path.exists():
            continue

        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("parent_gen_id") == parent_gen_id:
                children.append(gen_dir.name)
        except (OSError, json.JSONDecodeError):
            continue

    return children


def delete_generation_cascade(
    gens_root: Path, stage: str, gen_id: str, *, dry_run: bool = False
) -> List[str]:
    """Recursively delete a generation and all its descendants.

    Returns list of deleted gen_id paths for logging.
    """
    deleted = []

    # First, recursively delete children
    children = find_children(gens_root, stage, gen_id)
    for child_id in children:
        child_stage = "essay" if stage == "draft" else "evaluation"
        grandchildren = delete_generation_cascade(
            gens_root, child_stage, child_id, dry_run=dry_run
        )
        deleted.extend(grandchildren)

    # Then delete this generation
    gen_dir = gens_root / stage / gen_id
    if gen_dir.exists():
        if dry_run:
            print(f"[DRY-RUN] Would delete: {gen_dir}")
        else:
            shutil.rmtree(gen_dir)
            print(f"Deleted: {gen_dir}")
        deleted.append(f"{stage}/{gen_id}")

    return deleted


def main():
    parser = argparse.ArgumentParser(
        description="Delete a generation and all its descendants",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("stage", choices=["draft", "essay", "evaluation"], help="Stage name")
    parser.add_argument("gen_id", help="Generation ID to delete")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Data root directory (default: data)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )

    args = parser.parse_args()

    gens_root = args.data_root / "gens"
    if not gens_root.exists():
        print(f"Error: gens directory not found at {gens_root}", file=sys.stderr)
        sys.exit(1)

    gen_dir = gens_root / args.stage / args.gen_id
    if not gen_dir.exists():
        print(
            f"Error: generation {args.stage}/{args.gen_id} not found at {gen_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Cleaning up {args.stage}/{args.gen_id} and all descendants...")
    deleted = delete_generation_cascade(
        gens_root, args.stage, args.gen_id, dry_run=args.dry_run
    )

    if args.dry_run:
        print(f"\n[DRY-RUN] Would delete {len(deleted)} generation(s):")
    else:
        print(f"\nDeleted {len(deleted)} generation(s):")
    for path in deleted:
        print(f"  - {path}")

    if not args.dry_run:
        print(
            "\nRun dagster materialization to regenerate the deleted artifacts."
        )


if __name__ == "__main__":
    main()
