#!/usr/bin/env python3
"""Remove legacy top-level `model_id` fields from generation metadata.

Usage:
  uv run python scripts/migrations/remove_model_id_fields.py --data-root data_staging
  uv run python scripts/migrations/remove_model_id_fields.py --data-root data --execute
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

STAGES = ("draft", "essay", "evaluation")
META_FILES = ("metadata.json", "raw_metadata.json", "parsed_metadata.json")


@dataclass
class FileUpdate:
    stage: str
    gen_id: str
    filename: str


@dataclass
class MigrationResult:
    updates: List[FileUpdate]
    dry_run: bool


def _strip_model_id(path: Path, *, execute: bool) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    changed = _remove_key(payload, "model_id")
    if changed and execute:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return changed


def _remove_key(value, target_key: str) -> bool:
    """Recursively remove `target_key` entries from dictionaries/lists."""

    changed = False
    if isinstance(value, dict):
        if target_key in value:
            del value[target_key]
            changed = True
        for key in list(value.keys()):
            if _remove_key(value[key], target_key):
                changed = True
    elif isinstance(value, list):
        for item in value:
            if _remove_key(item, target_key):
                changed = True
    return changed


def migrate(data_root: Path, *, execute: bool) -> MigrationResult:
    data_root = Path(data_root)
    updates: List[FileUpdate] = []
    for stage in STAGES:
        stage_dir = data_root / "gens" / stage
        if not stage_dir.exists():
            continue
        for gen_dir in stage_dir.iterdir():
            if not gen_dir.is_dir():
                continue
            gen_id = gen_dir.name
            for filename in META_FILES:
                path = gen_dir / filename
                if _strip_model_id(path, execute=execute):
                    updates.append(FileUpdate(stage=stage, gen_id=gen_id, filename=filename))
    return MigrationResult(updates=updates, dry_run=not execute)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Remove legacy model_id fields from generation metadata")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--execute", action="store_true", help="Persist removals in place")
    args = parser.parse_args(list(argv) if argv is not None else None)

    result = migrate(args.data_root, execute=args.execute)
    print(f"data_root={args.data_root}")
    print(f"updates={len(result.updates)}")
    if result.dry_run:
        print("mode=dry-run")
    else:
        print("mode=execute")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
