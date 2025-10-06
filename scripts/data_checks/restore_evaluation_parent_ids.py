"""Restore missing evaluation parent_gen_id fields in gens metadata.

The prior copy-mode cleanup dropped ``parent_gen_id`` from some evaluation
metadata files. This script rebuilds those links by matching on
``parent_task_id`` (+ replicate) against essay metadata.

Usage examples
--------------

Dry-run (default)::

    uv run python scripts/data_checks/restore_evaluation_parent_ids.py

Apply fixes in-place::

    uv run python scripts/data_checks/restore_evaluation_parent_ids.py --apply

Options
~~~~~~~
- ``--data-root``: override the data root (default: ``data``).
- ``--apply``: persist updates; otherwise the script only reports.
- ``--limit``: optional cap on the number of metadata files to mutate.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple


@dataclass(frozen=True)
class EssayKey:
    parent_task_id: str
    replicate: int


def _normalize_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_int(value: object | None, default: int = 1) -> int:
    if value is None:
        return default
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default


def _essay_parsed_text(data_root: Path, essay_id: str) -> str | None:
    parsed_path = data_root / "gens" / "essay" / essay_id / "parsed.txt"
    if not parsed_path.exists():
        return None
    try:
        return parsed_path.read_text(encoding="utf-8")
    except OSError:
        return None


def _evaluation_prompt_text(data_root: Path, evaluation_id: str) -> str | None:
    prompt_path = data_root / "gens" / "evaluation" / evaluation_id / "prompt.txt"
    if not prompt_path.exists():
        return None
    try:
        return prompt_path.read_text(encoding="utf-8")
    except OSError:
        return None


def _load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _iter_metadata(stage_dir: Path) -> Iterable[Tuple[Path, dict]]:
    if not stage_dir.exists():
        return
    for meta_path in sorted(stage_dir.glob("*/metadata.json")):
        meta = _load_json(meta_path)
        if meta is None:
            continue
        yield meta_path, meta


def _extract_parent_task(meta: dict) -> str | None:
    parent_task = _normalize_str(meta.get("parent_task_id"))
    if parent_task:
        return parent_task
    legacy_task = _normalize_str(meta.get("task_id"))
    if legacy_task:
        return legacy_task
    legacy_fields = meta.get("legacy_fields")
    if isinstance(legacy_fields, dict):
        parent_task = _normalize_str(legacy_fields.get("parent_task_id"))
        if parent_task:
            return parent_task
        parent_task = _normalize_str(legacy_fields.get("task_id"))
        if parent_task:
            return parent_task
    return None


def _build_essay_index(data_root: Path) -> Dict[EssayKey, str]:
    essay_dir = data_root / "gens" / "essay"
    index: Dict[EssayKey, str] = {}
    for meta_path, meta in _iter_metadata(essay_dir):
        parent_task = _extract_parent_task(meta)
        essay_id = meta_path.parent.name
        if not parent_task:
            continue
        replicate = _normalize_int(meta.get("replicate"), default=1)
        key = EssayKey(parent_task_id=parent_task, replicate=replicate)
        index[key] = essay_id
    return index


def _lookup_essay(
    *,
    essay_index: Dict[EssayKey, str],
    parent_task: str,
    replicate: int,
) -> tuple[str | None, str | None]:
    """Return matching essay id and a note describing the match mode."""

    key = EssayKey(parent_task_id=parent_task, replicate=replicate)
    if key in essay_index:
        return essay_index[key], "direct"

    # Try dropping the final suffix (commonly the essay template segment).
    if "_" in parent_task:
        base = parent_task.rsplit("_", 1)[0]
        base_key = EssayKey(parent_task_id=base, replicate=replicate)
        if base_key in essay_index:
            return essay_index[base_key], "trimmed_suffix"

    # As a last resort, try replicate fallback (1) when none exists for the requested replicate.
    if replicate != 1:
        fallback_key = EssayKey(parent_task_id=parent_task, replicate=1)
        if fallback_key in essay_index:
            return essay_index[fallback_key], "replicate_fallback"
        if "_" in parent_task:
            base = parent_task.rsplit("_", 1)[0]
            fallback_base = EssayKey(parent_task_id=base, replicate=1)
            if fallback_base in essay_index:
                return essay_index[fallback_base], "trimmed_suffix_rep1"

    return None, None


def restore_parent_ids(
    *,
    data_root: Path,
    apply: bool,
    limit: int | None,
) -> tuple[int, int]:
    essay_index = _build_essay_index(data_root)
    evaluation_dir = data_root / "gens" / "evaluation"

    fixed = 0
    missing = 0

    for meta_path, meta in _iter_metadata(evaluation_dir):
        parent_gen = _normalize_str(meta.get("parent_gen_id"))
        if parent_gen:
            continue

        parent_task = _normalize_str(meta.get("parent_task_id"))
        replicate = _normalize_int(meta.get("replicate"), default=1)
        essay_id, match_mode = (None, None)
        if parent_task:
            essay_id, match_mode = _lookup_essay(
                essay_index=essay_index,
                parent_task=parent_task,
                replicate=replicate,
            )

        if not parent_task or not essay_id:
            missing += 1
            print(f"SKIP  missing essay match for {meta_path.parent.name} (parent_task_id={parent_task!r}, replicate={replicate})")
            continue

        essay_text = _essay_parsed_text(data_root, essay_id)
        prompt_text = _evaluation_prompt_text(data_root, meta_path.parent.name)

        if not essay_text or not prompt_text:
            missing += 1
            print(
                f"SKIP  missing artifacts for {meta_path.parent.name}"
                f" (essay_parsed={'yes' if essay_text else 'no'}, prompt={'yes' if prompt_text else 'no'})"
            )
            continue

        if essay_text not in prompt_text:
            missing += 1
            print(
                f"SKIP  prompt mismatch for {meta_path.parent.name} -> {essay_id}"
                f" (mode={match_mode}, essay_len={len(essay_text)}, prompt_len={len(prompt_text)})"
            )
            continue

        print(
            f"FIX   {meta_path.parent.name}: assigning parent_gen_id={essay_id}"
            + (f" (mode={match_mode})" if match_mode else "")
        )
        if apply:
            meta["parent_gen_id"] = essay_id
            _write_json(meta_path, meta)
            fixed += 1
            if limit is not None and fixed >= limit:
                break
        else:
            fixed += 1
            if limit is not None and fixed >= limit:
                break

    return fixed, missing


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Path to the data root (default: data)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist updates instead of dry-run",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of metadata files to update",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fixed, missing = restore_parent_ids(
        data_root=args.data_root,
        apply=args.apply,
        limit=args.limit,
    )

    mode = "applied" if args.apply else "would update"
    print(f"\nSummary: {mode} parent_gen_id for {fixed} evaluation metadata entries")
    if missing:
        print(f"         {missing} entries lacked a matching essay parent")


if __name__ == "__main__":
    main()
