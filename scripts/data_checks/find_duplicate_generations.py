#!/usr/bin/env python3
"""
Detect duplicate generations by comparing raw.txt contents across stages.

Scans data/gens/<stage>/<gen_id>/raw.txt for each stage and groups documents
whose raw contents are identical. By default, empty raw files are ignored.

Supported stages: draft, essay, evaluation, all (default: all)

Usage examples:
  uv run python scripts/data_checks/find_duplicate_generations.py
  uv run python scripts/data_checks/find_duplicate_generations.py --stage draft --normalize
  uv run python scripts/data_checks/find_duplicate_generations.py \
    --data-root data --output /tmp/duplicate_generations.csv --include-empty

Exit codes:
  0  no duplicates found
  2  duplicates found (one or more stages)
  3  fatal error during scan
"""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple
import sys
import csv


STAGES = ("draft", "essay", "evaluation")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument(
        "--stage",
        choices=[*STAGES, "all"],
        default="all",
        help="Stage to scan (default: all)",
    )
    p.add_argument(
        "--normalize",
        action="store_true",
        help="Normalize line endings (CRLF→LF) and strip trailing whitespace before hashing",
    )
    p.add_argument(
        "--include-empty",
        action="store_true",
        help="Include empty raw.txt content when reporting duplicates (ignored by default)",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional CSV output path listing duplicate groups (stage,hash,count,gen_ids,preview)",
    )
    return p.parse_args()


def _content_hash(text: str, *, normalize: bool) -> Tuple[str, str]:
    """Return (hex_digest, preview) for given text.

    If normalize=True, converts CRLF to LF and strips trailing whitespace on each line
    to reduce false negatives due to formatting differences.
    """
    if normalize:
        normalized = "\n".join(line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"))
    else:
        normalized = text
    digest = hashlib.sha256(normalized.encode("utf-8", errors="ignore")).hexdigest()
    preview = normalized.strip().splitlines()[0] if normalized.strip() else ""
    return digest, preview


def scan_stage_raw(data_root: Path, stage: str, *, normalize: bool, include_empty: bool) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
    """Scan <data_root>/gens/<stage> raw files and return mapping hash -> [gen_ids], and hash -> preview line."""
    assert stage in STAGES, stage
    stage_dir = data_root / "gens" / stage
    groups: Dict[str, List[str]] = {}
    preview_map: Dict[str, str] = {}
    if not stage_dir.exists():
        print(f"No {stage} directory found: {stage_dir}")
        return groups, preview_map
    for child in stage_dir.iterdir():
        if not child.is_dir():
            continue
        gen_id = child.name
        raw_fp = child / "raw.txt"
        if not raw_fp.exists():
            continue
        try:
            text = raw_fp.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            print(f"Warning: failed to read {raw_fp}: {e}", file=sys.stderr)
            continue
        if not include_empty and (not text.strip()):
            continue
        h, preview = _content_hash(text, normalize=normalize)
        groups.setdefault(h, []).append(gen_id)
        if h not in preview_map:
            preview_map[h] = preview[:120]
    return groups, preview_map


def write_csv(stage_groups: Dict[str, Dict[str, List[str]]], stage_previews: Dict[str, Dict[str, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["stage", "hash", "count", "gen_ids", "preview_first_line"])
        for stage in STAGES:
            groups = stage_groups.get(stage, {})
            if not groups:
                continue
            for h, ids in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
                if len(ids) <= 1:
                    continue
                w.writerow([stage, h, len(ids), ";".join(sorted(ids)), stage_previews.get(stage, {}).get(h, "")])
    print(f"Wrote duplicate groups to {out_path}")


def main() -> int:
    args = parse_args()
    stages = STAGES if args.stage == "all" else (args.stage,)

    try:
        stage_groups: Dict[str, Dict[str, List[str]]] = {}
        stage_previews: Dict[str, Dict[str, str]] = {}
        any_dups = False
        for stage in stages:
            groups, previews = scan_stage_raw(
                args.data_root, stage, normalize=args.normalize, include_empty=args.include_empty
            )
            stage_groups[stage] = groups
            stage_previews[stage] = previews

            dup_groups = {h: ids for h, ids in groups.items() if len(ids) > 1}
            total_docs = sum(len(v) for v in groups.values())
            total_dups = sum(len(v) for v in dup_groups.values())
            print(f"\n[{stage}] scanned parsed files: {total_docs} considered")
            print(f"[{stage}] duplicate groups: {len(dup_groups)} (docs in duplicate groups: {total_dups})")

            # Show top few groups per stage
            shown = 0
            for h, ids in sorted(dup_groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
                print(f"\n== [{stage}] Duplicate group (count={len(ids)}), hash={h}")
                print(f"preview: {stage_previews[stage].get(h, '')}")
                print("gen_ids:")
                for gid in sorted(ids)[:20]:
                    print(f"  - {gid}")
                if len(ids) > 20:
                    print(f"  ... (+{len(ids) - 20} more)")
                shown += 1
                if shown >= 10:
                    break
            any_dups = any_dups or bool(dup_groups)

        if args.output:
            write_csv(stage_groups, stage_previews, args.output)

        return 2 if any_dups else 0
    except Exception as e:
        print(f"ERROR: scan failed: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
