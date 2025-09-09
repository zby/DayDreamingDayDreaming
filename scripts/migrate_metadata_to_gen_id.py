#!/usr/bin/env python3
"""
Migrate gens metadata.json files from doc-id keys to gen-id keys.

Renames keys inside data/gens/<stage>/<gen_id>/metadata.json:
- parent_doc_id -> parent_gen_id
- draft_doc_id  -> draft_gen_id

Leaves other keys intact (task_id, template_id, model_id, function, etc.).
Writes files atomically and prints a short summary.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import json


def migrate_file(fp: Path) -> tuple[bool, dict]:
    try:
        meta = json.loads(fp.read_text(encoding="utf-8")) or {}
        if not isinstance(meta, dict):
            return False, {}
    except Exception:
        return False, {}
    changed = False
    # parent_* rename
    if "parent_doc_id" in meta:
        if "parent_gen_id" not in meta and isinstance(meta.get("parent_doc_id"), str):
            meta["parent_gen_id"] = meta["parent_doc_id"]
        del meta["parent_doc_id"]
        changed = True
    if "draft_doc_id" in meta:
        if "draft_gen_id" not in meta and isinstance(meta.get("draft_doc_id"), str):
            meta["draft_gen_id"] = meta["draft_doc_id"]
        del meta["draft_doc_id"]
        changed = True
    if changed:
        tmp = fp.with_suffix(fp.suffix + ".tmp")
        tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(fp)
    return changed, meta


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate gens metadata.json keys to gen-id schema")
    ap.add_argument("--root", type=Path, default=Path("data/gens"), help="Root gens directory (default: data/gens)")
    args = ap.parse_args()

    root: Path = args.root
    if not root.exists():
        print(f"Root not found: {root}")
        return 1
    total = 0
    changed = 0
    for stage in ("draft", "essay", "evaluation"):
        base = root / stage
        if not base.exists():
            continue
        for gen_dir in base.iterdir():
            if not gen_dir.is_dir():
                continue
            fp = gen_dir / "metadata.json"
            if not fp.exists():
                continue
            total += 1
            c, _ = migrate_file(fp)
            if c:
                changed += 1
    print(f"Scanned {total} metadata.json; updated {changed} with gen-id keys")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

