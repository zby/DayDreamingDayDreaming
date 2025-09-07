#!/usr/bin/env python3
"""
Backfill template_id into metadata.json for all documents using the SQLite documents table.

Reads rows from the documents DB (data/db/documents.sqlite by default), and for each row
updates the corresponding docs/<stage>/<doc_id>/metadata.json to include a "template_id"
field if missing or incorrect.

Usage:
  uv run python scripts/backfill_template_id_in_metadata.py \
    --data-root data --db data/db/documents.sqlite --dry-run

Options:
  --data-root   Base data directory (default: data)
  --db          Path to documents.sqlite (default: <data-root>/db/documents.sqlite)
  --stage       Optional stage filter: draft|essay|evaluation
  --limit       Optional limit on number of rows processed
  --dry-run     Show planned changes without writing files
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Optional


def _doc_dir(root: Path, stage: str, doc_id: str) -> Path:
    return root / "docs" / stage / doc_id


def _load_metadata(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        return {}


def _write_metadata(path: Path, data: dict, dry_run: bool) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        return True
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return True


def backfill(data_root: Path, db_path: Path, stage: Optional[str], limit: Optional[int], dry_run: bool) -> dict:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    where = []
    args: list = []
    if stage:
        where.append("stage=?")
        args.append(stage)
    where.append("template_id IS NOT NULL AND template_id != ''")
    q = "SELECT doc_id, stage, template_id FROM documents"
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY created_at ASC, rowid ASC"
    if isinstance(limit, int) and limit > 0:
        q += f" LIMIT {int(limit)}"

    docs_root = Path(data_root)
    updated = 0
    skipped = 0
    missing = 0
    mismatched = 0
    processed = 0
    for row in con.execute(q, args):
        processed += 1
        doc_id = str(row["doc_id"])  # type: ignore[index]
        stg = str(row["stage"])     # type: ignore[index]
        tpl = str(row["template_id"])  # type: ignore[index]
        meta_path = _doc_dir(docs_root, stg, doc_id) / "metadata.json"
        if not meta_path.parent.exists():
            missing += 1
            continue
        meta = _load_metadata(meta_path)
        current = str(meta.get("template_id") or "")
        if not current:
            meta["template_id"] = tpl
            if _write_metadata(meta_path, meta, dry_run):
                updated += 1
        elif current != tpl:
            # Keep DB as source of truth; overwrite
            meta["template_id"] = tpl
            if _write_metadata(meta_path, meta, dry_run):
                mismatched += 1
        else:
            skipped += 1

    return {
        "processed": processed,
        "updated": updated,
        "mismatched_overwritten": mismatched,
        "skipped": skipped,
        "missing_dirs": missing,
        "dry_run": dry_run,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill template_id into docs metadata.json from documents DB")
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--db", type=Path, default=None, help="Path to documents.sqlite (default: <data-root>/db/documents.sqlite)")
    ap.add_argument("--stage", choices=["draft", "essay", "evaluation"], default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    data_root: Path = args.data_root
    db_path: Path = args.db or (data_root / "db" / "documents.sqlite")

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    result = backfill(data_root, db_path, args.stage, args.limit, args.dry_run)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

