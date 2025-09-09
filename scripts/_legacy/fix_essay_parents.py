#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sqlite3
from typing import Optional


def load_mapping(data_root: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    essay_csv = data_root / "2_tasks" / "essay_generation_tasks.csv"
    if not essay_csv.exists():
        return mapping
    with essay_csv.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            e = row.get("essay_task_id")
            d = row.get("draft_task_id")
            if e and d and str(d).strip() and str(e).strip():
                mapping[str(e).strip()] = str(d).strip()
    return mapping


def find_draft_doc_id(con: sqlite3.Connection, draft_task_id: str) -> Optional[str]:
    cur = con.execute(
        "SELECT doc_id FROM documents WHERE stage='draft' AND task_id=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (draft_task_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def heuristic_match_draft(con: sqlite3.Connection, essay_task_id: str) -> Optional[str]:
    # essay_task_id format: combo__essay_template__model
    parts = essay_task_id.split("__")
    if len(parts) < 3:
        return None
    combo, essay_tpl, model = parts[0], parts[1], parts[2]
    # Prefer a draft for the same combo, regardless of model; bias to 'links' drafts if present
    cur = con.execute(
        "SELECT doc_id, task_id FROM documents WHERE stage='draft' AND task_id LIKE ? ORDER BY created_at DESC, rowid DESC",
        (f"{combo}__%",),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    # Try to pick a 'links' draft if present
    for r in rows:
        tid = r[1]
        if "links" in tid:
            return r[0]
    # Otherwise return the most recent
    return rows[0][0]


def fix_essay_parents(data_root: Path, db_path: Path, dry_run: bool) -> int:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    mapping = load_mapping(data_root)
    updated = 0
    missing = 0
    # Iterate over essay rows
    for row in con.execute("SELECT doc_id, task_id, parent_doc_id FROM documents WHERE stage='essay'"):
        essay_doc = row["doc_id"]
        essay_task = row["task_id"]
        parent = row["parent_doc_id"]
        if parent:
            continue
        draft_task_id = mapping.get(essay_task)
        draft_doc_id = None
        if draft_task_id:
            draft_doc_id = find_draft_doc_id(con, draft_task_id)
        if not draft_doc_id:
            draft_doc_id = heuristic_match_draft(con, essay_task)
        if draft_doc_id:
            if not dry_run:
                con.execute(
                    "UPDATE documents SET parent_doc_id=? WHERE doc_id=?",
                    (draft_doc_id, essay_doc),
                )
                con.commit()
            updated += 1
        else:
            missing += 1
    print(f"updated essays: {updated}; unresolved: {missing}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Fix parent_doc_id for essays to point to matched draft doc rows")
    ap.add_argument("--data-root", default=str(Path("data")), help="Data root path")
    ap.add_argument("--db", default=str(Path("data") / "db" / "documents.sqlite"), help="SQLite DB path")
    ap.add_argument("--dry-run", action="store_true", help="Report only without updating DB")
    args = ap.parse_args()
    return fix_essay_parents(Path(args.data_root), Path(args.db), args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
