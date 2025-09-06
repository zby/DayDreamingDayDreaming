#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex


def main():
    ap = argparse.ArgumentParser(description="List latest doc_ids for given partitions or list some recent ones")
    ap.add_argument("--db", default=str(Path("data") / "db" / "documents.sqlite"))
    ap.add_argument("--docs-root", default=str(Path("data") / "docs"))
    ap.add_argument("--stage", choices=["draft", "essay", "evaluation"], help="Stage to query", required=False)
    ap.add_argument("--task-id", action="append", help="Task id (partition key). Can be provided multiple times.")
    ap.add_argument("--head", type=int, default=10, help="Show this many recent rows when no task-id is provided")
    args = ap.parse_args()

    idx = SQLiteDocumentsIndex(Path(args.db), Path(args.docs_root))
    idx.init_maybe_create_tables()
    con = idx.connect()

    if args.task_id and args.stage:
        for tid in args.task_id:
            row = idx.get_latest_by_task(args.stage, tid)
            if not row:
                print(f"{tid}: (none)")
            else:
                base = idx.resolve_doc_dir(row)
                print(f"{tid}: doc_id={row['doc_id']} logical={row['logical_key_id']} dir={base}")
        return 0

    # Recent rows overall (optionally filtered by stage)
    if args.stage:
        q = "SELECT * FROM documents WHERE stage=? ORDER BY created_at DESC, rowid DESC LIMIT ?"
        cur = con.execute(q, (args.stage, args.head))
    else:
        q = "SELECT * FROM documents ORDER BY created_at DESC, rowid DESC LIMIT ?"
        cur = con.execute(q, (args.head,))
    for row in cur:
        base = idx.resolve_doc_dir(row)
        print(f"{row['stage']}:{row['task_id']} doc_id={row['doc_id']} logical={row['logical_key_id']} dir={base}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

