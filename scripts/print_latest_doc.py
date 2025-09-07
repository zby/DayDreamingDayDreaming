#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex


def main():
    ap = argparse.ArgumentParser(description="Print latest document row for a task or logical key")
    ap.add_argument("--db", default=str(Path("data") / "db" / "documents.sqlite"), help="Path to documents.sqlite")
    ap.add_argument("--docs-root", default=str(Path("data") / "docs"), help="Docs root")
    # Only task-based lookup is supported
    ap.add_argument("stage", choices=["draft", "essay", "evaluation"])
    ap.add_argument("task_id")

    args = ap.parse_args()
    idx = SQLiteDocumentsIndex(Path(args.db), Path(args.docs_root))
    idx.init_maybe_create_tables()

    row = idx.get_latest_by_task(args.stage, args.task_id)

    if not row:
        print("No row found")
        return 1
    base = idx.resolve_doc_dir(row)
    print("doc_id:", row.get("doc_id"))
    print("logical_key_id:", row.get("logical_key_id"))
    print("stage:", row.get("stage"))
    print("task_id:", row.get("task_id"))
    print("parent_doc_id:", row.get("parent_doc_id"))
    print("doc_dir:", str(base))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
