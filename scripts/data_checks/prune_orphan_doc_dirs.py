#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sqlite3
from pathlib import Path
from typing import Set, List


def load_doc_ids(db_path: Path) -> Set[str]:
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute("SELECT doc_id FROM documents")
        return {row[0] for row in cur.fetchall() if row and row[0]}
    finally:
        con.close()


def prune_stage(stage_dir: Path, keep_ids: Set[str], *, execute: bool) -> dict:
    to_delete: List[Path] = []
    examined = 0
    deleted = 0
    # Flat layout: stage/<doc_id>
    if stage_dir.exists():
        for child in stage_dir.iterdir():
            if not child.is_dir():
                continue
            examined += 1
            if child.name not in keep_ids:
                to_delete.append(child)
    # Perform deletions
    for d in to_delete:
        if execute:
            shutil.rmtree(d, ignore_errors=True)
        deleted += 1
    return {
        'examined': examined,
        'to_delete': len(to_delete),
        'deleted': deleted,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Prune orphan document directories not present in the DB")
    p.add_argument("--db", type=Path, default=Path("data/db/documents.sqlite"))
    p.add_argument("--docs-root", type=Path, default=Path("data/docs"))
    p.add_argument("--execute", action="store_true", help="Actually delete directories (otherwise dry-run)")
    args = p.parse_args(argv)

    keep_ids = load_doc_ids(args.db)
    print(f"loaded_doc_ids={len(keep_ids)}")

    total_examined = 0
    total_to_delete = 0
    total_deleted = 0
    for stage in ("draft", "essay", "evaluation"):
        stage_dir = args.docs_root / stage
        res = prune_stage(stage_dir, keep_ids, execute=args.execute)
        print(f"stage={stage} examined={res['examined']} to_delete={res['to_delete']} deleted={res['deleted']}")
        total_examined += res['examined']
        total_to_delete += res['to_delete']
        total_deleted += res['deleted']

    print(f"summary examined={total_examined} to_delete={total_to_delete} deleted={total_deleted} dry_run={not args.execute}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
