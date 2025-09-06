#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import sqlite3


def sha256_text(p: Path) -> str:
    data = p.read_bytes()
    return hashlib.sha256(data).hexdigest()


def enrich(db: Path, docs_root: Path, limit: int | None = None) -> int:
    con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    cur = con.execute("SELECT doc_id, doc_dir, content_hash FROM documents")
    count = 0
    for row in cur:
        if limit and count >= limit:
            break
        if row["content_hash"]:
            continue
        base = docs_root / row["doc_dir"]
        raw = base / "raw.txt"
        if not raw.exists():
            continue
        try:
            h = sha256_text(raw)
            con.execute("UPDATE documents SET content_hash=? WHERE doc_id=?", (h, row["doc_id"]))
            con.commit()
            count += 1
        except Exception:
            continue
    print(f"updated content_hash for {count} rows")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Compute and store content_hash for documents (raw.txt)")
    ap.add_argument("--db", default=str(Path("data")/"db"/"documents.sqlite"))
    ap.add_argument("--docs-root", default=str(Path("data")/"docs"))
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    return enrich(Path(args.db), Path(args.docs_root), args.limit or None)


if __name__ == "__main__":
    raise SystemExit(main())

