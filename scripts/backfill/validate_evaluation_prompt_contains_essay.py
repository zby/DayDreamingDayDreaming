#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex


def read_text(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return None


def validate(db_path: Path, docs_root: Path) -> Dict:
    idx = SQLiteDocumentsIndex(db_path, docs_root)
    con = idx.connect()
    con.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}

    evals = con.execute("SELECT * FROM documents WHERE stage='evaluation' AND status='ok'").fetchall()
    checked = 0
    ok = 0
    violations: List[Dict] = []

    for ev in evals:
        checked += 1
        parent = ev.get("parent_doc_id")
        if not parent:
            violations.append({
                "evaluation_doc_id": ev["doc_id"],
                "evaluation_task_id": ev["task_id"],
                "problem": "missing_parent",
                "detail": "evaluation has no parent_doc_id",
            })
            continue
        essay = con.execute("SELECT * FROM documents WHERE doc_id=?", (parent,)).fetchone()
        if essay is None:
            violations.append({
                "evaluation_doc_id": ev["doc_id"],
                "evaluation_task_id": ev["task_id"],
                "problem": "parent_missing",
                "detail": f"no essay row for parent {parent}",
            })
            continue
        ev_dir = idx.resolve_doc_dir(ev)
        es_dir = idx.resolve_doc_dir(essay)
        ev_prompt = read_text(ev_dir / "prompt.txt")
        es_parsed = read_text(es_dir / "parsed.txt")
        if not ev_prompt or not es_parsed:
            violations.append({
                "evaluation_doc_id": ev["doc_id"],
                "evaluation_task_id": ev["task_id"],
                "problem": "missing_prompt_or_essay_parsed",
                "detail": f"ev_prompt={bool(ev_prompt)} essay_parsed={bool(es_parsed)}",
            })
            continue
        if es_parsed in ev_prompt:
            ok += 1
        else:
            violations.append({
                "evaluation_doc_id": ev["doc_id"],
                "evaluation_task_id": ev["task_id"],
                "problem": "prompt_missing_essay",
                "detail": "essay parsed.txt not contained within evaluation prompt.txt",
            })

    return {"checked": checked, "ok": ok, "violations": violations}


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate that evaluation prompt contains the parent essay parsed text")
    p.add_argument("--db", type=Path, default=Path("data/db/documents.sqlite"))
    p.add_argument("--docs-root", type=Path, default=Path("data/docs"))
    p.add_argument("--execute", action="store_true", help="Mark violating evaluations as skipped")
    p.add_argument("--out", type=Path, help="Optional CSV of violations")
    args = p.parse_args(argv)

    res = validate(args.db, args.docs_root)
    print(f"evals_checked={res['checked']}")
    print(f"evals_ok={res['ok']}")
    print(f"evals_violations={len(res['violations'])}")
    if args.out and res["violations"]:
        import csv
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["evaluation_doc_id","evaluation_task_id","problem","detail"]) 
            w.writeheader()
            for v in res["violations"]:
                w.writerow(v)

    if args.execute and res["violations"]:
        idx = SQLiteDocumentsIndex(args.db, args.docs_root)
        con = idx.connect()
        with con:
            for v in res["violations"]:
                doc_id = v["evaluation_doc_id"]
                # annotate meta_small with skipped reason
                row = con.execute("SELECT meta_small FROM documents WHERE doc_id=?", (doc_id,)).fetchone()
                try:
                    meta_obj = json.loads(row["meta_small"]) if row and row.get("meta_small") else {}
                except Exception:
                    meta_obj = {}
                meta_obj["skipped_reason"] = v["problem"]
                con.execute("UPDATE documents SET status='skipped', meta_small=? WHERE doc_id=?", (json.dumps(meta_obj, ensure_ascii=False), doc_id))
        print("marked_skipped=", len(res["violations"]))

    return 0 if not res["violations"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

