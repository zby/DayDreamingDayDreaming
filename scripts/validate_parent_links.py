#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class Issue:
    child_doc_id: str
    child_stage: str
    child_task_id: str
    parent_doc_id: str
    problem: str
    parent_stage: Optional[str] = None
    parent_task_id: Optional[str] = None
    expected_parent_task_id: Optional[str] = None


def load_rows(db_path: Path) -> list[dict]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}
    rows = con.execute(
        "SELECT * FROM documents WHERE parent_doc_id IS NOT NULL ORDER BY created_at"
    ).fetchall()
    con.close()
    return rows


def get_parent(con: sqlite3.Connection, doc_id: str) -> Optional[dict]:
    con.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}
    return con.execute("SELECT * FROM documents WHERE doc_id=? LIMIT 1", (doc_id,)).fetchone()


def validate(db_path: Path) -> dict:
    issues: List[Issue] = []
    con = sqlite3.connect(str(db_path))
    con.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}
    children = con.execute(
        "SELECT * FROM documents WHERE parent_doc_id IS NOT NULL ORDER BY created_at"
    ).fetchall()
    # Build child index by parent_doc_id to assess coverage
    child_index = {}
    for ch in children:
        child_index.setdefault(ch["parent_doc_id"], []).append(ch)
    essays = con.execute("SELECT * FROM documents WHERE stage='essay'").fetchall()
    essays_without_eval = []
    for es in essays:
        kids = child_index.get(es["doc_id"], [])
        if not any(k["stage"] == "evaluation" for k in kids):
            essays_without_eval.append(es)
    for ch in children:
        child_stage = ch.get("stage")
        child_task_id = ch.get("task_id")
        parent_id = ch.get("parent_doc_id")
        meta_small = ch.get("meta_small")
        expected_parent_task_id = None
        if isinstance(meta_small, str) and meta_small.strip():
            try:
                ms = json.loads(meta_small)
                expected_parent_task_id = ms.get("parent_task_id")
            except Exception:
                pass

        pr = get_parent(con, str(parent_id)) if parent_id else None
        if pr is None:
            issues.append(
                Issue(
                    child_doc_id=ch["doc_id"],
                    child_stage=child_stage,
                    child_task_id=child_task_id,
                    parent_doc_id=str(parent_id),
                    problem="parent_missing",
                    expected_parent_task_id=expected_parent_task_id,
                )
            )
            continue

        parent_stage = pr.get("stage")
        parent_task_id = pr.get("task_id")

        # Stage compatibility checks
        if child_stage == "essay" and parent_stage != "draft":
            issues.append(
                Issue(
                    child_doc_id=ch["doc_id"],
                    child_stage=child_stage,
                    child_task_id=child_task_id,
                    parent_doc_id=str(parent_id),
                    parent_stage=parent_stage,
                    parent_task_id=parent_task_id,
                    problem="stage_mismatch_expected_draft",
                    expected_parent_task_id=expected_parent_task_id,
                )
            )
        elif child_stage == "evaluation" and parent_stage != "essay":
            issues.append(
                Issue(
                    child_doc_id=ch["doc_id"],
                    child_stage=child_stage,
                    child_task_id=child_task_id,
                    parent_doc_id=str(parent_id),
                    parent_stage=parent_stage,
                    parent_task_id=parent_task_id,
                    problem="stage_mismatch_expected_essay",
                    expected_parent_task_id=expected_parent_task_id,
                )
            )

        # If child meta recorded parent_task_id, verify it matches parent's task_id
        if expected_parent_task_id and parent_task_id and expected_parent_task_id != parent_task_id:
            issues.append(
                Issue(
                    child_doc_id=ch["doc_id"],
                    child_stage=child_stage,
                    child_task_id=child_task_id,
                    parent_doc_id=str(parent_id),
                    parent_stage=parent_stage,
                    parent_task_id=parent_task_id,
                    problem="parent_task_id_mismatch",
                    expected_parent_task_id=expected_parent_task_id,
                )
            )

    con.close()
    return {
        "checked": len(children),
        "issues": issues,
        "essays_total": len(essays),
        "essays_without_eval": [{"doc_id": e["doc_id"], "task_id": e["task_id"]} for e in essays_without_eval],
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate parent_doc_id relations in documents DB")
    p.add_argument("--db", type=Path, default=Path("data/db/documents.sqlite"))
    p.add_argument("--fail-on-issues", action="store_true", help="Exit with non-zero status if issues found")
    p.add_argument("--out", type=Path, help="Optional CSV to write issues")
    p.add_argument("--out-essays-without-evals", type=Path, help="Optional CSV listing essays with no evaluation children")
    args = p.parse_args(argv)

    res = validate(args.db)
    issues: List[Issue] = res["issues"]
    print(f"children_checked={res['checked']}")
    print(f"issues_found={len(issues)}")
    print(f"essays_total={res['essays_total']}")
    print(f"essays_without_eval_children={len(res['essays_without_eval'])}")

    if args.out and issues:
        import csv

        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "child_doc_id",
                    "child_stage",
                    "child_task_id",
                    "parent_doc_id",
                    "parent_stage",
                    "parent_task_id",
                    "expected_parent_task_id",
                    "problem",
                ],
            )
            w.writeheader()
            for it in issues:
                w.writerow(it.__dict__)

    # Optional report for essays without eval children
    if args.out_essays_without_evals and res["essays_without_eval"]:
        import csv
        args.out_essays_without_evals.parent.mkdir(parents=True, exist_ok=True)
        with args.out_essays_without_evals.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["doc_id", "task_id"])
            w.writeheader()
            w.writerows(res["essays_without_eval"])

    if (issues or res["essays_without_eval"]) and args.fail_on_issues:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
