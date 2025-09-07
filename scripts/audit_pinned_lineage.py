#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


@dataclass
class DbHandle:
    con: sqlite3.Connection

    def has_row(self, stage: str, doc_id: str) -> bool:
        try:
            cur = self.con.execute(
                "SELECT 1 FROM documents WHERE stage=? AND doc_id=? LIMIT 1",
                (str(stage), str(doc_id)),
            )
            return cur.fetchone() is not None
        except Exception:
            return False


def _open_db(db_path: Path) -> Optional[DbHandle]:
    if not db_path.exists():
        return None
    con = sqlite3.connect(str(db_path))
    return DbHandle(con=con)


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _eval_stage_from_row(row: pd.Series) -> Optional[str]:
    # Prefer explicit source_stage column if present
    if "source_stage" in row and isinstance(row["source_stage"], str) and row["source_stage"].strip():
        s = row["source_stage"].strip().lower()
        if s in ("draft", "essay", "evaluation"):
            return s
    # Fallback to generic stage column (e.g., essay2p/essay1p)
    if "stage" in row and isinstance(row["stage"], str) and row["stage"].strip():
        s = row["stage"].strip().lower()
        return "essay" if s.startswith("essay") else ("draft" if s.startswith("draft") else None)
    # Default: None (unknown)
    return None


def _doc_dir(docs_root: Path, stage: str, doc_id: str) -> Path:
    # Our current layout is flat by stage/doc_id
    return docs_root / stage / doc_id


def audit_essays(
    essay_csv: Path, docs_root: Path, db: Optional[DbHandle]
) -> Tuple[pd.DataFrame, dict]:
    df = _read_csv_if_exists(essay_csv)
    problems: list[dict] = []
    ok = 0
    total = len(df) if not df.empty else 0

    if df.empty:
        return pd.DataFrame(), {"total": 0, "ok": 0, "missing_parent_doc_id": 0, "missing_on_disk": 0, "missing_in_db": 0}

    has_parent = "parent_doc_id" in df.columns
    for _, row in df.iterrows():
        essay_task_id = str(row.get("essay_task_id"))
        parent_doc_id = str(row.get("parent_doc_id")) if has_parent else ""
        if not parent_doc_id or parent_doc_id.lower() == "nan":
            problems.append(
                {
                    "essay_task_id": essay_task_id,
                    "issue": "missing_parent_doc_id",
                }
            )
            continue
        stage = "draft"
        ddir = _doc_dir(docs_root, stage, parent_doc_id)
        exists_dir = ddir.exists()
        exists_file = (ddir / "parsed.txt").exists() or (ddir / "raw.txt").exists()
        exists_db = db.has_row(stage, parent_doc_id) if db else None
        if not exists_dir or not exists_file or (db and not exists_db):
            problems.append(
                {
                    "essay_task_id": essay_task_id,
                    "parent_doc_id": parent_doc_id,
                    "exists_dir": exists_dir,
                    "has_text": exists_file,
                    **({"in_db": exists_db} if db else {}),
                    "issue": "missing_on_disk_or_db",
                }
            )
        else:
            ok += 1

    problems_df = pd.DataFrame(problems)
    summary = {
        "total": total,
        "ok": ok,
        "missing_parent_doc_id": int((problems_df["issue"] == "missing_parent_doc_id").sum()) if not problems_df.empty else 0,
        "missing_on_disk": int((problems_df["issue"] == "missing_on_disk_or_db").sum()) if not problems_df.empty else 0,
        "missing_in_db": int(
            problems_df.get("in_db").apply(lambda v: v is False).sum()
        ) if (db and not problems_df.empty and "in_db" in problems_df.columns) else 0,
    }
    return problems_df, summary


def audit_evaluations(
    eval_csv: Path, docs_root: Path, db: Optional[DbHandle]
) -> Tuple[pd.DataFrame, dict]:
    df = _read_csv_if_exists(eval_csv)
    problems: list[dict] = []
    ok = 0
    total = len(df) if not df.empty else 0

    if df.empty:
        return pd.DataFrame(), {"total": 0, "ok": 0, "missing_target_doc_id": 0, "missing_stage": 0, "missing_on_disk": 0, "missing_in_db": 0}

    has_parent = "parent_doc_id" in df.columns
    has_target_legacy = "target_doc_id" in df.columns
    for _, row in df.iterrows():
        evaluation_task_id = str(row.get("evaluation_task_id"))
        # Prefer parent_doc_id (essay doc). Fall back to legacy target_doc_id if present.
        target_doc_id = ""
        if has_parent:
            target_doc_id = str(row.get("parent_doc_id"))
        if (not target_doc_id or target_doc_id.lower() == "nan") and has_target_legacy:
            target_doc_id = str(row.get("target_doc_id"))
        if not target_doc_id or target_doc_id.lower() == "nan":
            problems.append(
                {
                    "evaluation_task_id": evaluation_task_id,
                    "issue": "missing_parent_doc_id",
                }
            )
            continue
        stage = _eval_stage_from_row(row)
        if not stage:
            problems.append(
                {
                    "evaluation_task_id": evaluation_task_id,
                    "parent_doc_id": target_doc_id,
                    "issue": "missing_source_stage",
                }
            )
            continue
        ddir = _doc_dir(docs_root, stage, target_doc_id)
        exists_dir = ddir.exists()
        exists_file = (ddir / "parsed.txt").exists() or (ddir / "raw.txt").exists()
        exists_db = db.has_row(stage, target_doc_id) if db else None
        if not exists_dir or not exists_file or (db and not exists_db):
            problems.append(
                {
                    "evaluation_task_id": evaluation_task_id,
                    "parent_doc_id": target_doc_id,
                    "stage": stage,
                    "exists_dir": exists_dir,
                    "has_text": exists_file,
                    **({"in_db": exists_db} if db else {}),
                    "issue": "missing_on_disk_or_db",
                }
            )
        else:
            ok += 1

    problems_df = pd.DataFrame(problems)
    summary = {
        "total": total,
        "ok": ok,
        "missing_parent_doc_id": int((problems_df["issue"] == "missing_parent_doc_id").sum()) if not problems_df.empty else 0,
        "missing_stage": int((problems_df["issue"] == "missing_source_stage").sum()) if not problems_df.empty else 0,
        "missing_on_disk": int((problems_df["issue"] == "missing_on_disk_or_db").sum()) if not problems_df.empty else 0,
        "missing_in_db": int(
            problems_df.get("in_db").apply(lambda v: v is False).sum()
        ) if (db and not problems_df.empty and "in_db" in problems_df.columns) else 0,
    }
    return problems_df, summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit pinned lineage (doc_id-first) for essays and evaluations")
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--docs-root", type=Path, default=Path("data") / "docs")
    ap.add_argument("--db", type=Path, default=Path("data") / "db" / "documents.sqlite", help="Optional SQLite DB to cross-check (if present)")
    ap.add_argument("--out", type=Path, default=Path("data") / "7_reporting", help="Output directory for CSV reports")
    ap.add_argument("--strict", action="store_true", help="Exit with non-zero status if any problems found")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    db = _open_db(args.db) if args.db.exists() else None

    essay_csv = args.data_root / "2_tasks" / "essay_generation_tasks.csv"
    eval_csv = args.data_root / "2_tasks" / "evaluation_tasks.csv"

    essay_problems, essay_summary = audit_essays(essay_csv, args.docs_root, db)
    eval_problems, eval_summary = audit_evaluations(eval_csv, args.docs_root, db)

    # Write CSVs
    if not essay_problems.empty:
        essay_out = args.out / "missing_parent_docs.csv"
        essay_problems.to_csv(essay_out, index=False)
    if not eval_problems.empty:
        eval_out = args.out / "missing_target_docs.csv"
        eval_problems.to_csv(eval_out, index=False)

    summary = {
        "essays": essay_summary,
        "evaluations": eval_summary,
        "db_checked": bool(db is not None),
        "data_root": str(args.data_root),
        "docs_root": str(args.docs_root),
        "db_path": str(args.db) if db else None,
    }
    (args.out / "audit_pinned_lineage_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Print human summary
    print("Pinned lineage audit summary:\n" + json.dumps(summary, indent=2))

    problems_found = (essay_summary["total"] - essay_summary["ok"]) + (eval_summary["total"] - eval_summary["ok"]) > 0
    return 1 if (args.strict and problems_found) else 0


if __name__ == "__main__":
    raise SystemExit(main())
