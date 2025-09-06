#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
import pandas as pd
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex, DocumentRow
from daydreaming_dagster.utils.ids import (
    compute_logical_key_id_draft,
    compute_logical_key_id_essay,
    new_doc_id,
    doc_dir as build_doc_dir,
)


def _latest_versioned_txt(dir_path: Path, stem: str) -> Path | None:
    if not dir_path.exists():
        return None
    best = None
    best_ver = -1
    prefix = f"{stem}_v"
    for p in dir_path.iterdir():
        if not p.name.startswith(prefix) or p.suffix != ".txt":
            continue
        try:
            v = int(p.stem.split("_v")[-1])
        except Exception:
            continue
        if v > best_ver:
            best_ver = v
            best = p
    if best:
        return best
    legacy = dir_path / f"{stem}.txt"
    return legacy if legacy.exists() else None


def backfill(data_root: Path, db_path: Path, docs_root: Path, run_id: str, dry_run: bool) -> int:
    idx = SQLiteDocumentsIndex(db_path, docs_root)
    idx.init_maybe_create_tables()

    tasks2 = data_root / "2_tasks"
    drafts_csv = tasks2 / "draft_generation_tasks.csv"
    essays_csv = tasks2 / "essay_generation_tasks.csv"
    if not drafts_csv.exists():
        print("No draft_generation_tasks.csv; nothing to backfill")
        return 0

    draft_df = pd.read_csv(drafts_csv)
    essay_df = pd.read_csv(essays_csv) if essays_csv.exists() else pd.DataFrame()

    inserted = 0

    # Drafts
    draft_dir = data_root / "3_generation" / "draft_responses"
    raw_dir = data_root / "3_generation" / "draft_responses_raw"
    for _, row in draft_df.iterrows():
        task_id = str(row["draft_task_id"]) if "draft_task_id" in row else None
        if not task_id:
            continue
        combo_id = str(row.get("combo_id"))
        draft_template = str(row.get("draft_template"))
        model_id = str(row.get("generation_model") or row.get("generation_model_id"))

        file_path = _latest_versioned_txt(draft_dir, task_id)
        if not file_path:
            continue
        parsed = file_path.read_text(encoding="utf-8")
        raw_path = _latest_versioned_txt(raw_dir, task_id)
        raw = parsed
        if raw_path and raw_path.exists():
            raw = raw_path.read_text(encoding="utf-8")

        logical = compute_logical_key_id_draft(combo_id, draft_template, model_id)
        doc_id = new_doc_id(logical, run_id, task_id)
        out_dir = build_doc_dir(docs_root, "draft", logical, doc_id)
        if not dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "raw.txt").write_text(raw, encoding="utf-8")
            (out_dir / "parsed.txt").write_text(parsed, encoding="utf-8")
            (out_dir / "metadata.json").write_text(json.dumps({
                "task_id": task_id,
                "draft_template": draft_template,
                "model_id": model_id,
            }, ensure_ascii=False, indent=2), encoding="utf-8")
            rel = out_dir.relative_to(docs_root)
            idx.insert_document(DocumentRow(
                doc_id=doc_id,
                logical_key_id=logical,
                stage="draft",
                task_id=task_id,
                doc_dir=str(rel),
                status="ok",
                template_id=draft_template,
                model_id=model_id,
                run_id=run_id,
                raw_chars=len(raw),
                parsed_chars=len(parsed),
                meta_small={"function": "backfill"},
            ))
        inserted += 1

    # Essays
    essay_dir = data_root / "3_generation" / "essay_responses"
    for _, row in essay_df.iterrows():
        task_id = str(row["essay_task_id"]) if "essay_task_id" in row else None
        if not task_id:
            continue
        essay_template = str(row.get("essay_template"))
        model_id = str(row.get("generation_model") or row.get("generation_model_id"))
        draft_task_id = row.get("draft_task_id")

        file_path = _latest_versioned_txt(essay_dir, task_id)
        if not file_path:
            continue
        text = file_path.read_text(encoding="utf-8")

        parent_doc_id = None
        if isinstance(draft_task_id, str) and draft_task_id:
            parent = idx.get_latest_by_task("draft", draft_task_id)
            if parent:
                parent_doc_id = parent.get("doc_id")

        if parent_doc_id:
            logical = compute_logical_key_id_essay(parent_doc_id, essay_template, model_id)
        else:
            # fallback: still group by draft task id to maintain some determinism
            logical = compute_logical_key_id_essay(draft_task_id or "", essay_template, model_id)

        doc_id = new_doc_id(logical, run_id, task_id)
        out_dir = build_doc_dir(docs_root, "essay", logical, doc_id)
        if not dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "raw.txt").write_text(text, encoding="utf-8")
            (out_dir / "parsed.txt").write_text(text, encoding="utf-8")
            (out_dir / "metadata.json").write_text(json.dumps({
                "task_id": task_id,
                "essay_template": essay_template,
                "model_id": model_id,
                "parent_doc_id": parent_doc_id,
            }, ensure_ascii=False, indent=2), encoding="utf-8")
            rel = out_dir.relative_to(docs_root)
            idx.insert_document(DocumentRow(
                doc_id=doc_id,
                logical_key_id=logical,
                stage="essay",
                task_id=task_id,
                parent_doc_id=parent_doc_id,
                doc_dir=str(rel),
                status="ok",
                template_id=essay_template,
                model_id=model_id,
                run_id=run_id,
                raw_chars=len(text),
                parsed_chars=len(text),
                meta_small={"function": "backfill"},
            ))
        inserted += 1

    print(f"Inserted/processed rows: {inserted}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Backfill documents index and doc_dir from legacy versioned files")
    ap.add_argument("--data-root", default=str(Path("data")), help="Data root path")
    ap.add_argument("--db", default=str(Path("data") / "db" / "documents.sqlite"), help="SQLite DB path")
    ap.add_argument("--docs-root", default=str(Path("data") / "docs"), help="Docs root path")
    ap.add_argument("--run-id", default="backfill", help="Run id label for backfilled rows")
    ap.add_argument("--dry-run", action="store_true", help="Report actions without writing")
    args = ap.parse_args()
    return backfill(Path(args.data_root), Path(args.db), Path(args.docs_root), args.run_id, args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())

