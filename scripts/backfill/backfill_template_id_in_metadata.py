#!/usr/bin/env python3
"""
Backfill template_id and model_id into metadata.json for all documents using the SQLite documents table.

Reads rows from the documents DB (data/db/documents.sqlite by default), and for each row
updates the corresponding docs/<stage>/<doc_id>/metadata.json to include "template_id"
and "model_id" fields if missing or incorrect.

Usage:
  uv run python scripts/backfill/backfill_template_id_in_metadata.py \
    --data-root data --db data/db/documents.sqlite --dry-run

Options:
  --data-root   Base data directory (default: data)
  --db          Path to documents.sqlite (default: <data-root>/db/documents.sqlite)
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


def _infer_combo_from_task(task_id: str, template_id: str, model_id: str) -> str:
    try:
        suffix = f"_{template_id}_{model_id}"
        if task_id.endswith(suffix):
            return task_id[: -len(suffix)]
    except Exception:
        pass
    return ""


def _get_db_row(con: sqlite3.Connection, doc_id: str, stage: str) -> Optional[sqlite3.Row]:
    try:
        cur = con.execute("SELECT * FROM documents WHERE doc_id=? AND stage=? LIMIT 1", (doc_id, stage))
        return cur.fetchone()
    except Exception:
        return None


def backfill(data_root: Path, db_path: Path, limit: Optional[int], dry_run: bool) -> dict:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    where = [
        "template_id IS NOT NULL AND template_id != ''",
        "model_id IS NOT NULL AND model_id != ''",
    ]
    args: list = []
    q = "SELECT doc_id, stage, template_id, model_id, task_id, parent_doc_id FROM documents"
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
    template_added = 0
    template_overwritten = 0
    model_added = 0
    model_overwritten = 0
    processed = 0
    combo_added = 0
    combo_overwritten = 0
    for row in con.execute(q, args):
        processed += 1
        doc_id = str(row["doc_id"])  # type: ignore[index]
        stg = str(row["stage"])     # type: ignore[index]
        tpl = str(row["template_id"])  # type: ignore[index]
        mdl = str(row["model_id"])      # type: ignore[index]
        task = str(row["task_id"])      # type: ignore[index]
        parent = str(row["parent_doc_id"]) if row["parent_doc_id"] is not None else ""  # type: ignore[index]
        meta_path = _doc_dir(docs_root, stg, doc_id) / "metadata.json"
        if not meta_path.parent.exists():
            missing += 1
            continue
        meta = _load_metadata(meta_path)
        changed = False
        current_tpl = str(meta.get("template_id") or "")
        current_mdl = str(meta.get("model_id") or "")
        if not current_tpl:
            meta["template_id"] = tpl
            template_added += 1
            changed = True
        elif current_tpl != tpl:
            meta["template_id"] = tpl
            template_overwritten += 1
            changed = True
        if not current_mdl:
            meta["model_id"] = mdl
            model_added += 1
            changed = True
        elif current_mdl != mdl:
            meta["model_id"] = mdl
            model_overwritten += 1
            changed = True
        # combo_id backfill
        current_combo = str(meta.get("combo_id") or "")
        combo_val = ""
        if stg == "draft":
            combo_val = _infer_combo_from_task(task, tpl, mdl)
        elif stg == "essay":
            # Prefer draft metadata; else infer from draft DB row
            if parent:
                dmeta_p = _doc_dir(docs_root, "draft", parent) / "metadata.json"
                if dmeta_p.exists():
                    try:
                        dmeta = json.loads(dmeta_p.read_text(encoding="utf-8")) or {}
                        combo_val = str(dmeta.get("combo_id") or "")
                    except Exception:
                        combo_val = ""
                if not combo_val:
                    drow = _get_db_row(con, parent, "draft")
                    if drow is not None:
                        combo_val = _infer_combo_from_task(str(drow["task_id"]), str(drow["template_id"]), str(drow["model_id"]))
        elif stg == "evaluation":
            # Get essay parent, then draft parent
            essay_doc = parent
            if essay_doc:
                # try essay metadata for draft parent
                emeta_p = _doc_dir(docs_root, "essay", essay_doc) / "metadata.json"
                draft_parent = ""
                try:
                    if emeta_p.exists():
                        emeta = json.loads(emeta_p.read_text(encoding="utf-8")) or {}
                        draft_parent = str(emeta.get("parent_doc_id") or "")
                except Exception:
                    draft_parent = ""
                if not draft_parent:
                    erow = _get_db_row(con, essay_doc, "essay")
                    if erow is not None:
                        draft_parent = str(erow["parent_doc_id"] or "")
                if draft_parent:
                    dmeta_p = _doc_dir(docs_root, "draft", draft_parent) / "metadata.json"
                    if dmeta_p.exists():
                        try:
                            dmeta = json.loads(dmeta_p.read_text(encoding="utf-8")) or {}
                            combo_val = str(dmeta.get("combo_id") or "")
                        except Exception:
                            combo_val = ""
                    if not combo_val:
                        drow = _get_db_row(con, draft_parent, "draft")
                        if drow is not None:
                            combo_val = _infer_combo_from_task(str(drow["task_id"]), str(drow["template_id"]), str(drow["model_id"]))

        if combo_val:
            if not current_combo:
                meta["combo_id"] = combo_val
                combo_added += 1
                changed = True
            elif current_combo != combo_val:
                meta["combo_id"] = combo_val
                combo_overwritten += 1
                changed = True

        # parent_doc_id backfill for essays
        current_parent = str(meta.get("parent_doc_id") or "")
        if stg == "essay" and parent:
            if not current_parent:
                meta["parent_doc_id"] = parent
                changed = True
            elif current_parent != parent:
                meta["parent_doc_id"] = parent
                changed = True

        if changed:
            if _write_metadata(meta_path, meta, dry_run):
                updated += 1
        else:
            skipped += 1

    return {
        "processed": processed,
        "updated": updated,
        "mismatched_overwritten": mismatched,
        "skipped": skipped,
        "missing_dirs": missing,
        "template_added": template_added,
        "template_overwritten": template_overwritten,
        "model_added": model_added,
        "model_overwritten": model_overwritten,
        "dry_run": dry_run,
        "combo_added": combo_added,
        "combo_overwritten": combo_overwritten,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill template_id into docs metadata.json from documents DB")
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--db", type=Path, default=None, help="Path to documents.sqlite (default: <data-root>/db/documents.sqlite)")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    data_root: Path = args.data_root
    db_path: Path = args.db or (data_root / "db" / "documents.sqlite")

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    result = backfill(data_root, db_path, args.limit, args.dry_run)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
