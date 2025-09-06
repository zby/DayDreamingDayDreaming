#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import pandas as pd
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex, DocumentRow
from daydreaming_dagster.utils.ids import (
    compute_logical_key_id,
    compute_logical_key_id_draft,
    compute_logical_key_id_essay,
    compute_logical_key_id_evaluation,
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

    inserted = 0
    # Directory paths
    draft_dir = data_root / "3_generation" / "draft_responses"
    raw_dir = data_root / "3_generation" / "draft_responses_raw"
    links_resp_dir = data_root / "3_generation" / "links_responses"
    links_prompt_dir = data_root / "3_generation" / "links_prompts"
    essay_dir = data_root / "3_generation" / "essay_responses"

    # Additional pass: scan legacy directories to include all historical files (not only current tasks)
    def _insert_doc(stage: str, task_id: str, logical: str, text: str, *, raw_text: str | None = None, template_id: str | None = None, model_id: str | None = None, parent_doc_id: str | None = None, attempt_id: str | None = None, prompt_text: str | None = None, legacy_task_id: str | None = None):
        nonlocal inserted
        doc_id = new_doc_id(logical, run_id, attempt_id or f"{task_id}")
        out_dir = build_doc_dir(docs_root, stage, logical, doc_id)
        if not dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "raw.txt").write_text(raw_text if raw_text is not None else text, encoding="utf-8")
            (out_dir / "parsed.txt").write_text(text, encoding="utf-8")
            meta = {
                "task_id": task_id,
                **({"template_id": template_id} if template_id else {}),
                **({"model_id": model_id} if model_id else {}),
                **({"parent_doc_id": parent_doc_id} if parent_doc_id else {}),
            }
            (out_dir / "metadata.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            if prompt_text:
                (out_dir / "prompt.txt").write_text(prompt_text, encoding="utf-8")
            rel = out_dir.relative_to(docs_root)
            try:
                idx.insert_document(DocumentRow(
                    doc_id=doc_id,
                    logical_key_id=logical,
                    stage=stage,
                    task_id=task_id,
                    legacy_task_id=legacy_task_id or task_id,
                    parent_doc_id=parent_doc_id,
                    doc_dir=str(rel),
                    status="ok",
                    template_id=template_id,
                    model_id=model_id,
                    run_id=run_id,
                    raw_chars=len(raw_text if raw_text is not None else text),
                    parsed_chars=len(text),
                    content_hash=None,
                    meta_small={"function": "backfill-scan"},
                ))
            except sqlite3.IntegrityError:
                pass
        inserted += 1

    # Helpers to parse combo/model from id or filename base
    def _parse_triple(pk: str) -> tuple[str | None, str | None, str | None]:
        parts = pk.split("__")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        return None, None, None

    def _parse_combo_model_from_base(base: str) -> tuple[str | None, str | None]:
        # Prefer __-separated triple
        c,t,m = _parse_triple(base)
        if c and m:
            return c, m
        # Fallback: underscore-separated legacy names like
        # combo_v1_<hash>_<template>_<model>[_...]
        toks = base.split("_")
        # Expect at least: combo v1 <hash> <template> <model>
        if len(toks) >= 5 and toks[0] == "combo" and toks[1].startswith("v"):
            combo = "_".join(toks[:3])
            model = toks[4]
            return combo, model
        return None, None

    def _split_parts(stem: str) -> list[str]:
        return [p for p in stem.split("__") if p != ""]

    # Reporting counters
    prefix_hits = 0
    glob_hits = 0
    no_parent = 0
    alias_inserts = 0

    def _resolve_parent_draft(idx: SQLiteDocumentsIndex, essay_base: str, combo: str | None, model: str | None) -> str | None:
        nonlocal prefix_hits, glob_hits, no_parent
        con = idx.connect()
        parts = _split_parts(essay_base)
        # Legacy: essay id starts with draft id (first 3 parts)
        if len(parts) >= 4:
            draft_candidate = "__".join(parts[:3])
            row = con.execute(
                "SELECT doc_id FROM documents WHERE stage='draft' AND task_id=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (draft_candidate,),
            ).fetchone()
            if row:
                prefix_hits += 1
                return row[0] if isinstance(row, tuple) else row.get("doc_id")
        # Legacy underscore format: combo_v1_<hash>_<draft_tpl>_<model>[_<essay_tpl>]
        if "__" not in essay_base:
            toks = essay_base.split("_")
            if len(toks) >= 5 and toks[0] == "combo" and toks[1].startswith("v"):
                # Draft candidate is all but the last token (drop essay template)
                draft_candidate_u = "_".join(toks[:-1])
                row = con.execute(
                    "SELECT doc_id FROM documents WHERE stage='draft' AND task_id=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (draft_candidate_u,),
                ).fetchone()
                if row:
                    prefix_hits += 1
                    return row[0] if isinstance(row, tuple) else row.get("doc_id")
        # Fallback: combo+model using GLOB (treats '_' literally). Try double-underscore then underscore style.
        if combo and model:
            row = con.execute(
                "SELECT doc_id FROM documents WHERE stage='draft' AND task_id GLOB ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (f"{combo}__*__{model}",),
            ).fetchone()
            if not row:
                row = con.execute(
                    "SELECT doc_id FROM documents WHERE stage='draft' AND task_id GLOB ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (f"{combo}_*_{model}*",),
                ).fetchone()
            if row:
                glob_hits += 1
                return row[0] if isinstance(row, tuple) else row.get("doc_id")
        no_parent += 1
        return None

    # Scan all draft files
    if draft_dir.exists():
        for p in sorted(draft_dir.glob("*.txt")):
            name = p.stem
            base = name.split("_v")[0]
            combo, tmpl, model = _parse_triple(base)
            text = p.read_text(encoding="utf-8")
            # Prefer matching RAW by version when available
            raw_text = text
            if raw_dir.exists():
                v = None
                if "_v" in name:
                    try:
                        v = int(name.split("_v")[-1])
                    except Exception:
                        v = None
                if v is not None:
                    raw_candidate = raw_dir / f"{base}_v{v}.txt"
                    if raw_candidate.exists():
                        raw_text = raw_candidate.read_text(encoding="utf-8")
            logical = (
                compute_logical_key_id_draft(combo, tmpl, model)
                if combo and tmpl and model
                else compute_logical_key_id("draft", parts=(base,))
            )
            _insert_doc("draft", base, logical, text, raw_text=raw_text, template_id=tmpl, model_id=model, attempt_id=p.name, legacy_task_id=base)

    # Scan all links (legacy Phase-1 as drafts) BEFORE essays
    if links_resp_dir.exists():
        for p in sorted(links_resp_dir.glob("*.txt")):
            name = p.stem
            base = name.split("_v")[0]
            parts = _split_parts(base)
            combo = parts[0] if len(parts) >= 1 else None
            tmpl  = parts[1] if len(parts) >= 2 else None
            model = parts[2] if len(parts) >= 3 else None
            text = p.read_text(encoding="utf-8")
            # Try to find matching prompt (same version if present)
            prompt_text = None
            if links_prompt_dir.exists():
                v = None
                if "_v" in name:
                    try:
                        v = int(name.split("_v")[-1])
                    except Exception:
                        v = None
                cand = links_prompt_dir / (f"{base}_v{v}.txt" if v is not None else f"{base}.txt")
                if cand.exists():
                    prompt_text = cand.read_text(encoding="utf-8")
            logical = (
                compute_logical_key_id_draft(combo, tmpl, model)
                if combo and tmpl and model
                else compute_logical_key_id("draft", parts=(base,))
            )
            _insert_doc("draft", base, logical, text, raw_text=text, template_id=tmpl, model_id=model, attempt_id=p.name, prompt_text=prompt_text, legacy_task_id=base)

    # Scan all essay files
    if essay_dir.exists():
        for p in sorted(essay_dir.glob("*.txt")):
            name = p.stem
            base = name.split("_v")[0]
            combo_u, model_u = _parse_combo_model_from_base(base)
            combo, tmpl, model = _parse_triple(base)
            if (not combo or not model) and combo_u and model_u:
                combo, model = combo_u, model_u
            text = p.read_text(encoding="utf-8")
            # Resolve parent draft via legacy prefix-of rule then combo+model using GLOB
            parent_doc_id = _resolve_parent_draft(idx, base, combo, model)
            logical = (
                compute_logical_key_id_essay(parent_doc_id, tmpl, model)
                if parent_doc_id and tmpl and model
                else compute_logical_key_id("essay", parts=(base,))
            )
            _insert_doc("essay", base, logical, text, raw_text=text, template_id=tmpl, model_id=model, parent_doc_id=parent_doc_id, attempt_id=p.name, legacy_task_id=base)

    # Scan all evaluation files
    eval_dir = data_root / "4_evaluation" / "evaluation_responses"
    if eval_dir.exists():
        for p in sorted(eval_dir.glob("*.txt")):
            name = p.stem
            base = name.split("_v")[0]
            parts = _split_parts(base)
            document_id = "__".join(parts[:-2]) if len(parts) >= 3 else base
            eval_template = parts[-2] if len(parts) >= 2 else None
            model = parts[-1] if len(parts) >= 1 else None
            text = p.read_text(encoding="utf-8")
            parent_doc_id = None
            # Try resolve a parent doc by matching draft/essay task_id
            cur = idx.connect().execute(
                "SELECT doc_id FROM documents WHERE (stage='essay' OR stage='draft') AND task_id=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (document_id,),
            )
            r = cur.fetchone()
            if r:
                parent_doc_id = r.get("doc_id")
            logical = (
                compute_logical_key_id_evaluation(parent_doc_id or document_id, eval_template or "", model or "")
                if (eval_template and model)
                else compute_logical_key_id("evaluation", parts=(base,))
            )
            _insert_doc("evaluation", base, logical, text, raw_text=text, template_id=eval_template, model_id=model, parent_doc_id=parent_doc_id, attempt_id=p.name, legacy_task_id=base)

    # (links already processed above)

    # Ensure essays with explicit draft_task_id can resolve by exact match: add alias draft rows when absent
    tasks_csv = data_root / "2_tasks" / "essay_generation_tasks.csv"
    if tasks_csv.exists():
        try:
            edf = pd.read_csv(tasks_csv)
            con = idx.connect()
            for _, erow in edf.iterrows():
                dtid = erow.get("draft_task_id")
                if not isinstance(dtid, str) or not dtid.strip():
                    continue
                # Skip if exact draft already present
                if con.execute(
                    "SELECT 1 FROM documents WHERE stage='draft' AND task_id=? LIMIT 1",
                    (dtid,),
                ).fetchone():
                    continue
                combo_id = erow.get("combo_id")
                model_id = erow.get("generation_model") or erow.get("generation_model_id")
                if not (isinstance(combo_id, str) and isinstance(model_id, str) and combo_id and model_id):
                    continue
                like = f"{combo_id}__%__{model_id}"
                cand = con.execute(
                    "SELECT * FROM documents WHERE stage='draft' AND task_id LIKE ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (like,),
                ).fetchone()
                if not cand:
                    continue
                alias_doc_id = new_doc_id(cand["logical_key_id"], run_id, f"alias::{dtid}")
                rel = cand["doc_dir"]
                if not dry_run:
                    try:
                        con.execute(
                            "INSERT INTO documents (doc_id, logical_key_id, stage, task_id, parent_doc_id, template_id, model_id, run_id, prompt_path, parser, status, usage_prompt_tokens, usage_completion_tokens, usage_max_tokens, doc_dir, raw_chars, parsed_chars, content_hash, meta_small, lineage_prev_doc_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                alias_doc_id,
                                cand["logical_key_id"],
                                "draft",
                                dtid,
                                None,
                                cand["template_id"],
                                cand["model_id"],
                                run_id,
                                None,
                                None,
                                "ok",
                                None,
                                None,
                                None,
                                rel,
                                cand["raw_chars"],
                                cand["parsed_chars"],
                                cand["content_hash"],
                                '{"function":"backfill-alias"}',
                                None,
                            ),
                        )
                        con.commit()
                        inserted += 1
                    except sqlite3.IntegrityError:
                        pass
                alias_inserts += 1
        except Exception:
            pass

    print(f"Inserted/processed rows: {inserted}")
    print(f"essay parent resolution: prefix_hits={prefix_hits}, glob_hits={glob_hits}, none={no_parent}, alias_inserts={alias_inserts}")
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
