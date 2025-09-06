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
    essay_dir = data_root / "3_generation" / "essay_responses"

    # Additional pass: scan legacy directories to include all historical files (not only current tasks)
    def _insert_doc(stage: str, task_id: str, logical: str, text: str, *, raw_text: str | None = None, template_id: str | None = None, model_id: str | None = None, parent_doc_id: str | None = None, attempt_id: str | None = None, prompt_text: str | None = None):
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
                parent_doc_id=parent_doc_id,
                doc_dir=str(rel),
                status="ok",
                template_id=template_id,
                model_id=model_id,
                run_id=run_id,
                raw_chars=len(raw_text if raw_text is not None else text),
                parsed_chars=len(text),
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
            _insert_doc("draft", base, logical, text, raw_text=raw_text, template_id=tmpl, model_id=model, attempt_id=p.name)

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
            # Try link to parent if a draft with same combo+model exists
            parent_doc_id = None
            if combo and model:
                # Prefer any draft row with same combo+model (template segment differs)
                cur = idx.connect().execute(
                    "SELECT doc_id FROM documents WHERE stage='draft' AND task_id LIKE ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (f"{combo}__%__{model}",),
                )
                r = cur.fetchone()
                if r:
                    parent_doc_id = r[0] if isinstance(r, tuple) else r.get("doc_id")
            logical = (
                compute_logical_key_id_essay(parent_doc_id, tmpl, model)
                if parent_doc_id and tmpl and model
                else compute_logical_key_id("essay", parts=(base,))
            )
            _insert_doc("essay", base, logical, text, raw_text=text, template_id=tmpl, model_id=model, parent_doc_id=parent_doc_id, attempt_id=p.name)

    # Scan all evaluation files
    eval_dir = data_root / "4_evaluation" / "evaluation_responses"
    if eval_dir.exists():
        for p in sorted(eval_dir.glob("*.txt")):
            name = p.stem
            base = name.split("_v")[0]
            parts = base.split("__")
            document_id = parts[0] if parts else base
            eval_template = parts[1] if len(parts) > 1 else None
            model = parts[2] if len(parts) > 2 else None
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
            _insert_doc("evaluation", base, logical, text, raw_text=text, template_id=eval_template, model_id=model, parent_doc_id=parent_doc_id, attempt_id=p.name)

    # Scan all links (treat as draft-like)
    links_resp_dir = data_root / "3_generation" / "links_responses"
    links_prompt_dir = data_root / "3_generation" / "links_prompts"
    if links_resp_dir.exists():
        for p in sorted(links_resp_dir.glob("*.txt")):
            name = p.stem
            base = name.split("_v")[0]
            combo, tmpl, model = _parse_triple(base)
            text = p.read_text(encoding="utf-8")
            # Try to find matching prompt
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
            _insert_doc("draft", base, logical, text, raw_text=text, template_id=tmpl, model_id=model, attempt_id=p.name, prompt_text=prompt_text)

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
