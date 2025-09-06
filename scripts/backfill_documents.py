#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Iterable, Optional

from daydreaming_dagster.utils.documents_index import (
    DocumentRow,
    SQLiteDocumentsIndex,
)
from daydreaming_dagster.utils.ids import (
    compute_logical_key_id_draft,
    compute_logical_key_id_essay,
    compute_logical_key_id_evaluation,
    doc_dir as make_doc_dir,
    new_doc_id,
)


def iso_to_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat((s or "").strip().replace("Z", ""))
    except Exception:
        return None


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, text: str) -> None:
    ensure_parent(path)
    path.write_text(text, encoding="utf-8")


def read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return None


@dataclass
class BadRow:
    source_csv: str
    reason: str
    row_index: int
    task_id: str
    combo_id: str | None = None
    template_id: str | None = None
    model_id: str | None = None
    response_file: str | None = None
    timestamp: str | None = None


def append_bad_rows(path: Path, rows: Iterable[BadRow]) -> None:
    rows = list(rows)
    if not rows:
        return
    ensure_parent(path)
    header = [
        "source_csv",
        "reason",
        "row_index",
        "task_id",
        "combo_id",
        "template_id",
        "model_id",
        "response_file",
        "timestamp",
    ]
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if write_header:
            w.writeheader()
        for br in rows:
            w.writerow({
                "source_csv": br.source_csv,
                "reason": br.reason,
                "row_index": br.row_index,
                "task_id": br.task_id,
                "combo_id": br.combo_id or "",
                "template_id": br.template_id or "",
                "model_id": br.model_id or "",
                "response_file": br.response_file or "",
                "timestamp": br.timestamp or "",
            })


def backfill(
    *,
    db_path: Path,
    docs_root: Path,
    cross_root: Path,
    gen_root: Path,
    bad_list: Path,
    run_id: str,
    dry_run: bool = False,
    allow_skip: bool = False,
) -> None:
    idx = SQLiteDocumentsIndex(db_path, docs_root)
    idx.init_maybe_create_tables()

    drafts_csv = cross_root / "draft_generation_results.csv"
    links_csv = cross_root / "link_generation_results.csv"
    essays_csv = cross_root / "essay_generation_results.csv"
    evals_csv = cross_root / "evaluation_results.csv"

    drafts_rows = read_csv_rows(drafts_csv)
    links_rows = read_csv_rows(links_csv)
    essays_rows = read_csv_rows(essays_csv)
    eval_rows = read_csv_rows(evals_csv)

    bad: list[BadRow] = []

    # Local types
    @dataclass
    class DraftRef:
        task_id: str
        combo_id: str
        template_id: str
        model_id: str
        timestamp: datetime
        stage: str  # 'draft' or 'link'
        doc_id: Optional[str] = None
        logical_key_id: Optional[str] = None

    def norm_model(v: Optional[str]) -> Optional[str]:
        return v.strip() if isinstance(v, str) else None

    # Dedup by content hash across draft/link sources
    import hashlib

    seen_content: dict[str, str] = {}  # content_hash -> first task_id kept

    def content_sha256(text: str) -> str:
        h = hashlib.sha256()
        h.update(text.encode("utf-8"))
        return h.hexdigest()

    def ingest_draft_like(rows: list[dict], source_name: str, stage_label: str) -> list[DraftRef]:
        refs: list[DraftRef] = []
        for i, r in enumerate(rows):
            task_id = (r.get("draft_task_id") or r.get("link_task_id") or "").strip()
            combo_id = (r.get("combo_id") or "").strip()
            template_id = (r.get("draft_template_id") or r.get("link_template_id") or "").strip()
            model_id = norm_model(r.get("generation_model"))
            status = (r.get("generation_status") or "").strip().lower()
            ts_str = (r.get("generation_timestamp") or "").strip()
            ts = iso_to_dt(ts_str)
            resp_rel = (r.get("response_file") or "").strip()

            # Validate minimal fields
            if not task_id or not combo_id or not template_id or not model_id or not ts or status != "success":
                br = BadRow(
                    source_csv=source_name,
                    reason="invalid_row",
                    row_index=i,
                    task_id=task_id,
                    combo_id=combo_id,
                    template_id=template_id,
                    model_id=model_id or "",
                    response_file=resp_rel,
                    timestamp=ts_str,
                )
                if allow_skip:
                    bad.append(br)
                    continue
                raise RuntimeError(f"{source_name}: invalid_row at #{i}: {br}")

            # Read response content if present
            resp_path = gen_root / resp_rel if resp_rel else None
            content = read_text(resp_path) if resp_path else None
            if content is None:
                br = BadRow(
                    source_csv=source_name,
                    reason="missing_response_file",
                    row_index=i,
                    task_id=task_id,
                    combo_id=combo_id,
                    template_id=template_id,
                    model_id=model_id,
                    response_file=resp_rel,
                    timestamp=ts_str,
                )
                if allow_skip:
                    bad.append(br)
                    continue
                raise RuntimeError(f"{source_name}: missing_response_file at #{i}: {br}")

            logical = compute_logical_key_id_draft(combo_id, template_id, model_id)
            c_hash = content_sha256(content)

            # Deduplicate by content across sources
            if c_hash in seen_content:
                br = BadRow(
                    source_csv=source_name,
                    reason="duplicate_same_content",
                    row_index=i,
                    task_id=task_id,
                    combo_id=combo_id,
                    template_id=template_id,
                    model_id=model_id,
                    response_file=resp_rel,
                    timestamp=ts_str,
                )
                if allow_skip:
                    bad.append(br)
                    continue
                # Duplicates are acceptable if content matches; we skip silently in strict mode
                # but do not treat as failure. Just continue.
                continue

            seen_content[c_hash] = task_id

            # Make attempt key unique across sources and rows
            attempt_key = f"{source_name}:{i}:{ts_str}"
            doc_id = new_doc_id(logical, run_id, attempt_key)
            doc_base = make_doc_dir(docs_root, "draft", logical, doc_id)

            if not dry_run:
                write_text(doc_base / "raw.txt", content)
                write_text(doc_base / "parsed.txt", content)
                meta = {
                    "source_csv": source_name,
                    "response_file": resp_rel,
                    "generation_timestamp": ts_str,
                    "content_hash": c_hash,
                }
                write_text(doc_base / "metadata.json", __import__("json").dumps(meta, ensure_ascii=False, indent=2))
                idx.insert_document(
                    DocumentRow(
                        doc_id=doc_id,
                        logical_key_id=logical,
                        stage="draft",
                        task_id=task_id,
                        doc_dir=str(Path("draft") / logical / doc_id),
                        parent_doc_id=None,
                        template_id=template_id,
                        model_id=model_id,
                        run_id=run_id,
                        status="ok",
                        raw_chars=len(content),
                        parsed_chars=len(content),
                        meta_small=meta,
                        content_hash=c_hash,
                    )
                )

            refs.append(
                DraftRef(
                    task_id=task_id,
                    combo_id=combo_id,
                    template_id=template_id,
                    model_id=model_id,
                    timestamp=ts,
                    stage=stage_label,
                    doc_id=doc_id,
                    logical_key_id=logical,
                )
            )
        return refs

    draft_refs = ingest_draft_like(drafts_rows, drafts_csv.name, "draft")
    link_refs = ingest_draft_like(links_rows, links_csv.name, "link")
    all_drafts = draft_refs + link_refs

    # Build lookup: (combo_id, model) -> list sorted by timestamp asc
    from collections import defaultdict

    by_key: dict[tuple[str, str], list[DraftRef]] = defaultdict(list)
    for ref in all_drafts:
        key = (ref.combo_id, ref.model_id)
        by_key[key].append(ref)
    for lst in by_key.values():
        lst.sort(key=lambda r: r.timestamp)

    # Essays
    for i, r in enumerate(essays_rows):
        task_id = (r.get("essay_task_id") or "").strip()
        combo_id = (r.get("combo_id") or "").strip()
        template_id = (r.get("essay_template_id") or "").strip()
        model_id = norm_model(r.get("generation_model"))
        status = (r.get("generation_status") or "").strip().lower()
        ts_str = (r.get("generation_timestamp") or "").strip()
        ts = iso_to_dt(ts_str)
        resp_rel = (r.get("response_file") or "").strip()

        if not task_id or not combo_id or not template_id or not model_id or not ts or status != "success":
            br = BadRow(
                source_csv=essays_csv.name,
                reason="invalid_row",
                row_index=i,
                task_id=task_id,
                combo_id=combo_id,
                template_id=template_id,
                model_id=model_id or "",
                response_file=resp_rel,
                timestamp=ts_str,
            )
            if allow_skip:
                bad.append(br)
                continue
            raise RuntimeError(f"{essays_csv.name}: invalid_row at #{i}: {br}")

        resp_path = gen_root / resp_rel if resp_rel else None
        content = read_text(resp_path) if resp_path else None
        if content is None:
            br = BadRow(
                source_csv=essays_csv.name,
                reason="missing_response_file",
                row_index=i,
                task_id=task_id,
                combo_id=combo_id,
                template_id=template_id,
                model_id=model_id,
                response_file=resp_rel,
                timestamp=ts_str,
            )
            if allow_skip:
                bad.append(br)
                continue
            raise RuntimeError(f"{essays_csv.name}: missing_response_file at #{i}: {br}")

        # Resolve parent by (combo_id, model) with timestamp <= essay ts; prefer latest and drafts over links on tie
        parent: Optional[DraftRef] = None
        lst = by_key.get((combo_id, model_id), [])
        for cand in lst:
            if cand.timestamp <= ts:
                if (parent is None) or (cand.timestamp > parent.timestamp) or (
                    cand.timestamp == parent.timestamp and parent.stage == "link" and cand.stage == "draft"
                ):
                    parent = cand
            else:
                break
        if parent is None or parent.doc_id is None or parent.logical_key_id is None:
            br = BadRow(
                source_csv=essays_csv.name,
                reason="unresolved_parent",
                row_index=i,
                task_id=task_id,
                combo_id=combo_id,
                template_id=template_id,
                model_id=model_id,
                response_file=resp_rel,
                timestamp=ts_str,
            )
            if allow_skip:
                bad.append(br)
                continue
            raise RuntimeError(f"{essays_csv.name}: unresolved_parent at #{i}: {br}")

        logical = compute_logical_key_id_essay(parent.doc_id, template_id, model_id)
        attempt_key = f"{essays_csv.name}:{i}:{ts_str}"
        doc_id = new_doc_id(logical, run_id, attempt_key)
        doc_base = make_doc_dir(docs_root, "essay", logical, doc_id)

        if not dry_run:
            write_text(doc_base / "raw.txt", content)
            write_text(doc_base / "parsed.txt", content)
            meta = {
                "source_csv": essays_csv.name,
                "response_file": resp_rel,
                "generation_timestamp": ts_str,
                "parent_task_id": parent.task_id,
            }
            write_text(doc_base / "metadata.json", __import__("json").dumps(meta, ensure_ascii=False, indent=2))
            idx.insert_document(
                DocumentRow(
                    doc_id=doc_id,
                    logical_key_id=logical,
                    stage="essay",
                    task_id=task_id,
                    doc_dir=str(Path("essay") / logical / doc_id),
                    parent_doc_id=parent.doc_id,
                    template_id=template_id,
                    model_id=model_id,
                    run_id=run_id,
                    status="ok",
                    raw_chars=len(content),
                    parsed_chars=len(content),
                    meta_small=meta,
                )
            )

    # Map task_id -> doc_id for lookups
    draft_task_to_doc = {r.task_id: r for r in all_drafts if r.doc_id}

    # Build essay mapping by querying what we inserted
    con = idx.connect()
    essays_by_task: dict[str, dict] = {}
    for row in con.execute("SELECT * FROM documents WHERE stage='essay'").fetchall():
        essays_by_task[row["task_id"]] = row

    # Evaluations
    for i, r in enumerate(eval_rows):
        task_id = (r.get("evaluation_task_id") or "").strip()
        status = (r.get("evaluation_status") or "").strip().lower()
        ts_str = (r.get("evaluation_timestamp") or "").strip()
        ts = iso_to_dt(ts_str)
        resp_rel = (r.get("eval_response_file") or r.get("evaluation_response_file") or "").strip()
        eval_tmpl = (r.get("evaluation_template") or "").strip()
        eval_model = norm_model(r.get("evaluation_model"))
        essay_task = (r.get("essay_task_id") or "").strip()
        gen_task = (r.get("generation_task_id") or "").strip()

        if not task_id or not eval_tmpl or not eval_model or not ts or status != "success":
            br = BadRow(
                source_csv=evals_csv.name,
                reason="invalid_row",
                row_index=i,
                task_id=task_id,
                combo_id=r.get("combo_id") or "",
                template_id=eval_tmpl,
                model_id=eval_model or "",
                response_file=resp_rel,
                timestamp=ts_str,
            )
            if allow_skip:
                bad.append(br)
                continue
            raise RuntimeError(f"{evals_csv.name}: invalid_row at #{i}: {br}")

        resp_path = gen_root / resp_rel if resp_rel else None
        content = read_text(resp_path) if resp_path else None
        if content is None:
            br = BadRow(
                source_csv=evals_csv.name,
                reason="missing_response_file",
                row_index=i,
                task_id=task_id,
                combo_id=r.get("combo_id") or "",
                template_id=eval_tmpl,
                model_id=eval_model,
                response_file=resp_rel,
                timestamp=ts_str,
            )
            if allow_skip:
                bad.append(br)
                continue
            raise RuntimeError(f"{evals_csv.name}: missing_response_file at #{i}: {br}")

        parent_doc_id: Optional[str] = None
        if essay_task and essay_task in essays_by_task:
            parent_doc_id = essays_by_task[essay_task]["doc_id"]
        elif gen_task and gen_task in draft_task_to_doc:
            parent_doc_id = draft_task_to_doc[gen_task].doc_id
        else:
            br = BadRow(
                source_csv=evals_csv.name,
                reason="unresolved_target",
                row_index=i,
                task_id=task_id,
                combo_id=r.get("combo_id") or "",
                template_id=eval_tmpl,
                model_id=eval_model,
                response_file=resp_rel,
                timestamp=ts_str,
            )
            if allow_skip:
                bad.append(br)
                continue
            raise RuntimeError(f"{evals_csv.name}: unresolved_target at #{i}: {br}")

        logical = compute_logical_key_id_evaluation(parent_doc_id, eval_tmpl, eval_model)
        attempt_key = f"{evals_csv.name}:{i}:{ts_str}"
        doc_id = new_doc_id(logical, run_id, attempt_key)
        doc_base = make_doc_dir(docs_root, "evaluation", logical, doc_id)

        if not dry_run:
            write_text(doc_base / "raw.txt", content)
            write_text(doc_base / "parsed.txt", content)
            meta = {
                "source_csv": evals_csv.name,
                "response_file": resp_rel,
                "evaluation_timestamp": ts_str,
                "parent_doc_id": parent_doc_id,
            }
            write_text(doc_base / "metadata.json", __import__("json").dumps(meta, ensure_ascii=False, indent=2))
            idx.insert_document(
                DocumentRow(
                    doc_id=doc_id,
                    logical_key_id=logical,
                    stage="evaluation",
                    task_id=task_id,
                    doc_dir=str(Path("evaluation") / logical / doc_id),
                    parent_doc_id=parent_doc_id,
                    template_id=eval_tmpl,
                    model_id=eval_model,
                    run_id=run_id,
                    status="ok",
                    raw_chars=len(content),
                    parsed_chars=len(content),
                    meta_small=meta,
                )
            )

    # Write bad list
    if allow_skip:
        append_bad_rows(bad_list, bad)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Backfill documents index and directories from cross-experiment CSVs")
    p.add_argument("--db", type=Path, default=Path("data/db/documents.sqlite"), help="SQLite index path")
    p.add_argument("--docs-root", type=Path, default=Path("data/docs"), help="Root directory for per-document output")
    p.add_argument("--cross-root", type=Path, default=Path("data/7_cross_experiment"), help="Cross-experiment CSVs directory")
    p.add_argument("--gen-root", type=Path, default=Path("data/3_generation"), help="Root for response_file paths")
    p.add_argument("--bad-list", type=Path, default=Path("data/7_cross_experiment/bad_generations.csv"), help="Output CSV for non-ingested rows")
    p.add_argument("--run-id", type=str, default=datetime.now(UTC).strftime("backfill-%Y%m%d-%H%M%S%z"), help="Run identifier for doc_id derivation")
    p.add_argument("--dry-run", action="store_true", help="Do not write files or DB; only validate and (if --allow-skip) report bad rows")
    p.add_argument("--allow-skip", action="store_true", help="Skip and record bad rows instead of failing (writes bad_generations.csv)")
    args = p.parse_args(argv)

    backfill(
        db_path=args.db,
        docs_root=args.docs_root,
        cross_root=args.cross_root,
        gen_root=args.gen_root,
        bad_list=args.bad_list,
        run_id=args.run_id,
        dry_run=args.dry_run,
        allow_skip=args.allow_skip,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
