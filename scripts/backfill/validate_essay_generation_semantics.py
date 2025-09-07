#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex


@dataclass
class Violation:
    essay_doc_id: str
    essay_task_id: str
    template_id: str
    generator: str
    parent_doc_id: Optional[str]
    problem: str
    detail: str


def load_template_generators(path: Path) -> Dict[str, str]:
    m: Dict[str, str] = {}
    if not path.exists():
        return m
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            t = (row.get("id") or row.get("template_id") or "").strip()
            g = (row.get("generator") or "").strip()
            if t:
                m[t] = g
    return m


def read_text_or_none(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return None


def validate(db_path: Path, docs_root: Path, templates_csv: Path, *, list_limit: int = 0) -> Dict:
    idx = SQLiteDocumentsIndex(db_path, docs_root)
    con = idx.connect()
    con.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}

    tgens = load_template_generators(templates_csv)
    # Overrides for known templates if CSV is out of date
    overrides = {
        # creative-synthesis-v9 is an LLM-based essay (not copy)
        "creative-synthesis-v9": "llm",
    }
    tgens.update(overrides)

    essays = con.execute("SELECT * FROM documents WHERE stage='essay'").fetchall()

    violations: List[Violation] = []
    checked = 0
    copy_ok = 0
    llm_ok = 0
    skipped_unknown = 0

    for es in essays:
        checked += 1
        tmpl = (es.get("template_id") or "").strip()
        gen = (tgens.get(tmpl) or "").lower()
        parent = es.get("parent_doc_id") or None
        essay_dir = idx.resolve_doc_dir(es)
        essay_parsed = read_text_or_none(essay_dir / "parsed.txt")
        essay_prompt = read_text_or_none(essay_dir / "prompt.txt")

        if not gen:
            skipped_unknown += 1
            continue

        if parent is None:
            violations.append(
                Violation(
                    essay_doc_id=es["doc_id"],
                    essay_task_id=es["task_id"],
                    template_id=tmpl,
                    generator=gen,
                    parent_doc_id=None,
                    problem="missing_parent",
                    detail="essay has no parent_doc_id",
                )
            )
            continue

        pr = con.execute("SELECT * FROM documents WHERE doc_id=?", (parent,)).fetchone()
        if pr is None:
            violations.append(
                Violation(
                    essay_doc_id=es["doc_id"],
                    essay_task_id=es["task_id"],
                    template_id=tmpl,
                    generator=gen,
                    parent_doc_id=parent,
                    problem="parent_missing",
                    detail="no row for parent_doc_id",
                )
            )
            continue
        draft_dir = idx.resolve_doc_dir(pr)
        draft_parsed = read_text_or_none(draft_dir / "parsed.txt")

        if "copy" in gen:
            # Essay parsed should be derived from draft parsed
            # Relaxed rule for row-style draft outputs: essay_parsed must be contained within draft_parsed
            if essay_parsed is None or draft_parsed is None:
                violations.append(
                    Violation(
                        essay_doc_id=es["doc_id"],
                        essay_task_id=es["task_id"],
                        template_id=tmpl,
                        generator=gen,
                        parent_doc_id=parent,
                        problem="missing_parsed",
                        detail=f"essay_parsed={essay_parsed is not None}, draft_parsed={draft_parsed is not None}",
                    )
                )
            else:
                # exact match is fine; otherwise check containment
                if essay_parsed == draft_parsed or (essay_parsed in draft_parsed):
                    copy_ok += 1
                else:
                    violations.append(
                        Violation(
                            essay_doc_id=es["doc_id"],
                            essay_task_id=es["task_id"],
                            template_id=tmpl,
                            generator=gen,
                            parent_doc_id=parent,
                            problem="copy_mismatch",
                            detail="essay parsed.txt not contained within draft parsed.txt",
                        )
                    )
        elif "llm" in gen:
            # Essay prompt must include draft parsed content
            if draft_parsed is None or essay_prompt is None:
                violations.append(
                    Violation(
                        essay_doc_id=es["doc_id"],
                        essay_task_id=es["task_id"],
                        template_id=tmpl,
                        generator=gen,
                        parent_doc_id=parent,
                        problem="missing_prompt_or_draft",
                        detail=f"essay_prompt={essay_prompt is not None}, draft_parsed={draft_parsed is not None}",
                    )
                )
            elif draft_parsed not in essay_prompt:
                violations.append(
                    Violation(
                        essay_doc_id=es["doc_id"],
                        essay_task_id=es["task_id"],
                        template_id=tmpl,
                        generator=gen,
                        parent_doc_id=parent,
                        problem="prompt_missing_draft",
                        detail="draft parsed.txt not found in essay prompt.txt",
                    )
                )
            else:
                llm_ok += 1
        else:
            # Unknown generator class; record but don't fail
            skipped_unknown += 1

    return {
        "checked": checked,
        "copy_ok": copy_ok,
        "llm_ok": llm_ok,
        "violations": violations,
        "skipped_unknown": skipped_unknown,
    }


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate essay generation semantics against templates CSV")
    p.add_argument("--db", type=Path, default=Path("data/db/documents.sqlite"))
    p.add_argument("--docs-root", type=Path, default=Path("data/docs"))
    p.add_argument("--templates", type=Path, default=Path("data/1_raw/essay_templates.csv"))
    p.add_argument("--out", type=Path, help="Optional CSV path to write violations")
    args = p.parse_args(argv)

    res = validate(args.db, args.docs_root, args.templates)
    v = res["violations"]
    print(f"essays_checked={res['checked']}")
    print(f"copy_ok={res['copy_ok']}")
    print(f"llm_ok={res['llm_ok']}")
    print(f"skipped_unknown={res['skipped_unknown']}")
    print(f"violations={len(v)}")

    if args.out and v:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "essay_doc_id",
                    "essay_task_id",
                    "template_id",
                    "generator",
                    "parent_doc_id",
                    "problem",
                    "detail",
                ],
            )
            w.writeheader()
            for it in v:
                w.writerow(it.__dict__)

    return 0 if not v else 2


if __name__ == "__main__":
    raise SystemExit(main())
