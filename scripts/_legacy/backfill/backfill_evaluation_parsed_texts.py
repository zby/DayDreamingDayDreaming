#!/usr/bin/env python3
"""
Backfill parsed.txt files for existing evaluation documents (numeric-only), replacing existing parsed.txt when parsing succeeds and removing it when parsing fails.

Scans data/docs/evaluation/<doc_id>, and for any directory that has raw.txt but
no parsed.txt, parses the raw text using the strict parser mapping from
data/1_raw/evaluation_templates.csv and writes parsed.txt. The output matches
the runtime asset behavior: if the raw text doesn't contain an explicit
"SCORE: <float>" line, the script appends a trailing normalized SCORE line.

Usage:
  uv run scripts/backfill/backfill_evaluation_parsed_texts.py \
    --data-root data [--dry-run] [--limit 100]

Notes:
- Always parses raw.txt; on success, writes numeric-only parsed.txt (overwrites if present).
- On parse failure, removes existing parsed.txt (if any) to reflect failure state.
- Requires evaluation_templates.csv to include a strict 'parser' column.
- Dry-run reports would-write/overwrite/remove without changing files.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from daydreaming_dagster.utils.raw_readers import read_templates
from daydreaming_dagster.utils.parser_registry import list_parsers
from daydreaming_dagster.utils.eval_response_parser import parse_llm_response


def _normalize(text: str) -> str:
    return str(text).replace("\r\n", "\n")


def backfill_parsed(
    data_root: Path,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> dict:
    docs_eval = data_root / "docs" / "evaluation"
    if not docs_eval.exists():
        raise FileNotFoundError(f"Docs store not found: {docs_eval}")

    # Build template->parser mapping from evaluation_templates.csv (active only)
    try:
        df = read_templates(data_root, "evaluation", filter_active=True)
        allowed = set(list_parsers("evaluation").keys())
        parser_map = {}
        if "template_id" in df.columns and "parser" in df.columns:
            for _, r in df.iterrows():
                tid = str(r["template_id"]).strip()
                pv = str(r.get("parser") or "").strip().lower()
                if not tid or not pv:
                    continue
                if pv in allowed:
                    parser_map[tid] = pv
    except Exception:
        parser_map = {}

    visited = 0
    written = 0
    written = 0
    overwritten_existing = 0
    created_new = 0
    removed_bad_parsed = 0
    skipped_missing_raw = 0
    skipped_missing_meta = 0
    skipped_no_template = 0
    skipped_no_parser = 0
    errors = 0

    for doc_dir in sorted([p for p in docs_eval.iterdir() if p.is_dir()]):
        if limit is not None and visited >= limit:
            break
        visited += 1
        parsed_fp = doc_dir / "parsed.txt"
        raw_fp = doc_dir / "raw.txt"
        meta_fp = doc_dir / "metadata.json"

        if not raw_fp.exists():
            skipped_missing_raw += 1
            continue
        if not meta_fp.exists():
            skipped_missing_meta += 1
            continue

        # Load metadata and extract evaluation_template
        import json

        try:
            meta = json.loads(meta_fp.read_text(encoding="utf-8"))
        except Exception:
            errors += 1
            continue
        evaluation_template = meta.get("evaluation_template") or meta.get("template_id")
        if not evaluation_template:
            skipped_no_template += 1
            continue

        # Choose parser
        strategy = parser_map.get(str(evaluation_template))
        if not strategy:
            skipped_no_parser += 1
            continue

        # Read and parse raw
        try:
            raw = _normalize(raw_fp.read_text(encoding="utf-8", errors="ignore"))
            res = parse_llm_response(raw, strategy)
            score = res.get("score")
            if not isinstance(score, (int, float)):
                raise ValueError("Parser did not return numeric score")
            parsed_out = f"{float(score)}\n"
        except Exception:
            # Parsing failed: remove parsed.txt if present
            if parsed_fp.exists():
                if dry_run:
                    removed_bad_parsed += 1
                else:
                    try:
                        parsed_fp.unlink(missing_ok=True)
                        removed_bad_parsed += 1
                    except Exception:
                        errors += 1
            continue

        # Write/overwrite numeric-only parsed.txt
        if dry_run:
            written += 1
            if parsed_fp.exists():
                overwritten_existing += 1
            else:
                created_new += 1
        else:
            try:
                parsed_fp.write_text(parsed_out, encoding="utf-8")
                written += 1
                if parsed_fp.exists():
                    # We wrote it; categorize as overwrite/new by pre-existence
                    pass
            except Exception:
                errors += 1

    return {
        "visited": visited,
        "written": written,
        "overwritten_existing": overwritten_existing,
        "created_new": created_new,
        "skipped_missing_raw": skipped_missing_raw,
        "skipped_missing_meta": skipped_missing_meta,
        "skipped_no_template": skipped_no_template,
        "skipped_no_parser": skipped_no_parser,
        "removed_bad_parsed": removed_bad_parsed,
        "errors": errors,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill parsed.txt for evaluation docs")
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--dry-run", action="store_true", help="Do not write files; report only")
    ap.add_argument("--limit", type=int, default=None, help="Process at most this many doc dirs")
    args = ap.parse_args()

    stats = backfill_parsed(args.data_root, dry_run=args.dry_run, limit=args.limit)
    print(
        "Backfill summary:",
        "visited=", stats["visited"],
        "written=", stats["written"],
        "overwritten_existing=", stats["overwritten_existing"],
        "created_new=", stats["created_new"],
        "skipped_missing_raw=", stats["skipped_missing_raw"],
        "skipped_missing_meta=", stats["skipped_missing_meta"],
        "skipped_no_template=", stats["skipped_no_template"],
        "skipped_no_parser=", stats["skipped_no_parser"],
        "removed_bad_parsed=", stats["removed_bad_parsed"],
        "errors=", stats["errors"],
    )


if __name__ == "__main__":
    main()
