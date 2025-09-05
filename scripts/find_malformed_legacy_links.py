#!/usr/bin/env python3
"""
Scan legacy draft/link responses and report document IDs whose content fails
to parse according to the parser declared in data/1_raw/draft_templates.csv.

Sources scanned (in order of preference):
- Legacy: data/3_generation/links_responses/ <draft_task_id>.txt
- Then canonical: data/3_generation/draft_responses/ <draft_task_id>.txt
- Optionally, for canonical missing docs: data/3_generation/draft_responses_raw/ <draft_task_id>_vN.txt (highest N)

Template → parser mapping is taken from data/1_raw/draft_templates.csv.
Document → template mapping is taken from data/2_tasks/draft_generation_tasks.csv, if present.
Otherwise, we infer by matching known model IDs and template IDs as suffixes.

Outputs a newline-delimited list of failing draft_task_id values with file path,
template, parser, and error message. Optionally writes results to --out. Returns
non-zero exit code when any failures are found.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import os
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Import project utilities
from daydreaming_dagster.utils.raw_readers import read_draft_templates, read_llm_models
from daydreaming_dagster.utils.link_parsers import get_draft_parser


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--scan-raw", action="store_true", help="Also scan draft_responses_raw (latest version per id)")
    p.add_argument("--out", type=Path, default=None, help="Optional output path for failing document IDs (one per line)")
    return p.parse_args()


def _load_template_parsers(data_root: Path) -> Dict[str, str]:
    """Return mapping template_id -> parser_name for templates with a parser declared."""
    df = read_draft_templates(data_root, filter_active=False)
    mapping: Dict[str, str] = {}
    if not df.empty and "template_id" in df.columns and "parser" in df.columns:
        for _, row in df.iterrows():
            tid = row.get("template_id")
            parser = row.get("parser")
            if isinstance(tid, str) and tid and (isinstance(parser, str) and parser.strip()) and not pd.isna(parser):
                mapping[tid] = parser.strip()
    return mapping


def _load_model_ids(data_root: Path) -> List[str]:
    df = read_llm_models(data_root)
    if df is None or df.empty or "id" not in df.columns:
        return []
    return [str(v) for v in df["id"].astype(str).tolist()]


def _load_task_map(data_root: Path) -> Dict[str, str]:
    """Map draft_task_id -> draft_template using 2_tasks/draft_generation_tasks.csv if present."""
    csvp = data_root / "2_tasks" / "draft_generation_tasks.csv"
    if not csvp.exists():
        return {}
    try:
        df = pd.read_csv(csvp)
        if {"draft_task_id", "draft_template"}.issubset(df.columns):
            return {str(r["draft_task_id"]): str(r["draft_template"]) for _, r in df.iterrows()}
    except Exception:
        return {}
    return {}


def _infer_template_from_id(doc_id: str, known_templates: List[str], model_ids: List[str]) -> Optional[str]:
    base = doc_id
    for mid in sorted(model_ids, key=len, reverse=True):
        suf = "_" + mid
        if base.endswith(suf):
            base = base[: -len(suf)]
            break
    for tpl in sorted(known_templates, key=len, reverse=True):
        suf = "_" + tpl
        if base.endswith(suf):
            return tpl
    return None


def _read_latest_raw(raw_dir: Path, base_id: str) -> Optional[Tuple[Path, str]]:
    if not raw_dir.exists():
        return None
    pattern = re.compile(rf"^{re.escape(base_id)}_v(\d+)\.txt$")
    best_ver = -1
    best_name = None
    try:
        for name in os.listdir(raw_dir):
            m = pattern.match(name)
            if not m:
                continue
            try:
                v = int(m.group(1))
            except Exception:
                continue
            if v > best_ver:
                best_ver = v
                best_name = name
    except Exception:
        return None
    if best_name:
        fp = raw_dir / best_name
        if fp.exists():
            try:
                return fp, fp.read_text(encoding="utf-8")
            except Exception:
                return None
    return None


def _iter_responses(data_root: Path, scan_raw: bool) -> List[Tuple[str, str, Path]]:
    """Yield (draft_task_id, text, path) for each available draft response.

    Prefers canonical draft_responses; optionally scans raw for latest version.
    """
    out: List[Tuple[str, str, Path]] = []
    # 1) Legacy single-phase directory first
    legacy_dir = data_root / "3_generation" / "links_responses"
    if legacy_dir.exists():
        for p in legacy_dir.glob("*.txt"):
            try:
                out.append((p.stem, p.read_text(encoding="utf-8"), p))
            except Exception:
                pass
    # 2) Canonical draft responses next, add if not already present
    canon_dir = data_root / "3_generation" / "draft_responses"
    if canon_dir.exists():
        seen = {doc_id for doc_id, _, _ in out}
        for p in canon_dir.glob("*.txt"):
            if p.stem in seen:
                continue
            try:
                out.append((p.stem, p.read_text(encoding="utf-8"), p))
            except Exception:
                pass
    if scan_raw:
        raw_dir = data_root / "3_generation" / "draft_responses_raw"
        seen = {doc_id for doc_id, _, _ in out}
        # Try to fill missing from raw using latest version
        if raw_dir.exists():
            for name in os.listdir(raw_dir):
                if not name.endswith(".txt"):
                    continue
                # Strip _vN suffix
                base = name[:-4]
                m = re.match(r"^(.*)_v\d+$", base)
                if not m:
                    continue
                doc_id = m.group(1)
                if doc_id in seen:
                    continue
                got = _read_latest_raw(raw_dir, doc_id)
                if got is not None:
                    fp, text = got
                    out.append((doc_id, text, fp))
    return out


def main() -> int:
    args = parse_args()
    data_root = args.data_root

    tpl_parsers = _load_template_parsers(data_root)
    if not tpl_parsers:
        print("No templates with parsers found in draft_templates.csv; nothing to check", file=sys.stderr)
        return 0
    model_ids = _load_model_ids(data_root)
    task_map = _load_task_map(data_root)
    known_templates = list(tpl_parsers.keys())

    failures: List[dict] = []
    checked = 0

    for doc_id, text, path in _iter_responses(data_root, scan_raw=args.scan_raw):
        # Determine template id
        tpl = task_map.get(doc_id)
        if not tpl:
            tpl = _infer_template_from_id(doc_id, known_templates, model_ids)
        if not tpl:
            continue  # Unknown template; skip
        parser_name = tpl_parsers.get(tpl)
        if not parser_name:
            continue  # Template doesn't require a parser
        parser_fn = get_draft_parser(parser_name)
        if parser_fn is None:
            print(f"Warning: parser '{parser_name}' for template '{tpl}' not found in registry; skipping template", file=sys.stderr)
            continue
        checked += 1
        try:
            parsed = parser_fn(text)
            if not isinstance(parsed, str) or not parsed.strip():
                failures.append({
                    "doc_id": doc_id,
                    "path": str(path),
                    "template": tpl,
                    "parser": parser_name,
                    "error": "parser returned empty/invalid text",
                })
        except Exception as e:
            failures.append({
                "doc_id": doc_id,
                "path": str(path),
                "template": tpl,
                "parser": parser_name,
                "error": str(e),
            })

    # Output
    if failures:
        print("Malformed legacy draft responses (parser failures):")
        for rec in failures:
            print(f"{rec['doc_id']}\t{rec['path']}\t{rec['template']}\t{rec['parser']}\t{rec['error']}")
    else:
        print("No malformed legacy links found for parser-enforced templates.")
    print(f"Checked {checked} documents that require parsing; failures: {len(failures)}")

    if args.out:
        try:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            if failures:
                lines = [f"{r['doc_id']}\t{r['path']}\t{r['template']}\t{r['parser']}\t{r['error']}" for r in failures]
                args.out.write_text("\n".join(lines) + "\n", encoding="utf-8")
            else:
                args.out.write_text("", encoding="utf-8")
            print(f"Wrote failing document IDs to {args.out}")
        except Exception as e:
            print(f"Warning: could not write output file: {e}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
