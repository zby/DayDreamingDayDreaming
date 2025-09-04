#!/usr/bin/env python3
"""
Inspect selected generation IDs and print parsed fields for quick debugging.

Reads a list of document_id strings (draft or essay IDs), matches known
draft/essay templates and model IDs, and prints a CSV with parsed fields:

document_id,kind,combo_id,draft_template,essay_template,generation_model_id,generation_model_name,parse_status

Usage:
  uv run python scripts/inspect_selected_generations.py \
    --input data/2_tasks/selected_generations.txt \
    --output /tmp/parsed_selection.csv

If --output is omitted, writes CSV to stdout.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Tuple, Dict
import re
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", type=Path, default=Path("data/2_tasks/selected_generations.txt"), help="Path to list/CSV of document IDs")
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--output", type=Path, default=None, help="Optional CSV output path (stdout if omitted)")
    return p.parse_args()


def _read_list(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input not found: {path}")
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
        for col in ("document_id", "essay_task_id", "draft_task_id"):
            if col in df.columns:
                vals = df[col].dropna().astype(str).tolist()
                if vals:
                    return vals
        raise ValueError("CSV must include one of: document_id, essay_task_id, draft_task_id")
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_templates(data_root: Path) -> Tuple[set[str], set[str]]:
    draft_tpls: set[str] = set()
    essay_tpls: set[str] = set()
    d_csv = data_root / "1_raw" / "draft_templates.csv"
    l_csv = data_root / "1_raw" / "link_templates.csv"
    e_csv = data_root / "1_raw" / "essay_templates.csv"
    try:
        if d_csv.exists():
            df = pd.read_csv(d_csv)
            if "template_id" in df.columns:
                draft_tpls = set(df["template_id"].astype(str))
        elif l_csv.exists():
            df = pd.read_csv(l_csv)
            if "template_id" in df.columns:
                draft_tpls = set(df["template_id"].astype(str))
        if e_csv.exists():
            df = pd.read_csv(e_csv)
            if "template_id" in df.columns:
                essay_tpls = set(df["template_id"].astype(str))
    except Exception as e:
        print(f"Warning: failed to read template CSVs ({e}); parsing may be degraded", file=sys.stderr)
    return draft_tpls, essay_tpls


def _load_models(data_root: Path) -> Tuple[set[str], Dict[str, str]]:
    ids: set[str] = set()
    id_to_name: Dict[str, str] = {}
    models_csv = data_root / "1_raw" / "llm_models.csv"
    if models_csv.exists():
        try:
            df = pd.read_csv(models_csv)
            if {"id", "model"}.issubset(df.columns):
                ids = set(df["id"].astype(str).tolist())
                id_to_name = dict(zip(df["id"].astype(str), df["model"].astype(str)))
        except Exception as e:
            print(f"Warning: failed to read llm_models.csv ({e}); model name resolution disabled", file=sys.stderr)
    return ids, id_to_name


_COMBO_PREFIX_RE = re.compile(r"^combo_v\d+_[0-9a-f]{12}")


def _extract_combo_prefix(doc: str) -> str | None:
    m = _COMBO_PREFIX_RE.match(doc)
    return m.group(0) if m else None


def _parse_draft_tokens(doc: str, draft_tpls: set[str], model_ids: set[str]) -> Tuple[str | None, str | None, str | None, str]:
    """Return (combo_id, draft_template, generation_model, status)."""
    # 1) Model id suffix match (longest)
    gen_model = None
    base = doc
    if model_ids:
        for mid in sorted(model_ids, key=len, reverse=True):
            if doc.endswith("_" + mid):
                gen_model = mid
                base = doc[: -(len(mid) + 1)]
                break
    # 2) Draft template suffix match (longest)
    draft_template = None
    combo_id = None
    if draft_tpls:
        for tpl in sorted(draft_tpls, key=len, reverse=True):
            if base.endswith("_" + tpl):
                draft_template = tpl
                combo_id = base[: -(len(tpl) + 1)]
                break
    # 3) Fallback naive
    if combo_id is None or draft_template is None or gen_model is None:
        parts = doc.split("_")
        if len(parts) >= 3:
            draft_template = draft_template or parts[-2]
            gen_model = gen_model or parts[-1]
            combo_id = combo_id or "_".join(parts[:-2])
    # Validate
    if model_ids and gen_model not in model_ids:
        gen_model = None
    if draft_tpls and draft_template not in draft_tpls:
        draft_template = None
    if combo_id and draft_template and gen_model:
        return combo_id, draft_template, gen_model, "ok"
    # Salvage just combo prefix if possible
    prefix = _extract_combo_prefix(doc)
    if prefix:
        return prefix, draft_template, gen_model, "salvaged_combo_prefix"
    return None, None, None, "failed"


def main() -> int:
    args = parse_args()
    data_root = args.data_root
    rows = _read_list(args.input)
    draft_tpls, essay_tpls = _load_templates(data_root)
    model_ids, id_to_name = _load_models(data_root)

    out_rows: List[Dict[str, str]] = []
    for doc in rows:
        kind = "essay" if any(doc.endswith("_" + t) for t in essay_tpls) else "draft"
        essay_template = None
        combo_id = None
        draft_template = None
        gen_model = None
        status = ""
        if kind == "essay":
            # find essay template suffix (longest)
            matched = None
            for tpl in sorted(essay_tpls, key=len, reverse=True):
                if doc.endswith("_" + tpl):
                    matched = tpl
                    break
            if matched:
                essay_template = matched
                draft_part = doc[: -(len(matched) + 1)]
                combo_id, draft_template, gen_model, status = _parse_draft_tokens(draft_part, draft_tpls, model_ids)
            else:
                status = "failed"
        else:
            combo_id, draft_template, gen_model, status = _parse_draft_tokens(doc, draft_tpls, model_ids)

        out_rows.append({
            "document_id": doc,
            "kind": kind,
            "combo_id": combo_id or "",
            "draft_template": draft_template or "",
            "essay_template": essay_template or "",
            "generation_model_id": gen_model or "",
            "generation_model_name": id_to_name.get(gen_model or "", ""),
            "parse_status": status or "",
        })

    df = pd.DataFrame(out_rows, columns=[
        "document_id","kind","combo_id","draft_template","essay_template","generation_model_id","generation_model_name","parse_status"
    ])
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.output, index=False)
        print(f"Wrote parsed selection to {args.output} ({len(df)} rows)")
    else:
        df.to_csv(sys.stdout, index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

