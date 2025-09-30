#!/usr/bin/env python3
"""
Parse evaluation scores from the gens store and write a consolidated CSV.

This script performs CROSS-EXPERIMENT analysis by scanning ALL evaluation generations
under data/gens/evaluation, not just those from the current experiment's task definitions.
This enables historical analysis across multiple experimental runs.

Key cross-experiment features:
- Scans gens/evaluation/<gen_id> for parsed/raw texts and reads metadata.json
- Uses evaluation_templates.csv to select the parser for each evaluation_template
- No dependence on evaluation_tasks.csv or legacy responses directory

Defaults:
- Gens store: data/gens/evaluation/<gen_id>/{parsed.txt,metadata.json}
- Output: data/7_cross_experiment/aggregated_scores.csv

Usage examples:
- uv run scripts/aggregate_scores.py --output data/7_cross_experiment/aggregated_scores.csv
- uv run scripts/aggregate_scores.py --data-root data --output tmp/aggregated_scores.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import json
from datetime import datetime


def _ensure_src_on_path() -> None:
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))


_ensure_src_on_path()

from daydreaming_dagster.utils.evaluation_scores import aggregate_evaluation_scores_for_ids
from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.utils.errors import DDError, Err

import pandas as pd

"""parsed.txt-only aggregator: no parser maps or raw fallback"""


# Aggregator does not use evaluation parser maps; runtime/backfill produce parsed.txt


def load_known_templates(base_data: Path) -> Dict[str, Set[str]]:
    """Load known template IDs from data/1_raw where available.

    Returns a dict with sets: eval_templates, link_templates, essay_templates
    Fallback to empty sets if files not found.
    """
    eval_templates: Set[str] = set()
    link_templates: Set[str] = set()
    essay_templates: Set[str] = set()

    # Evaluation templates from CSV and directory
    eval_csv = base_data / "1_raw" / "evaluation_templates.csv"
    eval_dir = base_data / "1_raw" / "templates" / "evaluation"
    if eval_csv.exists():
        try:
            df = pd.read_csv(eval_csv)
            if "template_id" in df.columns:
                eval_templates.update(df["template_id"].astype(str).tolist())
        except Exception:
            pass
    if eval_dir.exists():
        for p in eval_dir.glob("*.txt"):
            eval_templates.add(p.stem)

    # Generation templates (draft/links and essay)
    link_csv = base_data / "1_raw" / "link_templates.csv"
    draft_csv = base_data / "1_raw" / "draft_templates.csv"
    essay_csv = base_data / "1_raw" / "essay_templates.csv"
    link_dir = base_data / "1_raw" / "templates" / "draft"
    draft_dir = base_data / "1_raw" / "templates" / "draft"
    essay_dir = base_data / "1_raw" / "templates" / "essay"
    if link_csv.exists():
        try:
            df = pd.read_csv(link_csv)
            if "template_id" in df.columns:
                link_templates.update(df["template_id"].astype(str).tolist())
        except Exception:
            pass
    if draft_csv.exists():
        try:
            df = pd.read_csv(draft_csv)
            if "template_id" in df.columns:
                link_templates.update(df["template_id"].astype(str).tolist())
        except Exception:
            pass
    if essay_csv.exists():
        try:
            df = pd.read_csv(essay_csv)
            if "template_id" in df.columns:
                essay_templates.update(df["template_id"].astype(str).tolist())
        except Exception:
            pass
    if draft_dir.exists():
        for p in draft_dir.glob("*.txt"):
            link_templates.add(p.stem)
    if link_dir.exists():
        for p in link_dir.glob("*.txt"):
            link_templates.add(p.stem)
    if essay_dir.exists():
        for p in essay_dir.glob("*.txt"):
            essay_templates.add(p.stem)

    return {
        "eval_templates": eval_templates,
        "link_templates": link_templates,
        "essay_templates": essay_templates,
    }


def load_model_mapping(base_data: Path) -> Dict[str, str]:
    """Load evaluation model id -> provider/model mapping from llm_models.csv.

    Returns empty dict if file missing or unreadable.
    """
    mapping: Dict[str, str] = {}
    models_csv = base_data / "1_raw" / "llm_models.csv"
    if models_csv.exists():
        try:
            mdf = pd.read_csv(models_csv)
            if {"id", "model"}.issubset(mdf.columns):
                for _, r in mdf.iterrows():
                    mid = str(r["id"]) if pd.notna(r["id"]) else None
                    mname = str(r["model"]) if pd.notna(r["model"]) else None
                    if mid and mname:
                        mapping[mid] = mname
        except Exception:
            pass
    return mapping


def parse_identifiers_from_eval_task_id(
    evaluation_task_id: str,
    known: Optional[Dict[str, Set[str]]] = None,
) -> Dict[str, Any]:
    """Parse identifiers from an evaluation_task_id string using the new scheme.

    Canonical format (doc-id first):
      evaluation_task_id = {parent_doc_id}__{eval_template}__{eval_model}
      parent_doc_id      = essay doc_id (evaluations always target essays; copy generator mirrors single-phase)
      essay_task_id      = {combo_id}_{draft_template}_{generation_model}_{essay_template}

    We extract:
      - essay_task_id
      - combo_id
      - link_template
      - essay_template (reported as generation_template for backward compatibility)
      - generation_model
      - evaluation_template, evaluation_model
    """
    result: Dict[str, Any] = {
        "document_id": None,
        "essay_task_id": None,
        "combo_id": None,
        "link_template": None,
        "generation_template": None,  # holds essay_template for compatibility
        "generation_model": None,
        "evaluation_template": None,
        "evaluation_model": None,
        "evaluation_llm_model": None,
    }

    if not evaluation_task_id:
        return result

    # Known template IDs loaded from disk if provided
    eval_templates = set()
    link_templates = set()
    essay_templates = set()
    if known:
        eval_templates = known.get("eval_templates", set())
        link_templates = known.get("link_templates", set())
        essay_templates = known.get("essay_templates", set())

    # First, try the modern double-underscore format
    parts3 = evaluation_task_id.split("__")
    eval_template = None
    eval_model = None
    document_id = None
    if len(parts3) == 3:
        document_id, eval_template, eval_model = parts3
        result["document_id"] = document_id
    else:
        # Fallback: older underscore-only format (best-effort)
        parts = evaluation_task_id.split("_")
        if len(parts) >= 2:
            eval_template = parts[-2]
            eval_model = parts[-1]
            document_id = "_".join(parts[:-2])
            result["document_id"] = document_id

    # Attempt to parse document_id into essay_task_id-like components
    combo_id = None
    link_template_val = None
    essay_template = None
    gen_model = None
    if result["document_id"]:
        doc_parts = result["document_id"].split("_")
        # Find essay_template by scanning from right among known essay templates
        if essay_templates and link_templates:
            for i in range(len(doc_parts)-1, -1, -1):
                token = doc_parts[i]
                if token in essay_templates:
                    essay_template = token
                    # Find last link template before i
                    l_idx = None
                    for j in range(i-1, -1, -1):
                        if doc_parts[j] in link_templates:
                            l_idx = j
                            break
                    if l_idx is not None:
                        link_template_val = doc_parts[l_idx]
                        gen_tokens = doc_parts[l_idx+1:i]
                        gen_model = "_".join(gen_tokens) if gen_tokens else None
                        combo_parts = doc_parts[:l_idx]
                        combo_id = "_".join(combo_parts) if combo_parts else None
                    break
    # Legacy case: if no essay_template found, may have draft_template + model
        if essay_template is None and link_templates:
            for i in range(len(doc_parts)-1, -1, -1):
                if doc_parts[i] in link_templates:
                    link_template_val = doc_parts[i]
                    gen_tokens = doc_parts[i+1:]
                    gen_model = "_".join(gen_tokens) if gen_tokens else None
                    combo_parts = doc_parts[:i]
                    combo_id = "_".join(combo_parts) if combo_parts else None
                    # Treat generation_template as link_template for drafts
                    essay_template = link_template_val
                    break

    result.update({
        "essay_task_id": result["document_id"],  # for two-phase this equals essay_task_id; for drafts it’s link_task_id
        "combo_id": combo_id,
        "link_template": link_template_val,
        "generation_template": essay_template,
        "generation_model": gen_model,
        "evaluation_template": eval_template,
        "evaluation_model": eval_model,
        "evaluation_llm_model": eval_model,
    })

    return result


# No raw parsing helpers in aggregator (parsed.txt-only)


# Removed load_tasks function - no longer needed for tasks-free implementation


def parse_all(
    data_root: Path,
    output_csv: Path,
    *,
    strict: bool = False,
) -> pd.DataFrame:
    """Parse all evaluation scores from the gens store.

    Source: data_root/gens/evaluation/<gen_id>/{parsed.txt,metadata.json}
    """

    docs_eval = data_root / "gens" / "evaluation"
    paths_helper = Paths.from_str(str(data_root))
    # Prefer shared helper to aggregate scores; keep legacy logic below for backcompat
    try:
       if docs_eval.exists():
            gen_ids = [p.name for p in docs_eval.iterdir() if p.is_dir()]
            df = aggregate_evaluation_scores_for_ids(data_root, gen_ids)
            # Order and normalize columns for readability if present
            column_order = [
                "parent_gen_id",
                "gen_id",
                "evaluation_template",
                "evaluation_model",
                "evaluation_llm_model",
                "score",
                "error",
                "evaluation_response_path",
                # enrichments
                "combo_id",
                "draft_template",
                "generation_template",
                "generation_model",
                "generation_response_path",
                "origin_cohort_id",
                "input_mode",
                "copied_from",
            ]
            existing = [c for c in column_order if c in df.columns]
            df = df[existing + [c for c in df.columns if c not in existing]]
            output_csv.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_csv, index=False)
            return df
    except Exception:
        pass

    # Create only the output directory
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []
    # parsed.txt-only; do not consult parsers or raw fallbacks
    model_map = load_model_mapping(data_root)

    warnings: list[str] = []
    if docs_eval.exists():
        for doc_dir in sorted([p for p in docs_eval.iterdir() if p.is_dir()]):
            gen_id = doc_dir.name
            parsed_fp = doc_dir / "parsed.txt"
            meta_fp = doc_dir / "metadata.json"
            raw_meta_fp = doc_dir / "raw_metadata.json"

            raw_meta: Dict[str, Any] = {}
            if raw_meta_fp.exists():
                try:
                    raw_meta = json.loads(raw_meta_fp.read_text(encoding="utf-8")) or {}
                except Exception:
                    raw_meta = {}
            meta: Dict[str, Any] = {}
            if not parsed_fp.exists():
                # Record missing parsed and continue
                parent_gen_id = ""
                template_id = None
                eval_model = None
                try:
                    if meta_fp.exists():
                        meta = json.loads(meta_fp.read_text(encoding="utf-8"))
                        if isinstance(meta, dict):
                            parent_gen_id = str(meta.get("parent_gen_id") or "")
                            template_id = meta.get("template_id")
                            eval_model = meta.get("model_id")
                            req = ["parent_gen_id", "template_id", "model_id"]
                            present = [k for k in req if meta.get(k)]
                            if len(present) < len(req):
                                warnings.append(f"Warning: {meta_fp} missing keys: {sorted(set(req) - set(present))}")
                                if strict and len(present) == 0:
                                    raise DDError(
                                        Err.DATA_MISSING,
                                        ctx={
                                            "reason": "evaluation_metadata_missing_keys",
                                            "path": str(meta_fp),
                                        },
                                    )
                except Exception:
                    pass
                # Enrich generation-side identifiers from essay/draft metadata when possible
                gen_template = None
                gen_model = None
                combo_id = None
                draft_template = None
                stage = "essay2p"
                gen_path = str((data_root / "gens" / "essay" / parent_gen_id / "parsed.txt").resolve()) if parent_gen_id else ""
                try:
                    if parent_gen_id:
                        emeta_path = data_root / "gens" / "essay" / parent_gen_id / "metadata.json"
                        if emeta_path.exists():
                            emeta = json.loads(emeta_path.read_text(encoding="utf-8")) or {}
                            gen_template = str(emeta.get("template_id") or emeta.get("essay_template") or "")
                            gen_model = str(emeta.get("model_id") or "")
                            draft_id = str(emeta.get("parent_gen_id") or "")
                            if draft_id:
                                dmeta_path = data_root / "gens" / "draft" / draft_id / "metadata.json"
                                if dmeta_path.exists():
                                    dmeta = json.loads(dmeta_path.read_text(encoding="utf-8")) or {}
                                    combo_id = str(dmeta.get("combo_id") or "")
                                    draft_template = str(dmeta.get("template_id") or dmeta.get("draft_template") or "")
                                    if not gen_model:
                                        gen_model = str(dmeta.get("model_id") or "")
                except Exception:
                    pass
                input_mode = raw_meta.get("input_mode")
                copied_from = raw_meta.get("copied_from")
                rows.append({
                    "gen_id": gen_id,
                    "parent_gen_id": parent_gen_id,
                    "template_id": template_id,
                    "evaluation_template": template_id,
                    "evaluation_model": eval_model,
                    "score": None,
                    "error": "missing parsed.txt",
                    "evaluation_response_path": str(parsed_fp),
                    # enrichments
                    "combo_id": combo_id,
                    "draft_template": draft_template,
                    "generation_template": gen_template,
                    "generation_model": gen_model,
                    "stage": stage,
                    "generation_response_path": gen_path,
                    "origin_cohort_id": meta.get("origin_cohort_id") if isinstance(meta, dict) else None,
                    "input_mode": input_mode,
                    "copied_from": copied_from,
                })
                continue
            try:
                text_path = parsed_fp
                text = text_path.read_text(encoding="utf-8", errors="ignore")
            except Exception as e:
                rows.append({"gen_id": gen_id, "score": None, "error": f"Read error: {e}", "evaluation_response_path": str(text_path)})
                continue
            # metadata
            parent_gen_id = ""
            template_id = None
            eval_model = None
            try:
                        if meta_fp.exists():
                            meta = json.loads(meta_fp.read_text(encoding="utf-8"))
                            if isinstance(meta, dict):
                                parent_gen_id = str(meta.get("parent_gen_id") or "")
                                template_id = meta.get("template_id")
                                eval_model = meta.get("model_id")
                                req = ["parent_gen_id", "template_id", "model_id"]
                                present = [k for k in req if meta.get(k)]
                                if len(present) < len(req):
                                    warnings.append(f"Warning: {meta_fp} missing keys: {sorted(set(req) - set(present))}")
                                    if strict and len(present) == 0:
                                        raise DDError(
                                            Err.DATA_MISSING,
                                            ctx={
                                                "reason": "evaluation_metadata_missing_keys",
                                                "path": str(meta_fp),
                                            },
                                        )
            except Exception:
                pass

            # Enrich generation-side identifiers from essay/draft metadata
            gen_template = None
            gen_model = None
            combo_id = None
            draft_template = None
            stage = "essay2p"
            gen_path = str((data_root / "gens" / "essay" / parent_gen_id / "parsed.txt").resolve()) if parent_gen_id else ""
            try:
                if parent_gen_id:
                    emeta_path = data_root / "gens" / "essay" / parent_gen_id / "metadata.json"
                    if emeta_path.exists():
                        emeta = json.loads(emeta_path.read_text(encoding="utf-8")) or {}
                        gen_template = str(emeta.get("template_id") or emeta.get("essay_template") or "")
                        gen_model = str(emeta.get("model_id") or "")
                        draft_id = str(emeta.get("parent_gen_id") or "")
                        if draft_id:
                            dmeta_path = data_root / "gens" / "draft" / draft_id / "metadata.json"
                            if dmeta_path.exists():
                                dmeta = json.loads(dmeta_path.read_text(encoding="utf-8")) or {}
                                combo_id = str(dmeta.get("combo_id") or "")
                                draft_template = str(dmeta.get("template_id") or dmeta.get("draft_template") or "")
                                if not gen_model:
                                    gen_model = str(dmeta.get("model_id") or "")
            except Exception:
                pass

            # parsed.txt-only extraction: numeric-only or trailing SCORE: <float>
            import re
            stripped = text.strip()
            score = None
            err = None
            if re.fullmatch(r"-?\d+(?:\.\d+)?", stripped):
                try:
                    score = float(stripped)
                except Exception:
                    score = None
                    err = "Invalid numeric format in parsed.txt"
            else:
                try:
                    matches = re.findall(r"(?im)^\s*SCORE:\s*(-?\d+(?:\.\d+)?)\s*$", text)
                    if matches:
                        score = float(matches[-1])
                        err = None
                    else:
                        score = None
                        err = "No SCORE found in parsed.txt"
                except Exception as e:
                    score = None
                    err = f"Parse error: {e}"
            rows.append({
                "gen_id": gen_id,
                "parent_gen_id": parent_gen_id,
                "template_id": template_id,
                "evaluation_template": template_id,
                "evaluation_model": eval_model,
                "score": score,
                "error": err,
                "evaluation_response_path": str(text_path),
                # enrichments
                "combo_id": combo_id,
                "draft_template": draft_template,
                "generation_template": gen_template,
                "generation_model": gen_model,
                "stage": stage,
                "generation_response_path": gen_path,
                "origin_cohort_id": meta.get("origin_cohort_id") if isinstance(meta, dict) else None,
                "input_mode": raw_meta.get("input_mode"),
                "copied_from": raw_meta.get("copied_from"),
            })
    else:
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "evaluation_docs_missing", "path": str(docs_eval)},
        )

    df = pd.DataFrame(rows)

    # Ensure consistent schema with defaults
    expected_columns = [
        "gen_id",
        "parent_gen_id",
        "template_id",
        "evaluation_template",
        "evaluation_model",
        "evaluation_llm_model",
        "score",
        "error",
        "evaluation_response_path",
        # enrichments
        "combo_id",
        "draft_template",
        "generation_template",
        "generation_model",
        "stage",
        "generation_response_path",
        "origin_cohort_id",
        "input_mode",
        "copied_from",
    ]
    # No document_id column — doc_id is canonical

    # evaluation_model_name no longer included in output

    # Fill missing expected columns with None to stabilize schema
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Order columns for readability if present
    column_order = [
        "parent_gen_id",
        "gen_id",
        "template_id",
        "evaluation_template",
        "evaluation_model",
        "evaluation_llm_model",
        "score",
        "error",
        "evaluation_response_path",
        # enrichments
        "combo_id",
        "draft_template",
        "generation_template",
        "generation_model",
        "stage",
        "generation_response_path",
        "origin_cohort_id",
        "input_mode",
        "copied_from",
    ]
    existing = [c for c in column_order if c in df.columns]
    df = df[existing + [c for c in df.columns if c not in existing]]

    # Normalize types and NaNs for readability
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    # Replace NaN with empty string for text-like columns
    text_like = [
        "gen_id","parent_gen_id","evaluation_template","evaluation_model","evaluation_llm_model","evaluation_response_path","error"
    ]
    for col in text_like:
        if col in df.columns:
            df[col] = df[col].fillna("")

    # Sort strictly by file modification time (newest first). If unavailable, keep existing order.
    if "evaluation_response_mtime_ns" in df.columns and df["evaluation_response_mtime_ns"].notna().any():
        # Oldest first (reverse of previous behavior)
        df = df.sort_values(by=["evaluation_response_mtime_ns"], ascending=[True], na_position="last").reset_index(drop=True)

    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows to {output_csv}")
    if warnings:
        # Deduplicate warnings
        seen = set()
        for w in warnings:
            if w not in seen:
                print(w)
                seen.add(w)
    if "score" in df.columns:
        valid = df[df["score"].notna()]["score"]
        if len(valid) > 0:
            print(
                "Summary: count=", len(valid),
                "avg=", round(float(valid.mean()), 2),
                "min=", round(float(valid.min()), 2),
                "max=", round(float(valid.max()), 2),
            )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse evaluation scores from gens store")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Base data root directory (default: data)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/7_cross_experiment/aggregated_scores.csv"),
        help="Output CSV path; default is data/7_cross_experiment/aggregated_scores.csv",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if required metadata keys are completely absent in a metadata.json",
    )

    args = parser.parse_args()

    data_root: Path = args.data_root
    output_csv: Path = args.output
    try:
        parse_all(data_root=data_root, output_csv=output_csv, strict=args.strict)
    except DDError as err:
        print(f"ERROR [{err.code.name}]: {err.ctx}")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
