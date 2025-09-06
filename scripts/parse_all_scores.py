#!/usr/bin/env python3
"""
Parse evaluation scores for all response files and write a consolidated CSV.

This script performs CROSS-EXPERIMENT analysis by scanning ALL evaluation response
files in the directory, not just those from the current experiment's task definitions.
This enables historical analysis across multiple experimental runs.

Key cross-experiment features:
- Scans all *.txt files in responses directory (ignores evaluation_tasks.csv)
- Parses identifiers from filenames when task metadata is unavailable
- Handles multiple template types and evaluation strategies from different experiments
- Graceful fallback parsing for legacy evaluation formats

Defaults:
- Responses dir: data/4_evaluation/evaluation_responses (read-only)
- Tasks dir: data/2_tasks (used for metadata enrichment only, not file filtering)
- Output: data/7_cross_experiment/parsed_scores.csv

Usage examples:
- uv run scripts/parse_all_scores.py --output data/cross_experiment/parsed_scores.csv
- uv run scripts/parse_all_scores.py --data-root data --output tmp/parsed_scores.csv
- uv run scripts/parse_all_scores.py --responses-dir data/4_evaluation/evaluation_responses --output tmp/parsed_scores.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from datetime import datetime

import pandas as pd

from daydreaming_dagster.utils.eval_response_parser import parse_llm_response
from daydreaming_dagster.utils.evaluation_parsing_config import load_parser_map, require_parser_for_template


def load_eval_parsing_strategies(base_data: Path) -> Dict[str, str]:
    # Backwards-compatible name; now strictly reads 'parser' column
    return load_parser_map(base_data)


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
    eval_dir = base_data / "1_raw" / "evaluation_templates"
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
    link_dir = base_data / "1_raw" / "generation_templates" / "links"
    draft_dir = base_data / "1_raw" / "generation_templates" / "draft"
    essay_dir = base_data / "1_raw" / "generation_templates" / "essay"
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

    New format (split tasks, 2025-09):
      evaluation_task_id = {document_id}__{eval_template}__{eval_model}
      document_id        = essay_task_id (two-phase) or link_task_id (draft-as-one-phase)
      essay_task_id      = {combo_id}_{link_template}_{generation_model}_{essay_template}

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
        # Draft-as-one-phase case: no essay_template found, but may have link_template + model
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
    })

    return result


def detect_parsing_strategy(evaluation_template: Optional[str], strategy_map: Optional[Dict[str, str]] = None) -> str:
    # Deprecated: use require_parser_for_template
    if evaluation_template is None:
        raise ValueError("evaluation_template is required in strict mode")
    if not strategy_map or evaluation_template not in strategy_map:
        raise ValueError(f"No parser configured for template '{evaluation_template}'")
    return strategy_map[evaluation_template]


def parse_strict(text: str, template: str, parser_map: Dict[str, str]) -> Dict[str, Any]:
    parser = require_parser_for_template(template, parser_map)
    res = parse_llm_response(text, parser)
    return {"score": res["score"], "error": None}


# Removed load_tasks function - no longer needed for tasks-free implementation


def parse_all(
    responses_dir: Path,
    output_csv: Path,
) -> pd.DataFrame:
    """Parse all evaluation response files and write a consolidated CSV.
    
    TASKS-FREE IMPLEMENTATION: Uses only filename parsing, no task CSV dependencies.
    This enables true cross-experiment analysis without requiring task definitions.
    """

    # Do not create or modify the responses directory; require it to exist to avoid
    # interfering with the standard pipeline structure.
    if not responses_dir.exists() or not responses_dir.is_dir():
        raise FileNotFoundError(f"Responses directory not found: {responses_dir}")

    # Create only the output directory (outside the pipeline directories, as provided by user)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []

    # Load known templates from data root to improve parsing robustness
    # Expect responses_dir like data/4_evaluation/evaluation_responses
    # Base data root should be 'data'
    base_data = responses_dir.parents[1] if len(responses_dir.parents) >= 2 else Path("data")
    known = load_known_templates(base_data)
    strategy_map = load_eval_parsing_strategies(base_data)
    model_map = load_model_mapping(base_data)

    # CROSS-EXPERIMENT: Scan all *.txt files and parse entirely from filenames
    # No dependency on task CSV files - works across all experiments
    candidate_ids: List[str] = [p.stem for p in responses_dir.glob("*.txt")]

    for evaluation_task_id in candidate_ids:
        file_path = responses_dir / f"{evaluation_task_id}.txt"
        if not file_path.exists():
            rows.append({
                "evaluation_task_id": evaluation_task_id,
                "score": None,
                "error": f"Missing file: {file_path}",
                "evaluation_response_path": str(file_path),
            })
            continue

        # File times (mtime as proxy for creation time on Linux)
        try:
            stat = file_path.stat()
            # Use nanosecond precision for stable ordering; use mtime as proxy for creation
            mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1e9)))
            mtime_epoch = mtime_ns / 1e9
            mtime_iso = datetime.fromtimestamp(mtime_epoch).isoformat()
        except Exception:
            mtime_ns = None
            mtime_epoch = None
            mtime_iso = ""

        text = file_path.read_text(encoding="utf-8", errors="ignore")

        # Parse all metadata from filename - no task CSV dependencies
        id_parts = parse_identifiers_from_eval_task_id(evaluation_task_id, known)
        tpl = id_parts.get("evaluation_template")
        if not tpl:
            result = {"score": None, "error": "Unknown evaluation_template in filename; cannot choose parser"}
        else:
            try:
                result = parse_strict(text, tpl, strategy_map)
            except Exception as e:
                result = {"score": None, "error": f"Parse error: {e}"}

        row: Dict[str, Any] = {
            "evaluation_task_id": evaluation_task_id,
            "document_id": id_parts.get("document_id"),
            "score": result["score"],
            "error": result["error"],
            "evaluation_template": id_parts.get("evaluation_template"),
            "evaluation_model": id_parts.get("evaluation_model"),
            "essay_task_id": id_parts.get("essay_task_id"),
            "combo_id": id_parts.get("combo_id"),
            # Canonical going forward: draft_template; keep link_template for compatibility
            "draft_template": id_parts.get("link_template"),
            "link_template": id_parts.get("link_template"),
            "generation_template": id_parts.get("generation_template"),
            "generation_model": id_parts.get("generation_model"),
            "evaluation_response_path": str(file_path),
            "evaluation_response_mtime": mtime_iso,
            "evaluation_response_mtime_epoch": mtime_epoch,
            "evaluation_response_mtime_ns": mtime_ns,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Ensure consistent schema with defaults
    expected_columns = [
        "evaluation_task_id",
        "document_id",
        "essay_task_id",
        "combo_id",
        "link_template",
        "generation_template",
        "generation_model",
        "evaluation_template",
        "evaluation_model",
        "evaluation_model_name",
        "score",
        "error",
        "evaluation_response_path",
    ]

    if "document_id" not in df.columns:
        # Derive from evaluation_task_id if possible (new format)
        if "evaluation_task_id" in df.columns:
            def _doc_from_tid(tid: str) -> Optional[str]:
                if not isinstance(tid, str):
                    return None
                parts = tid.split("__")
                return parts[0] if len(parts) == 3 else None
            df["document_id"] = df["evaluation_task_id"].map(_doc_from_tid)
        else:
            df["document_id"] = None

    # evaluation_model_name from mapping
    if "evaluation_model" in df.columns:
        df["evaluation_model_name"] = df["evaluation_model"].map(model_map).fillna(df.get("evaluation_model"))
    else:
        df["evaluation_model_name"] = None

    # Fill missing expected columns with None to stabilize schema
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Order columns for readability if present
    column_order = [
        "document_id",
        "combo_id",
        "link_template",
        "generation_template",
        "generation_model",
        "evaluation_template",
        "evaluation_model",
        "evaluation_model_name",
        "score",
        "error",
        "evaluation_task_id",
        "essay_task_id",
        "evaluation_response_path",
    ]
    existing = [c for c in column_order if c in df.columns]
    df = df[existing + [c for c in df.columns if c not in existing]]

    # Normalize types and NaNs for readability
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    # Replace NaN with empty string for text-like columns
    text_like = [
        "document_id","combo_id","link_template","generation_template","generation_model",
        "evaluation_template","evaluation_model","evaluation_model_name","evaluation_task_id","essay_task_id","evaluation_response_path","error"
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
    parser = argparse.ArgumentParser(description="Parse evaluation scores from response files")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Base data root directory (default: data)",
    )
    parser.add_argument(
        "--responses-dir",
        type=Path,
        default=None,
        help="Path to evaluation responses directory (default: <data-root>/4_evaluation/evaluation_responses)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path('data/7_cross_experiment/parsed_scores.csv'),
        help="Output CSV path; default is data/7_cross_experiment/parsed_scores.csv",
    )

    args = parser.parse_args()

    data_root: Path = args.data_root
    responses_dir: Path = args.responses_dir or (data_root / "4_evaluation" / "evaluation_responses")
    output_csv: Path = args.output

    parse_all(responses_dir=responses_dir, output_csv=output_csv)


if __name__ == "__main__":
    main()
