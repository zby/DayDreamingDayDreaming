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
- Output: data/cross_experiment/parsed_scores.csv

Usage examples:
- uv run scripts/parse_all_scores.py --output data/cross_experiment/parsed_scores.csv
- uv run scripts/parse_all_scores.py --data-root data --output tmp/parsed_scores.csv
- uv run scripts/parse_all_scores.py --responses-dir data/4_evaluation/evaluation_responses --output tmp/parsed_scores.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from daydreaming_dagster.utils.eval_response_parser import parse_llm_response


LEGACY_TEMPLATES = {
    "creativity-metrics",
    "daydreaming-verification",
    "iterative-loops",
    "scientific-rigor",
}


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

    # Generation templates (links and essay)
    link_csv = base_data / "1_raw" / "link_templates.csv"
    essay_csv = base_data / "1_raw" / "essay_templates.csv"
    link_dir = base_data / "1_raw" / "generation_templates" / "links"
    essay_dir = base_data / "1_raw" / "generation_templates" / "essay"
    if link_csv.exists():
        try:
            df = pd.read_csv(link_csv)
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


def parse_identifiers_from_eval_task_id(
    evaluation_task_id: str,
    known: Optional[Dict[str, Set[str]]] = None,
) -> Dict[str, Any]:
    """Parse identifiers from an evaluation_task_id string using the new scheme.

    New format (split tasks):
      evaluation_task_id = {essay_task_id}_{eval_template}_{eval_model}
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

    parts = evaluation_task_id.split("_")
    if len(parts) < 5:  # Need at least combo_X_template_model_eval_template_eval_model
        return result

    # Strategy: Find evaluation template by scanning from right for a token in eval_templates
    eval_template = None
    eval_model = None
    remaining_parts = parts[:]
    if eval_templates:
        for i in range(len(parts)-1, -1, -1):
            token = parts[i]
            if token in eval_templates:
                eval_template = token
                eval_model = "_".join(parts[i+1:]) if i+1 < len(parts) else None
                remaining_parts = parts[:i]
                break
    if not eval_template:
        # Fallback: assume last two logical segments
        eval_template = parts[-2] if len(parts) >= 2 else None
        eval_model = parts[-1] if len(parts) >= 1 else None
        remaining_parts = parts[:-2] if len(parts) >= 2 else []

    # Now parse essay_task_id side from remaining_parts
    # Expect: combo_parts + link_template + generation_model (+ underscores) + essay_template
    if len(remaining_parts) < 4:  # Need at least combo + link_template + model + essay_template
        result.update({
            "evaluation_template": eval_template,
            "evaluation_model": eval_model,
        })
        return result

    # Essay template is last known essay template token from the right
    essay_template = None
    gen_model = None
    link_template_val = None
    combo_parts: List[str] = []

    if essay_templates and link_templates:
        for i in range(len(remaining_parts)-1, -1, -1):
            token = remaining_parts[i]
            if token in essay_templates:
                essay_template = token
                # Find last link template before position i
                l_idx = None
                for j in range(i-1, -1, -1):
                    if remaining_parts[j] in link_templates:
                        l_idx = j
                        break
                if l_idx is not None:
                    link_template_val = remaining_parts[l_idx]
                    gen_tokens = remaining_parts[l_idx+1:i]
                    gen_model = "_".join(gen_tokens) if gen_tokens else None
                    combo_parts = remaining_parts[:l_idx]
                break

    combo_id = "_".join(combo_parts) if combo_parts else None
    essay_task_id = "_".join(remaining_parts) if remaining_parts else None

    result.update({
        "essay_task_id": essay_task_id,
        "combo_id": combo_id,
        "link_template": link_template_val,
        "generation_template": essay_template,
        "generation_model": gen_model,
        "evaluation_template": eval_template,
        "evaluation_model": eval_model,
    })

    return result


def detect_parsing_strategy(evaluation_template: Optional[str]) -> str:
    """Detect appropriate parsing strategy based on evaluation template.

    If template is unknown, prefer the modern "in_last_line" strategy.
    """
    if not evaluation_template:
        return "in_last_line"
    if evaluation_template == "daydreaming-verification-v2":
        return "in_last_line"
    if evaluation_template in LEGACY_TEMPLATES:
        return "complex"
    return "in_last_line"


def try_parse_with_fallback(text: str, primary_strategy: str) -> Dict[str, Any]:
    """Try parsing with primary strategy, then fall back to the other strategy.

    Returns a dict with keys: score, error.
    """
    try:
        res = parse_llm_response(text, primary_strategy)
        return {"score": res["score"], "error": None}
    except Exception as primary_exc:
        fallback = "complex" if primary_strategy == "in_last_line" else "in_last_line"
        try:
            res = parse_llm_response(text, fallback)
            return {"score": res["score"], "error": None}
        except Exception as fallback_exc:
            return {
                "score": None,
                "error": f"Parse error (primary={primary_strategy}: {primary_exc}); (fallback={fallback}: {fallback_exc})",
            }


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

        text = file_path.read_text(encoding="utf-8", errors="ignore")

        # Parse all metadata from filename - no task CSV dependencies
        id_parts = parse_identifiers_from_eval_task_id(evaluation_task_id, known)
        strategy = detect_parsing_strategy(id_parts.get("evaluation_template"))
        result = try_parse_with_fallback(text, strategy)

        row: Dict[str, Any] = {
            "evaluation_task_id": evaluation_task_id,
            "score": result["score"],
            "error": result["error"],
            "evaluation_template": id_parts.get("evaluation_template"),
            "evaluation_model": id_parts.get("evaluation_model"),
            "essay_task_id": id_parts.get("essay_task_id"),
            "combo_id": id_parts.get("combo_id"),
            "link_template": id_parts.get("link_template"),
            "generation_template": id_parts.get("generation_template"),
            "generation_model": id_parts.get("generation_model"),
            "evaluation_response_path": str(file_path),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Order columns for readability if present
    column_order = [
        "combo_id",
        "link_template",
        "generation_template",
        "generation_model",
        "evaluation_template",
        "evaluation_model",
        "score",
        "error",
        "evaluation_task_id",
        "essay_task_id",
        "evaluation_response_path",
    ]
    existing = [c for c in column_order if c in df.columns]
    df = df[existing + [c for c in df.columns if c not in existing]]

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
        default=Path('data/cross_experiment/parsed_scores.csv'),
        help="Output CSV path; default is data/cross_experiment/parsed_scores.csv",
    )

    args = parser.parse_args()

    data_root: Path = args.data_root
    responses_dir: Path = args.responses_dir or (data_root / "4_evaluation" / "evaluation_responses")
    output_csv: Path = args.output

    parse_all(responses_dir=responses_dir, output_csv=output_csv)


if __name__ == "__main__":
    main()
