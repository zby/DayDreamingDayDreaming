from __future__ import annotations

from pathlib import Path
from typing import Iterable, Dict, Any, List
import pandas as pd

from .generation import load_generation
from ..config.paths import Paths


def aggregate_evaluation_scores_for_ids(data_root: Path, gen_ids: Iterable[str]) -> pd.DataFrame:
    """Aggregate evaluation scores for the given evaluation gen_ids.

    For each id under data/gens/evaluation/<gen_id> reads parsed.txt and metadata.json, and
    enriches with generation-side metadata (essay/draft). Returns a DataFrame with columns:

    - gen_id, parent_gen_id
    - evaluation_template, evaluation_llm_model
    - score (float or None), error (str or None)
    - evaluation_response_path, generation_response_path
    - combo_id, draft_template, generation_template, generation_model
    """
    paths = Paths.from_str(str(data_root))
    gens_root = paths.gens_root
    rows: List[Dict[str, Any]] = []
    for gid in gen_ids:
        gid = str(gid)
        try:
            eval_doc = load_generation(gens_root, "evaluation", gid)
        except Exception:
            # Missing or unreadable: still emit a row with error
            rows.append(
                {
                    "gen_id": gid,
                    "parent_gen_id": "",
                    "evaluation_template": None,
                    "evaluation_llm_model": None,
                    "score": None,
                    "error": "unreadable evaluation generation",
                    "evaluation_response_path": str(paths.parsed_path("evaluation", gid).resolve()),
                    "combo_id": None,
                    "draft_template": None,
                    "generation_template": None,
                    "generation_model": None,
                    "cohort_id": None,
                    "generation_response_path": "",
                }
            )
            continue

        md = eval_doc.get("metadata") or {}
        parent_essay_id = str(md.get("parent_gen_id") or "")
        eval_template = md.get("template_id")
        # Prefer canonical field name used by new pipeline; fall back to legacy key
        eval_model = md.get("llm_model_id") or md.get("model_id")
        cohort_id = md.get("cohort_id")
        eval_parsed = eval_doc.get("parsed_text")
        eval_parsed_path = paths.parsed_path("evaluation", gid).resolve()

        score = None
        error = None
        if isinstance(eval_parsed, str) and eval_parsed.strip():
            try:
                last = eval_parsed.strip().splitlines()[-1].strip()
                score = float(last)
            except Exception as e:
                error = f"Invalid parsed.txt: {e}"
        else:
            error = "missing parsed.txt"

        combo_id = ""
        generation_template = ""
        generation_model = ""
        parent_draft_id = ""
        if parent_essay_id:
            try:
                emd = load_generation(gens_root, "essay", parent_essay_id).get("metadata") or {}
                generation_template = str(emd.get("template_id") or emd.get("essay_template") or "")
                generation_model = str(
                    emd.get("llm_model_id") or emd.get("model_id") or ""
                )
                parent_draft_id = str(emd.get("parent_gen_id") or "")
            except Exception:
                pass

        draft_template = ""
        if parent_draft_id:
            try:
                dmd = load_generation(gens_root, "draft", parent_draft_id).get("metadata") or {}
                combo_id = str(dmd.get("combo_id") or "")
                draft_template = str(dmd.get("template_id") or dmd.get("draft_template") or "")
                if not generation_model:
                    generation_model = str(
                        dmd.get("llm_model_id") or dmd.get("model_id") or ""
                    )
            except Exception:
                pass

        cohort_value = None
        if cohort_id is not None and pd.notna(cohort_id):
            cohort_value = str(cohort_id)

        rows.append(
            {
                "gen_id": gid,
                "parent_gen_id": parent_essay_id,
                "evaluation_template": eval_template,
                "evaluation_llm_model": eval_model,
                "score": score,
                "error": error,
                "evaluation_response_path": str(eval_parsed_path),
                # enrichments
                "combo_id": combo_id,
                "draft_template": draft_template,
                "generation_template": generation_template,
                "generation_model": generation_model,
                "cohort_id": cohort_value,
                "generation_response_path": str(paths.parsed_path("essay", parent_essay_id).resolve())
                if parent_essay_id
                else "",
            }
        )

    df = pd.DataFrame(rows)
    # Fill missing expected columns with None to stabilize schema
    expected_columns = [
        "gen_id",
        "parent_gen_id",
        "evaluation_template",
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
        "cohort_id",
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    # Normalize types
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df[expected_columns + [c for c in df.columns if c not in expected_columns]]
