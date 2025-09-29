from __future__ import annotations

from pathlib import Path
from typing import Iterable, Dict, Any, List
import json
import pandas as pd

from .generation import load_generation
from ..data_layer.paths import Paths


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
        eval_model = md.get("llm_model_id")
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
        draft_template = str(md.get("draft_template") or "").strip()
        generation_template = str(md.get("essay_template_id") or md.get("generation_template") or "").strip()
        generation_model = ""
        parent_draft_id = ""
        if parent_essay_id:
            try:
                emd = load_generation(gens_root, "essay", parent_essay_id).get("metadata") or {}
                generation_template = str(emd.get("template_id") or emd.get("essay_template") or "")
                generation_model = str(emd.get("llm_model_id") or "")
                parent_draft_id = str(emd.get("parent_gen_id") or "")
                if not draft_template:
                    draft_val = emd.get("draft_template")
                    if draft_val is not None:
                        draft_template = str(draft_val).strip()
            except Exception:
                pass

        if parent_draft_id:
            try:
                dmd = load_generation(gens_root, "draft", parent_draft_id).get("metadata") or {}
                combo_id = str(dmd.get("combo_id") or "")
                draft_template = str(dmd.get("template_id") or dmd.get("draft_template") or "")
                if not generation_model:
                    generation_model = str(dmd.get("llm_model_id") or "")
            except Exception:
                pass
        elif not draft_template:
            draft_template = str(md.get("draft_template") or "")

        if not draft_template:
            draft_template_meta = md.get("draft_template")
            if draft_template_meta is not None:
                draft_template = str(draft_template_meta).strip()
        if not generation_template:
            generation_template_meta = md.get("essay_template_id") or md.get("generation_template")
            if generation_template_meta is not None:
                generation_template = str(generation_template_meta).strip()

        cohort_value = None
        if cohort_id is not None and pd.notna(cohort_id):
            cohort_value = str(cohort_id)

        input_mode = None
        copied_from = None
        try:
            raw_meta_path = paths.raw_metadata_path("evaluation", gid)
            if raw_meta_path.exists():
                raw_meta = json.loads(raw_meta_path.read_text(encoding="utf-8")) or {}
                input_mode = raw_meta.get("input_mode")
                copied_from = raw_meta.get("copied_from")
        except Exception:
            pass

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
                "input_mode": input_mode,
                "copied_from": copied_from,
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
        "input_mode",
        "copied_from",
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    # Normalize types
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df[expected_columns + [c for c in df.columns if c not in expected_columns]]
