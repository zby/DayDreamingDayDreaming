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
        eval_doc = load_generation(gens_root, "evaluation", gid)

        md = eval_doc.get("metadata") or {}
        parent_essay_id = str(md.get("parent_gen_id") or "").strip()
        eval_template = md.get("template_id")
        eval_model = md.get("llm_model_id")
        origin_cohort_id = md.get("origin_cohort_id")
        eval_parsed = (eval_doc.get("parsed_text") or "").strip()
        eval_parsed_path = paths.parsed_path("evaluation", gid).resolve()

        score = None
        error = None
        if eval_parsed:
            last_line = eval_parsed.splitlines()[-1].strip()
            try:
                score = float(last_line)
            except ValueError as exc:  # keep row but note failure
                error = f"Invalid parsed.txt: {exc}"
        else:
            error = "missing parsed.txt"

        generation_template = ""
        generation_model = ""
        parent_draft_id = ""
        if parent_essay_id:
            essay_doc = load_generation(gens_root, "essay", parent_essay_id)
            essay_md = essay_doc.get("metadata") or {}
            generation_template = str(
                essay_md.get("template_id")
                or essay_md.get("essay_template_id")
                or ""
            ).strip()
            generation_model = str(essay_md.get("llm_model_id") or "").strip()
            parent_draft_id = str(essay_md.get("parent_gen_id") or "").strip()

        combo_id = str(md.get("combo_id") or "").strip()
        draft_template = str(md.get("draft_template") or "").strip()
        if parent_draft_id:
            draft_doc = load_generation(gens_root, "draft", parent_draft_id)
            draft_md = draft_doc.get("metadata") or {}
            combo_id = str(draft_md.get("combo_id") or combo_id or "").strip()
            draft_template = str(
                draft_md.get("template_id")
                or draft_md.get("draft_template")
                or draft_template
            ).strip()
            if not generation_model:
                generation_model = str(draft_md.get("llm_model_id") or "").strip()

        cohort_value = None
        if origin_cohort_id is not None and pd.notna(origin_cohort_id):
            origin_str = str(origin_cohort_id).strip()
            cohort_value = origin_str or None

        raw_meta_path = paths.raw_metadata_path("evaluation", gid)
        input_mode = None
        copied_from = None
        if raw_meta_path.exists():
            raw_meta = json.loads(raw_meta_path.read_text(encoding="utf-8")) or {}
            input_mode = raw_meta.get("input_mode")
            copied_from = raw_meta.get("copied_from")

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
                "origin_cohort_id": cohort_value,
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
        "origin_cohort_id",
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
