from __future__ import annotations

from pathlib import Path
from typing import Iterable, Dict, Any, List, Sequence
import json
import pandas as pd

from .errors import DDError, Err
from .generation import load_generation
from ..data_layer.paths import Paths


def _normalize_parent_id(value: object) -> str | None:
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except Exception:
        if value is None:
            return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


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
            try:
                raw_meta = json.loads(raw_meta_path.read_text(encoding="utf-8")) or {}
            except OSError as exc:
                raise DDError(Err.IO_ERROR, ctx={"path": str(raw_meta_path)}) from exc
            except json.JSONDecodeError as exc:
                raise DDError(Err.PARSER_FAILURE, ctx={"path": str(raw_meta_path)}) from exc
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


def rank_parent_essays_by_template(
    df: pd.DataFrame,
    template_id: str,
    *,
    evaluation_models: Sequence[str] | None = None,
    score_span: float | None = None,
    top_n: int | None = None,
    score_column: str = "avg_score",
) -> pd.DataFrame:
    """Return ordered aggregate scores per parent essay for the given evaluation template.

    The caller supplies a DataFrame shaped like aggregated_scores.csv. We filter down to
    successful rows (score present, no error), aggregate the best score per evaluation model,
    compute average and max across models, and sort descending.

    Parameters
    ----------
    df:
        Input DataFrame containing at least parent_gen_id, evaluation_template,
        evaluation_llm_model, and score columns.
    template_id:
        Evaluation template to rank by (e.g., "novelty").
    evaluation_models:
        Optional subset of evaluation models to include. If omitted, all models present
        for the template participate.
    score_span:
        If provided and >= 0, keeps all rows whose score_column is within span of the top score.
    top_n:
        If provided and >= 0, truncate to the first N rows after span filtering.
    score_column:
        Column used for ranking (defaults to the computed "avg_score").
    """

    required_columns = {"parent_gen_id", "evaluation_template", "evaluation_llm_model", "score"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {sorted(missing)}")

    subset = df[df["evaluation_template"] == template_id].copy()
    if evaluation_models is not None:
        allowed = {str(mid).strip() for mid in evaluation_models if str(mid).strip()}
        if allowed:
            subset = subset[subset["evaluation_llm_model"].astype(str).isin(allowed)]
        else:
            subset = subset.iloc[0:0]

    if subset.empty:
        return pd.DataFrame(columns=["parent_gen_id", "avg_score", "max_score", "models_covered"])

    subset["parent_gen_id"] = subset["parent_gen_id"].apply(_normalize_parent_id)
    subset["evaluation_llm_model"] = subset["evaluation_llm_model"].apply(_normalize_parent_id)

    cond_parent = subset["parent_gen_id"].notna()
    cond_model = subset["evaluation_llm_model"].notna()
    cond_score = subset["score"].notna()
    cond_error = subset["error"].isna() if "error" in subset.columns else True
    subset = subset[cond_parent & cond_model & cond_score & cond_error].copy()

    if subset.empty:
        return pd.DataFrame(columns=["parent_gen_id", "avg_score", "max_score", "models_covered"])

    per_model = (
        subset.groupby(["parent_gen_id", "evaluation_llm_model"])["score"].max().reset_index()
    )
    if per_model.empty:
        return pd.DataFrame(columns=["parent_gen_id", "avg_score", "max_score", "models_covered"])

    pivot = per_model.pivot(
        index="parent_gen_id",
        columns="evaluation_llm_model",
        values="score",
    )
    pivot.columns.name = None
    pivot = pivot.rename(columns=lambda col: f"score__{col}")

    score_columns = [col for col in pivot.columns if str(col).startswith("score__")]
    if score_columns:
        pivot["models_covered"] = pivot[score_columns].notna().sum(axis=1)
        pivot["avg_score"] = pivot[score_columns].mean(axis=1, skipna=True)
        pivot["max_score"] = pivot[score_columns].max(axis=1, skipna=True)
    else:
        pivot["models_covered"] = 0
        pivot["avg_score"] = pd.Series(dtype=float)
        pivot["max_score"] = pd.Series(dtype=float)

    result = pivot.reset_index()
    ordered_columns = ["parent_gen_id", "avg_score", "max_score", "models_covered"]
    remaining = [col for col in score_columns if col not in ordered_columns]
    ordered_columns.extend(sorted(remaining))
    for col in ordered_columns:
        if col not in result.columns:
            result[col] = pd.NA
    result = result[ordered_columns]

    if score_column not in result.columns:
        raise ValueError(f"Score column '{score_column}' not present in result")

    if score_columns:
        result = result.sort_values(by=[score_column, "parent_gen_id"], ascending=[False, True])
    else:
        result = result.sort_values(by=["parent_gen_id"])

    if score_span is not None and score_span >= 0 and not result.empty:
        top_score = result[score_column].max()
        result = result[result[score_column] >= top_score - score_span]

    if top_n is not None and top_n >= 0:
        result = result.head(top_n)

    return result.reset_index(drop=True)


def parent_ids_missing_template(
    df: pd.DataFrame,
    parent_gen_ids: Sequence[str],
    *,
    template_id: str,
    require_score: bool = True,
) -> List[str]:
    """Return parent_gen_ids lacking successful evaluations for the given template."""

    if not parent_gen_ids:
        return []

    subset = df[df["evaluation_template"] == template_id].copy() if "evaluation_template" in df.columns else pd.DataFrame()
    if subset.empty:
        normalized_candidates: List[str] = []
        seen: set[str] = set()
        for pid in parent_gen_ids:
            normalized = _normalize_parent_id(pid)
            if not normalized or normalized in seen:
                continue
            normalized_candidates.append(normalized)
            seen.add(normalized)
        return normalized_candidates

    subset["parent_gen_id"] = subset["parent_gen_id"].apply(_normalize_parent_id)
    if require_score:
        cond_score = subset["score"].notna() if "score" in subset.columns else False
        cond_error = subset["error"].isna() if "error" in subset.columns else True
        subset = subset[cond_score & cond_error]
    subset = subset[subset["parent_gen_id"].notna()]

    available = {str(pid) for pid in subset["parent_gen_id"]}

    missing: List[str] = []
    seen: set[str] = set()
    for pid in parent_gen_ids:
        normalized = _normalize_parent_id(pid)
        if not normalized or normalized in seen:
            continue
        if normalized not in available:
            missing.append(normalized)
        seen.add(normalized)
    return missing
