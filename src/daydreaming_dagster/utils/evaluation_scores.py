from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Dict, Any, List, Sequence, Callable
import json
import pandas as pd

from .errors import DDError, Err
from .generation import load_generation
from ..data_layer.paths import Paths


@dataclass
class _EvaluationRecord:
    gen_id: str
    metadata: Dict[str, Any]
    parsed_text: str
    parsed_path: Path


@dataclass
class _EssayRecord:
    gen_id: str
    metadata: Dict[str, Any]


@dataclass
class _DraftRecord:
    gen_id: str
    metadata: Dict[str, Any]


class _EvaluationScoreAggregator:
    def __init__(
        self,
        *,
        paths: Paths,
        load_generation_fn: Callable[[Path, str, str], Dict[str, Any]] = load_generation,
    ) -> None:
        self._paths = paths
        self._load_generation = load_generation_fn

    def collect_rows(self, gen_ids: Iterable[str]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for raw_gid in gen_ids:
            gid = str(raw_gid)
            if not gid:
                continue
            eval_record = self._load_evaluation_record(gid)
            if eval_record is None:
                continue

            parent_essay = self._load_parent_essay(eval_record)
            parent_draft = self._load_parent_draft(parent_essay)
            parent_essay_id = parent_essay.gen_id if parent_essay else self._safe_str(
                eval_record.metadata.get("parent_gen_id")
            )

            score, score_error = self._parse_score(eval_record.parsed_text)
            combo_id, draft_template, generation_template, generation_model = self._resolve_generation_fields(
                eval_record.metadata,
                parent_essay.metadata if parent_essay else {},
                parent_draft.metadata if parent_draft else {},
            )

            origin_cohort_id = self._normalize_origin(eval_record.metadata.get("origin_cohort_id"))
            raw_meta = self._load_raw_metadata(gid)

            rows.append(
                {
                    "gen_id": gid,
                    "parent_gen_id": parent_essay_id,
                    "evaluation_template": eval_record.metadata.get("template_id"),
                    "evaluation_llm_model": eval_record.metadata.get("llm_model_id"),
                    "score": score,
                    "error": score_error,
                    "evaluation_response_path": str(eval_record.parsed_path),
                    "combo_id": combo_id,
                    "draft_template": draft_template,
                    "generation_template": generation_template,
                    "generation_model": generation_model,
                    "origin_cohort_id": origin_cohort_id,
                    "generation_response_path": self._essay_parsed_path(parent_essay),
                    "input_mode": raw_meta.get("input_mode") if raw_meta else None,
                    "copied_from": raw_meta.get("copied_from") if raw_meta else None,
                }
            )
        return rows

    def _load_evaluation_record(self, gen_id: str) -> _EvaluationRecord | None:
        doc = self._load_generation(self._paths.gens_root, "evaluation", gen_id)
        metadata = doc.get("metadata") or {}
        parsed_text = (doc.get("parsed_text") or "").strip()
        parsed_path = self._paths.parsed_path("evaluation", gen_id).resolve()
        return _EvaluationRecord(gen_id=gen_id, metadata=metadata, parsed_text=parsed_text, parsed_path=parsed_path)

    def _load_parent_essay(self, record: _EvaluationRecord) -> _EssayRecord | None:
        parent_essay_id = self._normalize_str(record.metadata.get("parent_gen_id"))
        if not parent_essay_id:
            return None
        essay_doc = self._load_generation(self._paths.gens_root, "essay", parent_essay_id)
        metadata = essay_doc.get("metadata") or {}
        return _EssayRecord(gen_id=parent_essay_id, metadata=metadata)

    def _load_parent_draft(self, essay_record: _EssayRecord | None) -> _DraftRecord | None:
        if essay_record is None:
            return None
        parent_draft_id = self._normalize_str(essay_record.metadata.get("parent_gen_id"))
        if not parent_draft_id:
            return None
        draft_doc = self._load_generation(self._paths.gens_root, "draft", parent_draft_id)
        metadata = draft_doc.get("metadata") or {}
        return _DraftRecord(gen_id=parent_draft_id, metadata=metadata)

    def _parse_score(self, parsed_text: str) -> tuple[float | None, str | None]:
        if not parsed_text:
            return None, "missing parsed.txt"
        last_line = parsed_text.splitlines()[-1].strip()
        try:
            return float(last_line), None
        except ValueError as exc:
            return None, f"Invalid parsed.txt: {exc}"

    def _resolve_generation_fields(
        self,
        eval_metadata: Dict[str, Any],
        essay_metadata: Dict[str, Any],
        draft_metadata: Dict[str, Any],
    ) -> tuple[str, str, str, str]:
        combo_id = self._normalize_str(eval_metadata.get("combo_id")) or ""
        draft_template = self._normalize_str(eval_metadata.get("draft_template")) or ""
        generation_template = self._normalize_str(
            essay_metadata.get("template_id")
            or essay_metadata.get("essay_template_id")
        ) or ""
        generation_model = self._normalize_str(essay_metadata.get("llm_model_id")) or ""

        if draft_metadata:
            combo_id = self._normalize_str(draft_metadata.get("combo_id")) or combo_id
            draft_template = self._normalize_str(
                draft_metadata.get("template_id")
                or draft_metadata.get("draft_template")
            ) or draft_template
            if not generation_model:
                generation_model = self._normalize_str(draft_metadata.get("llm_model_id")) or ""

        return combo_id, draft_template, generation_template, generation_model

    def _normalize_origin(self, value: object) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return None
        except Exception:
            pass
        text = str(value).strip()
        return text or None

    def _essay_parsed_path(self, essay_record: _EssayRecord | None) -> str:
        if essay_record is None:
            return ""
        essay_path = self._paths.parsed_path("essay", essay_record.gen_id).resolve()
        return str(essay_path)

    def _load_raw_metadata(self, gen_id: str) -> Dict[str, Any] | None:
        raw_meta_path = self._paths.raw_metadata_path("evaluation", gen_id)
        if not raw_meta_path.exists():
            return None
        try:
            return json.loads(raw_meta_path.read_text(encoding="utf-8")) or {}
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(raw_meta_path)}) from exc
        except json.JSONDecodeError as exc:
            raise DDError(Err.PARSER_FAILURE, ctx={"path": str(raw_meta_path)}) from exc

    @staticmethod
    def _normalize_str(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _safe_str(value: object) -> str:
        text = _EvaluationScoreAggregator._normalize_str(value)
        return text or ""


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
    aggregator = _EvaluationScoreAggregator(paths=paths)
    rows = aggregator.collect_rows(gen_ids)
    return _rows_to_dataframe(rows)


def _rows_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    expected_columns = [
        "gen_id",
        "parent_gen_id",
        "evaluation_template",
        "evaluation_llm_model",
        "score",
        "error",
        "evaluation_response_path",
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

    The caller supplies a DataFrame shaped like cohort_aggregated_scores.csv. We filter down to
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
