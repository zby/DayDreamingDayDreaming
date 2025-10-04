#!/usr/bin/env python3
"""Select top essays from aggregated scores and generate a cohort spec.

This replaces the legacy selected_essays.txt workflow. The script ranks essays
using `data/7_cross_experiment/aggregated_scores.csv`, builds a trimmed
membership consisting of the selected draft/essay/evaluation rows, and calls the
spec migration helper to emit a DSL cohort spec bundle.

Examples:
  uv run python scripts/select_top_essays.py \
      --cohort-id novelty-top-30 \
      --template novelty \
      --top-n 30

  uv run python scripts/select_top_essays.py \
      --cohort-id novelty-span \
      --template novelty \
      --score-span 0.5

  uv run python scripts/select_top_essays.py \
      --cohort-id novelty-max \
      --template novelty \
      --max-only
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from daydreaming_dagster.assets.group_cohorts import MEMBERSHIP_COLUMNS
from daydreaming_dagster.cohorts import generate_spec_bundle, SpecGenerationError
from daydreaming_dagster.utils.errors import DDError, Err


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cohort-id",
        required=True,
        help="Name of the cohort spec to generate (written under data/cohorts/<id>/)",
    )
    parser.add_argument(
        "--template",
        required=True,
        help="Evaluation template to rank by (e.g. novelty, novelty-v2)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Select the top N essays by average score (default: 10)",
    )
    parser.add_argument(
        "--score-span",
        type=float,
        default=None,
        help="Include all essays within this score distance from the top (overrides --top-n)",
    )
    parser.add_argument(
        "--max-only",
        action="store_true",
        help="Select all essays tied for the maximum score (overrides --top-n and --score-span)",
    )
    parser.add_argument(
        "--aggregated-scores",
        type=Path,
        default=Path("data/7_cross_experiment/aggregated_scores.csv"),
        help="Path to aggregated_scores.csv (default: data/7_cross_experiment/aggregated_scores.csv)",
    )
    parser.add_argument(
        "--average-models",
        nargs="*",
        default=["sonnet-4", "gemini_25_pro"],
        help="Evaluation models to average scores across (default: sonnet-4 gemini_25_pro)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Project data root containing gens/ and cohorts/ (default: ./data)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing membership/spec directories if they already exist.",
    )
    return parser.parse_args(argv)


def _ensure_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise DDError(Err.DATA_MISSING, ctx={"reason": "aggregated_scores_missing_columns", "columns": missing})


def _select_parent_gen_ids(df: pd.DataFrame, args: argparse.Namespace) -> list[str]:
    filtered = df[
        (df["evaluation_template"] == args.template)
        & (df["evaluation_llm_model"].isin(args.average_models))
        & df["score"].notna()
    ].copy()
    if filtered.empty:
        raise DDError(
            Err.DATA_MISSING,
            ctx={
                "reason": "no_scores_for_template",
                "template": args.template,
                "models": args.average_models,
            },
        )

    per_model = (
        filtered.groupby(["parent_gen_id", "evaluation_llm_model"])["score"].max().reset_index()
    )
    averaged = (
        per_model.groupby("parent_gen_id")["score"].mean().reset_index()
        .sort_values(by=["score", "parent_gen_id"], ascending=[False, True])
    )

    if averaged.empty:
        raise DDError(Err.DATA_MISSING, ctx={"reason": "no_valid_scores_after_group"})

    if args.max_only:
        max_score = averaged["score"].max()
        selected = averaged[averaged["score"] == max_score]
    elif args.score_span is not None:
        max_score = averaged["score"].max()
        threshold = max_score - args.score_span
        selected = averaged[averaged["score"] >= threshold]
    else:
        selected = averaged.head(args.top_n)

    parent_ids = selected["parent_gen_id"].astype(str).tolist()
    if not parent_ids:
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "selection_empty", "template": args.template},
        )
    return parent_ids


def _load_metadata(base: Path, stage: str, gen_id: str) -> dict:
    meta_path = base / "gens" / stage / gen_id / "metadata.json"
    if not meta_path.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "metadata_missing", "stage": stage, "gen_id": gen_id, "path": str(meta_path)},
        )
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - safety net
        raise DDError(
            Err.PARSER_FAILURE,
            ctx={"reason": "invalid_metadata", "stage": stage, "gen_id": gen_id, "path": str(meta_path)},
        ) from exc


def _normalize_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_int(value: object, default: int = 1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _build_membership(
    data_root: Path,
    cohort_id: str,
    parent_gen_ids: Sequence[str],
    evaluation_rows: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    seen_stage_ids: set[tuple[str, str]] = set()

    for essay_gen_id in parent_gen_ids:
        essay_id = _normalize_str(essay_gen_id)
        if not essay_id:
            continue
        essay_meta = _load_metadata(data_root, "essay", essay_id)
        draft_id = _normalize_str(essay_meta.get("parent_gen_id"))
        if not draft_id:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "essay_missing_parent", "essay_gen_id": essay_id},
            )
        draft_meta = _load_metadata(data_root, "draft", draft_id)
        combo_id = _normalize_str(draft_meta.get("combo_id"))
        if not combo_id:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "draft_missing_combo", "draft_gen_id": draft_id},
            )

        draft_row = {
            "stage": "draft",
            "gen_id": draft_id,
            "origin_cohort_id": cohort_id,
            "parent_gen_id": "",
            "combo_id": combo_id,
            "template_id": _normalize_str(draft_meta.get("template_id")),
            "llm_model_id": _normalize_str(draft_meta.get("llm_model_id")),
            "replicate": _normalize_int(draft_meta.get("replicate"), default=1),
        }
        key = (draft_row["stage"], draft_row["gen_id"])
        if key not in seen_stage_ids:
            records.append(draft_row)
            seen_stage_ids.add(key)

        essay_row = {
            "stage": "essay",
            "gen_id": essay_id,
            "origin_cohort_id": cohort_id,
            "parent_gen_id": draft_id,
            "combo_id": combo_id,
            "template_id": _normalize_str(essay_meta.get("template_id")),
            "llm_model_id": _normalize_str(essay_meta.get("llm_model_id")),
            "replicate": _normalize_int(essay_meta.get("replicate"), default=1),
        }
        key = (essay_row["stage"], essay_row["gen_id"])
        if key not in seen_stage_ids:
            records.append(essay_row)
            seen_stage_ids.add(key)

        candidate_evals = evaluation_rows[evaluation_rows["parent_gen_id"] == essay_id]
        if candidate_evals.empty:
            raise DDError(
                Err.DATA_MISSING,
                ctx={"reason": "no_evaluations_for_essay", "essay_gen_id": essay_id},
            )

        eval_ids_used: set[str] = set()
        for _, eval_row in candidate_evals.iterrows():
            eval_gen_id = _normalize_str(eval_row.get("gen_id"))
            if not eval_gen_id or eval_gen_id in eval_ids_used:
                continue
            eval_ids_used.add(eval_gen_id)
            eval_meta = _load_metadata(data_root, "evaluation", eval_gen_id)
            evaluation_entry = {
                "stage": "evaluation",
                "gen_id": eval_gen_id,
                "origin_cohort_id": cohort_id,
                "parent_gen_id": essay_id,
                "combo_id": combo_id,
                "template_id": _normalize_str(eval_meta.get("template_id")),
                "llm_model_id": _normalize_str(eval_meta.get("llm_model_id")),
                "replicate": _normalize_int(eval_meta.get("replicate"), default=1),
            }
            key = (evaluation_entry["stage"], evaluation_entry["gen_id"])
            if key not in seen_stage_ids:
                records.append(evaluation_entry)
                seen_stage_ids.add(key)

    membership = pd.DataFrame.from_records(records, columns=MEMBERSHIP_COLUMNS)
    return membership


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    aggregated_path = args.aggregated_scores
    if not aggregated_path.exists():
        raise SystemExit(f"aggregated_scores not found: {aggregated_path}")

    df = pd.read_csv(aggregated_path)
    _ensure_columns(df, ["gen_id", "parent_gen_id", "evaluation_template", "evaluation_llm_model", "score"])

    try:
        parent_ids = _select_parent_gen_ids(df, args)
    except DDError as err:
        print(f"ERROR [{err.code.name}]: {err.ctx}")
        return 2

    selected_set = set(parent_ids)
    evaluation_rows = df[(df["parent_gen_id"].isin(selected_set)) & (df["evaluation_template"] == args.template)]
    if evaluation_rows.empty:
        print("ERROR: No evaluation rows found for selected essays")
        return 2

    data_root = args.data_root.resolve()
    cohort_id = args.cohort_id.strip()
    cohort_dir = data_root / "cohorts" / cohort_id
    membership_path = cohort_dir / "membership.csv"
    if membership_path.exists() and not args.overwrite:
        print(f"ERROR: membership already exists at {membership_path}; rerun with --overwrite")
        return 2

    try:
        membership = _build_membership(data_root, cohort_id, parent_ids, evaluation_rows)
    except DDError as err:
        print(f"ERROR [{err.code.name}]: {err.ctx}")
        return 2

    cohort_dir.mkdir(parents=True, exist_ok=True)
    membership[MEMBERSHIP_COLUMNS].drop_duplicates(subset=["stage", "gen_id"]).to_csv(
        membership_path,
        index=False,
    )
    print(f"Wrote membership with {len(membership)} rows to {membership_path}")

    try:
        spec_dir = generate_spec_bundle(
            data_root=data_root,
            cohort_id=cohort_id,
            overwrite=args.overwrite,
        )
    except SpecGenerationError as exc:
        print(f"ERROR: failed to generate spec bundle: {exc}")
        return 2

    print(f"Generated spec under {spec_dir}")
    print(f"Selected essays (count={len(parent_ids)}): {', '.join(parent_ids)}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
