#!/usr/bin/env python3
"""Rank essays by evaluation scores and stage a curated candidate list.

This replaces the legacy ``selected_essays.txt`` generator. The script reads
``data/7_cross_experiment/aggregated_scores.csv`` (or a caller-provided CSV),
aggregates scores for the requested evaluation template and models, and writes
an editable candidate list to ``data/curation/top_essay_candidates.csv`` by
default. After human review/editing, run ``scripts/build_cohort_from_list.py``
to materialise the final cohort spec and membership CSV.

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
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.evaluation_scores import rank_parent_essays_by_template

DEFAULT_OUTPUT = Path("data/curation/top_essay_candidates.csv")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cohort-id",
        required=True,
        help="Label for the prospective cohort (stored alongside the candidate list)",
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
        "--overwrite",
        action="store_true",
        help="Overwrite the output file if it already exists.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Destination for the editable candidate CSV (default: data/curation/top_essay_candidates.csv)",
    )
    return parser.parse_args(argv)


def _ensure_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise DDError(Err.DATA_MISSING, ctx={"reason": "aggregated_scores_missing_columns", "columns": missing})


def _select_candidates(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    rank_kwargs: dict[str, object] = {}
    if args.max_only:
        rank_kwargs["score_span"] = 0.0
    elif args.score_span is not None:
        rank_kwargs["score_span"] = args.score_span
    else:
        rank_kwargs["top_n"] = args.top_n

    ranked = rank_parent_essays_by_template(
        df,
        template_id=args.template,
        evaluation_models=args.average_models,
        **rank_kwargs,
    )
    if ranked.empty:
        raise DDError(
            Err.DATA_MISSING,
            ctx={
                "reason": "selection_empty",
                "template": args.template,
                "models": args.average_models,
            },
        )

    ranked = ranked.reset_index(drop=True)
    ranked.insert(0, "selection_rank", ranked.index + 1)
    ranked.insert(1, "selected", True)
    ranked.rename(columns={"parent_gen_id": "essay_gen_id"}, inplace=True)
    ranked["evaluation_template"] = args.template
    ranked["source_models"] = ", ".join(args.average_models)

    if args.max_only:
        method = "max_only"
        limit_value: object = None
    elif args.score_span is not None:
        method = "score_span"
        limit_value = args.score_span
    else:
        method = "top_n"
        limit_value = args.top_n

    ranked["selection_method"] = method
    ranked["selection_limit"] = limit_value
    ranked["proposed_cohort_id"] = args.cohort_id.strip()

    columns = list(ranked.columns)
    score_cols = [col for col in columns if col.startswith("score__")]
    lead_cols = [
        "selection_rank",
        "selected",
        "essay_gen_id",
        "avg_score",
        "max_score",
        "models_covered",
    ]
    ordered = lead_cols + score_cols + [
        "evaluation_template",
        "source_models",
        "selection_method",
        "selection_limit",
        "proposed_cohort_id",
    ]
    ranked = ranked[
        [col for col in ordered if col in ranked.columns]
        + [col for col in columns if col not in ordered]
    ]
    return ranked


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    aggregated_path = args.aggregated_scores
    if not aggregated_path.exists():
        raise SystemExit(f"aggregated_scores not found: {aggregated_path}")

    df = pd.read_csv(aggregated_path)
    _ensure_columns(df, ["gen_id", "parent_gen_id", "evaluation_template", "evaluation_llm_model", "score"])

    try:
        candidates = _select_candidates(df, args)
    except DDError as err:
        print(f"ERROR [{err.code.name}]: {err.ctx}")
        return 2

    output_path = args.output
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not args.overwrite:
        print(
            f"ERROR: output already exists at {output_path}; rerun with --overwrite "
            "or set --output"
        )
        return 2

    candidates.to_csv(output_path, index=False)

    print(f"Wrote candidate list with {len(candidates)} essays to {output_path}")
    print(
        "Edit the 'selected' column as needed, then run "
        "scripts/build_cohort_from_list.py to generate the cohort spec."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
