#!/usr/bin/env python3
"""Build a cohort spec from a curated essay list.

Reads a curated candidate CSV (usually produced by ``scripts/select_top_essays.py``),
collects the corresponding metadata from the gens store, and generates the cohort
membership CSV + DSL spec bundle under ``data/cohorts/<cohort_id>/``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from daydreaming_dagster.assets.group_cohorts import MEMBERSHIP_COLUMNS
from daydreaming_dagster.cohorts import SpecGenerationError, generate_spec_bundle
from daydreaming_dagster.utils.errors import DDError, Err

DEFAULT_CANDIDATE_LIST = Path("data/curation/top_essay_candidates.csv")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cohort-id",
        required=True,
        help="Name of the cohort to create (written under data/cohorts/<id>/)",
    )
    parser.add_argument(
        "--candidate-list",
        type=Path,
        default=DEFAULT_CANDIDATE_LIST,
        help="Path to the curated essay list CSV (default: data/curation/top_essay_candidates.csv)",
    )
    parser.add_argument(
        "--aggregated-scores",
        type=Path,
        default=Path("data/7_cross_experiment/aggregated_scores.csv"),
        help="Aggregated scores CSV used to resolve evaluation gen_ids",
    )
    parser.add_argument(
        "--template",
        help="Override the evaluation template to use (otherwise inferred from candidate list)",
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
        help="Overwrite existing cohort directory if it already exists.",
    )
    return parser.parse_args(argv)


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y"}


def _ensure_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "aggregated_scores_missing_columns", "columns": missing},
        )


def _normalize_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_int(value: object, default: int = 1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_metadata(base: Path, stage: str, gen_id: str) -> dict:
    meta_path = base / "gens" / stage / gen_id / "metadata.json"
    if not meta_path.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={
                "reason": "metadata_missing",
                "stage": stage,
                "gen_id": gen_id,
                "path": str(meta_path),
            },
        )
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - safety net
        raise DDError(
            Err.PARSER_FAILURE,
            ctx={
                "reason": "invalid_metadata",
                "stage": stage,
                "gen_id": gen_id,
                "path": str(meta_path),
            },
        ) from exc


def _build_membership(
    data_root: Path,
    cohort_id: str,
    essay_ids: Sequence[str],
    evaluation_rows: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    seen_stage_ids: set[tuple[str, str]] = set()

    for essay_id in essay_ids:
        essay = _normalize_str(essay_id)
        if not essay:
            continue
        essay_meta = _load_metadata(data_root, "essay", essay)
        draft_id = _normalize_str(essay_meta.get("parent_gen_id"))
        if not draft_id:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "essay_missing_parent", "essay_gen_id": essay},
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
        if (draft_row["stage"], draft_row["gen_id"]) not in seen_stage_ids:
            records.append(draft_row)
            seen_stage_ids.add((draft_row["stage"], draft_row["gen_id"]))

        essay_row = {
            "stage": "essay",
            "gen_id": essay,
            "origin_cohort_id": cohort_id,
            "parent_gen_id": draft_id,
            "combo_id": combo_id,
            "template_id": _normalize_str(essay_meta.get("template_id")),
            "llm_model_id": _normalize_str(essay_meta.get("llm_model_id")),
            "replicate": _normalize_int(essay_meta.get("replicate"), default=1),
        }
        if (essay_row["stage"], essay_row["gen_id"]) not in seen_stage_ids:
            records.append(essay_row)
            seen_stage_ids.add((essay_row["stage"], essay_row["gen_id"]))

        eval_rows = evaluation_rows[evaluation_rows["parent_gen_id"] == essay]
        if eval_rows.empty:
            raise DDError(
                Err.DATA_MISSING,
                ctx={"reason": "no_evaluations_for_essay", "essay_gen_id": essay},
            )

        eval_seen: set[str] = set()
        for _, row in eval_rows.iterrows():
            eval_gen_id = _normalize_str(row.get("gen_id"))
            if not eval_gen_id or eval_gen_id in eval_seen:
                continue
            eval_seen.add(eval_gen_id)
            eval_meta = _load_metadata(data_root, "evaluation", eval_gen_id)
            eval_entry = {
                "stage": "evaluation",
                "gen_id": eval_gen_id,
                "origin_cohort_id": cohort_id,
                "parent_gen_id": essay,
                "combo_id": combo_id,
                "template_id": _normalize_str(eval_meta.get("template_id")),
                "llm_model_id": _normalize_str(eval_meta.get("llm_model_id")),
                "replicate": _normalize_int(eval_meta.get("replicate"), default=1),
            }
            if (eval_entry["stage"], eval_entry["gen_id"]) not in seen_stage_ids:
                records.append(eval_entry)
                seen_stage_ids.add((eval_entry["stage"], eval_entry["gen_id"]))

    return pd.DataFrame.from_records(records, columns=MEMBERSHIP_COLUMNS)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    candidate_path = args.candidate_list
    if not candidate_path.exists():
        print(f"ERROR: candidate list not found at {candidate_path}")
        return 2

    candidates = pd.read_csv(candidate_path)
    essay_column = "essay_gen_id" if "essay_gen_id" in candidates.columns else "parent_gen_id"
    if essay_column not in candidates.columns:
        print("ERROR: candidate list must contain an essay_gen_id column")
        return 2

    if "selected" in candidates.columns:
        mask = candidates["selected"].map(_truthy)
        candidates = candidates[mask]

    essay_ids = [
        value
        for value in candidates[essay_column].astype(str).str.strip().tolist()
        if value
    ]
    if not essay_ids:
        print("ERROR: candidate list has no selected essays")
        return 2

    template = args.template
    if template is None:
        for column in ("evaluation_template", "template", "template_id"):
            if column in candidates.columns:
                values = {
                    str(value).strip()
                    for value in candidates[column].dropna().tolist()
                    if str(value).strip()
                }
                if values:
                    if len(values) > 1:
                        print(
                            "ERROR: multiple templates found in candidate list; "
                            "pass --template to disambiguate"
                        )
                        return 2
                    template = values.pop()
                    break
    if template is None:
        print("ERROR: could not determine evaluation template from candidate list; pass --template")
        return 2

    aggregated_path = args.aggregated_scores
    if not aggregated_path.exists():
        print(f"ERROR: aggregated scores not found at {aggregated_path}")
        return 2

    aggregated = pd.read_csv(aggregated_path)
    _ensure_columns(
        aggregated,
        ["gen_id", "parent_gen_id", "evaluation_template", "evaluation_llm_model", "score"],
    )

    selected_set = set(essay_ids)
    evaluation_rows = aggregated[
        (aggregated["parent_gen_id"].isin(selected_set))
        & (aggregated["evaluation_template"] == template)
    ]
    if evaluation_rows.empty:
        print("ERROR: no evaluation rows found for selected essays")
        return 2

    data_root = args.data_root.resolve()
    cohort_id = args.cohort_id.strip()
    cohort_dir = data_root / "cohorts" / cohort_id
    membership_path = cohort_dir / "membership.csv"

    if membership_path.exists() and not args.overwrite:
        print(
            f"ERROR: membership already exists at {membership_path}; rerun with --overwrite "
            "or use a new cohort id"
        )
        return 2

    try:
        membership = _build_membership(data_root, cohort_id, essay_ids, evaluation_rows)
    except DDError as err:
        print(f"ERROR [{err.code.name}]: {err.ctx}")
        return 2

    cohort_dir.mkdir(parents=True, exist_ok=True)
    membership[MEMBERSHIP_COLUMNS].drop_duplicates(subset=["stage", "gen_id"]).to_csv(
        membership_path,
        index=False,
    )
    print(f"Wrote membership with {len(membership)} rows to {membership_path}")

    # Stash the curated list for traceability.
    curated_dir = cohort_dir / "curation"
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated_path = curated_dir / "essay_candidates.csv"
    candidates.to_csv(curated_path, index=False)
    print(f"Copied candidate list to {curated_path}")

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
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
