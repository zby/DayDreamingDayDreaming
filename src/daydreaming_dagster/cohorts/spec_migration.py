from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import csv
import shutil
import json

import pandas as pd
import yaml

AXIS_ORDER = [
    "combo_id",
    "draft_template",
    "draft_llm",
    "draft_replicate",
    "essay_template",
    "essay_llm",
    "essay_replicate",
    "evaluation_template",
    "evaluation_llm",
    "evaluation_replicate",
]


class SpecGenerationError(RuntimeError):
    """Raised when input membership data cannot be migrated into a spec."""


def _coerce_str(value: Any, *, field: str) -> str:
    text = str(value).strip()
    if not text:
        raise SpecGenerationError(f"missing value for {field}")
    return text


def _coerce_int(value: Any, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
        raise SpecGenerationError(f"invalid integer for {field}: {value!r}") from exc


def _load_membership(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SpecGenerationError(f"membership.csv not found at {path}")
    df = pd.read_csv(path)
    required = {"stage", "gen_id"}
    missing = required - set(df.columns)
    if missing:
        raise SpecGenerationError(f"membership.csv missing columns: {sorted(missing)}")
    return df


def _load_stage_metadata(
    base: Path,
    stage: str,
    gen_ids: Iterable[str],
) -> dict[str, dict[str, Any]]:
    stage_dir = base / "gens" / stage
    results: dict[str, dict[str, Any]] = {}
    for gen_id in gen_ids:
        gen_id_str = _coerce_str(gen_id, field=f"{stage}.gen_id")
        meta_path = stage_dir / gen_id_str / "metadata.json"
        if not meta_path.exists():
            raise SpecGenerationError(f"metadata missing for {stage} gen_id {gen_id_str}")
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise SpecGenerationError(f"invalid metadata for {stage} {gen_id_str}") from exc
        results[gen_id_str] = payload
    return results


def _axis_levels() -> dict[str, set]:
    return {axis: set() for axis in AXIS_ORDER}


def _build_items(
    metadata: dict[str, dict[str, dict[str, Any]]]
) -> tuple[list[list[Any]], dict[str, set]]:
    draft_rows = metadata.get("draft") or {}
    essay_rows = metadata.get("essay") or {}
    evaluation_rows = metadata.get("evaluation") or {}
    if not evaluation_rows:
        raise SpecGenerationError("cohort has no evaluation rows; add evaluations before migrating")

    axis_levels = _axis_levels()
    items: list[list[Any]] = []
    seen: set[tuple[Any, ...]] = set()

    for eval_gen, eval_meta in evaluation_rows.items():
        essay_gen = _coerce_str(eval_meta.get("parent_gen_id"), field="evaluation.parent_gen_id")
        essay = essay_rows.get(essay_gen)
        if essay is None:
            raise SpecGenerationError(f"missing essay row for {essay_gen}")
        draft_gen = _coerce_str(essay.get("parent_gen_id"), field="essay.parent_gen_id")
        draft = draft_rows.get(draft_gen)
        if draft is None:
            raise SpecGenerationError(f"missing draft row for {draft_gen}")

        combo = _coerce_str(draft.get("combo_id"), field="combo_id")
        draft_template = _coerce_str(draft.get("template_id") or draft.get("draft_template"), field="draft.template")
        draft_llm = _coerce_str(draft.get("llm_model_id"), field="draft.llm")
        draft_rep = _coerce_int(draft.get("replicate", 1), field="draft.replicate")

        essay_template = _coerce_str(essay.get("template_id") or essay.get("essay_template"), field="essay.template")
        essay_llm = _coerce_str(essay.get("llm_model_id"), field="essay.llm")
        essay_rep = _coerce_int(essay.get("replicate", 1), field="essay.replicate")

        evaluation_template = _coerce_str(
            eval_meta.get("template_id") or eval_meta.get("evaluation_template"),
            field="evaluation.template",
        )
        evaluation_llm = _coerce_str(eval_meta.get("llm_model_id"), field="evaluation.llm")
        evaluation_rep = _coerce_int(eval_meta.get("replicate", 1), field="evaluation.replicate")

        tuple_values = [
            combo,
            draft_template,
            draft_llm,
            draft_rep,
            essay_template,
            essay_llm,
            essay_rep,
            evaluation_template,
            evaluation_llm,
            evaluation_rep,
        ]
        key = tuple(tuple_values)
        if key in seen:
            continue
        seen.add(key)
        items.append(tuple_values)
        for axis, value in zip(AXIS_ORDER, tuple_values, strict=False):
            axis_levels[axis].add(value)

    items.sort(key=lambda values: tuple(str(v) for v in values))
    return items, axis_levels


def _prepare_axes(axis_levels: dict[str, set]) -> dict[str, list[Any]]:
    axes: dict[str, list[Any]] = {}
    for axis in AXIS_ORDER:
        levels = sorted(axis_levels[axis])
        axes[axis] = [str(level) for level in levels]
    return axes


def generate_spec_bundle(
    data_root: Path | str,
    cohort_id: str,
    *,
    output_dir: Path | None = None,
    overwrite: bool = False,
) -> Path:
    base = Path(data_root)
    cohort = base / "cohorts" / cohort_id
    membership_path = cohort / "membership.csv"
    membership = _load_membership(membership_path)
    stage_ids = {
        stage: membership[membership["stage"] == stage]["gen_id"].astype(str).tolist()
        for stage in {"draft", "essay", "evaluation"}
    }
    metadata = {
        stage: _load_stage_metadata(base, stage, ids)
        for stage, ids in stage_ids.items()
        if ids
    }

    items, axis_levels = _build_items(metadata)
    axes = _prepare_axes(axis_levels)

    target_dir = output_dir or (cohort / "spec")
    target_dir = target_dir.resolve()
    if target_dir.exists():
        if not overwrite:
            raise SpecGenerationError(f"spec directory already exists: {target_dir}")
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    items_dir = target_dir / "items"
    items_dir.mkdir(parents=True, exist_ok=True)
    items_path = items_dir / "cohort_rows.csv"
    with items_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(AXIS_ORDER)
        for row in items:
            writer.writerow([str(value) for value in row])

    config = {
        "axes": axes,
        "rules": {
            "tuples": {
                "cohort_rows": {
                    "axes": AXIS_ORDER,
                    "items": "@file:items/cohort_rows.csv",
                }
            }
        },
        "output": {"field_order": AXIS_ORDER},
    }

    config_path = target_dir / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(config, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return target_dir


__all__ = ["generate_spec_bundle", "SpecGenerationError"]
