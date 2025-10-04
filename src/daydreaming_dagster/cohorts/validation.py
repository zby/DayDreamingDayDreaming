"""Validation helpers for cohort definitions."""

from __future__ import annotations

from typing import Iterable, Sequence

from daydreaming_dagster.utils.errors import DDError, Err

from .membership_engine import CohortCatalog, MembershipRow
from .spec_planner import CohortDefinition


def _normalize_set(values: Iterable[object]) -> set[str]:
    normalized: set[str] = set()
    for raw in values:
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            normalized.add(text)
    return normalized


def _ensure_present(allowed: set[str], reason: str) -> None:
    if allowed:
        return
    raise DDError(
        Err.INVALID_CONFIG,
        ctx={"reason": reason},
    )


def _validate_subset(
    *,
    values: Iterable[object],
    allowed: Iterable[str],
    reason: str,
    ctx_extra: dict[str, object] | None = None,
) -> None:
    required = _normalize_set(values)
    if not required:
        return

    allowed_set = _normalize_set(allowed)
    _ensure_present(allowed_set, reason)

    missing = sorted(required - allowed_set)
    if not missing:
        return

    ctx = {"reason": reason, "missing": missing}
    if ctx_extra:
        ctx.update(ctx_extra)
    raise DDError(Err.INVALID_CONFIG, ctx=ctx)


def validate_cohort_definition(
    definition: CohortDefinition | None,
    *,
    catalog: CohortCatalog,
) -> None:
    """Ensure the compiled cohort definition only references allowlisted resources."""

    if definition is None:
        return

    _validate_subset(
        values=(entry.combo_id for entry in definition.drafts),
        allowed=catalog.combos,
        reason="catalog_combos_missing",
    )

    _validate_subset(
        values=(entry.template_id for entry in definition.drafts),
        allowed=catalog.draft_templates,
        reason="catalog_draft_templates_missing",
        ctx_extra={"stage": "draft"},
    )

    _validate_subset(
        values=(entry.template_id for entry in definition.essays),
        allowed=catalog.essay_templates,
        reason="catalog_essay_templates_missing",
        ctx_extra={"stage": "essay"},
    )

    _validate_subset(
        values=(entry.template_id for entry in definition.evaluations),
        allowed=catalog.evaluation_templates,
        reason="catalog_evaluation_templates_missing",
        ctx_extra={"stage": "evaluation"},
    )

    _validate_subset(
        values=(entry.llm_model_id for entry in definition.drafts),
        allowed=catalog.draft_llms or catalog.essay_llms,
        reason="catalog_generation_llms_missing",
        ctx_extra={"stage": "draft"},
    )

    _validate_subset(
        values=(entry.llm_model_id for entry in definition.essays),
        allowed=catalog.essay_llms,
        reason="catalog_essay_llms_missing",
        ctx_extra={"stage": "essay"},
    )

    _validate_subset(
        values=(entry.llm_model_id for entry in definition.evaluations),
        allowed=catalog.evaluation_llms,
        reason="catalog_evaluation_llms_missing",
        ctx_extra={"stage": "evaluation"},
    )


def validate_membership_against_catalog(
    rows: Iterable[MembershipRow] | Sequence[MembershipRow],
    *,
    catalog: CohortCatalog,
) -> None:
    """Ensure membership rows only reference cataloged combos, templates, and models."""

    materialized: Sequence[MembershipRow]
    if isinstance(rows, Sequence):
        materialized = rows
    else:
        materialized = tuple(rows)

    if not materialized:
        return

    _validate_subset(
        values=(row.combo_id for row in materialized),
        allowed=catalog.combos,
        reason="catalog_combos_missing",
    )

    _validate_subset(
        values=(row.template_id for row in materialized if row.stage == "draft"),
        allowed=catalog.draft_templates,
        reason="catalog_draft_templates_missing",
        ctx_extra={"stage": "draft"},
    )

    _validate_subset(
        values=(row.template_id for row in materialized if row.stage == "essay"),
        allowed=catalog.essay_templates,
        reason="catalog_essay_templates_missing",
        ctx_extra={"stage": "essay"},
    )

    _validate_subset(
        values=(row.template_id for row in materialized if row.stage == "evaluation"),
        allowed=catalog.evaluation_templates,
        reason="catalog_evaluation_templates_missing",
        ctx_extra={"stage": "evaluation"},
    )

    _validate_subset(
        values=(row.llm_model_id for row in materialized if row.stage == "draft"),
        allowed=catalog.draft_llms or catalog.essay_llms,
        reason="catalog_generation_llms_missing",
        ctx_extra={"stage": "draft"},
    )

    _validate_subset(
        values=(row.llm_model_id for row in materialized if row.stage == "essay"),
        allowed=catalog.essay_llms,
        reason="catalog_essay_llms_missing",
        ctx_extra={"stage": "essay"},
    )

    _validate_subset(
        values=(row.llm_model_id for row in materialized if row.stage == "evaluation"),
        allowed=catalog.evaluation_llms,
        reason="catalog_evaluation_llms_missing",
        ctx_extra={"stage": "evaluation"},
    )
