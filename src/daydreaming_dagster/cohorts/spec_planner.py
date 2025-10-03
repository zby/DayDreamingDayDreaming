from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field

from daydreaming_dagster.spec_dsl import compile_design, load_spec
from daydreaming_dagster.spec_dsl.models import ExperimentSpec
from daydreaming_dagster.utils.errors import DDError, Err


def _require_str(row: Mapping[str, Any], key: str, *, aliases: Iterable[str] = ()) -> str:
    candidates = (key, *list(aliases))
    for candidate in candidates:
        if candidate not in row:
            continue
        value = row[candidate]
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    raise DDError(Err.INVALID_CONFIG, ctx={"field": key, "reason": "missing"})


def _replicate_index(
    row: Mapping[str, Any],
    primary: str,
    *,
    fallbacks: Sequence[str] = (),
    default: int = 1,
) -> int:
    for candidate in (primary, *fallbacks):
        if candidate not in row:
            continue
        value = row[candidate]
        if value is None or str(value).strip() == "":
            continue
        try:
            idx = int(value)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"field": candidate, "value": value},
                cause=exc,
            )
        if idx < 1:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"field": candidate, "value": value},
            )
        return idx
    return default


class DraftPlanEntry(BaseModel):
    combo_id: str
    template_id: str
    llm_model_id: str
    replicate: int = Field(1, ge=1)

    model_config = ConfigDict(frozen=True)

    def key(self) -> tuple[str, str, str, int]:
        return (self.combo_id, self.template_id, self.llm_model_id, self.replicate)


class EssayPlanEntry(BaseModel):
    draft: DraftPlanEntry
    template_id: str
    llm_model_id: str
    replicate: int = Field(1, ge=1)

    model_config = ConfigDict(frozen=True)

    def key(self) -> tuple[tuple[str, str, str, int], str, str, int]:
        return (self.draft.key(), self.template_id, self.llm_model_id, self.replicate)


class EvaluationPlanEntry(BaseModel):
    essay: EssayPlanEntry
    template_id: str
    llm_model_id: str
    replicate: int = Field(1, ge=1)

    model_config = ConfigDict(frozen=True)

    def key(self) -> tuple[tuple[tuple[str, str, str, int], str, str, int], str, str, int]:
        return (
            self.essay.key(),
            self.template_id,
            self.llm_model_id,
            self.replicate,
        )


class CohortPlan(BaseModel):
    drafts: list[DraftPlanEntry] = Field(default_factory=list)
    essays: list[EssayPlanEntry] = Field(default_factory=list)
    evaluations: list[EvaluationPlanEntry] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)

    @classmethod
    def from_design_rows(cls, rows: Sequence[Mapping[str, Any]]) -> "CohortPlan":
        if not rows:
            return cls()

        draft_entries: "OrderedDict[tuple[str, str, str, int], DraftPlanEntry]" = OrderedDict()
        essay_entries: "OrderedDict[tuple[tuple[str, str, str, int], str, str, int], EssayPlanEntry]" = (
            OrderedDict()
        )
        evaluation_entries: "OrderedDict[tuple[Any, ...], EvaluationPlanEntry]" = OrderedDict()

        for row in rows:
            if not isinstance(row, Mapping):  # pragma: no cover - defensive
                raise DDError(
                    Err.INVALID_CONFIG,
                    ctx={"field": "row", "reason": "not_mapping"},
                )

            combo_id = _require_str(row, "combo_id", aliases=("combo",))
            draft_tpl = _require_str(row, "draft_template")
            draft_llm = _require_str(row, "draft_llm", aliases=("draft_model", "generation_llm"))
            draft_rep = _replicate_index(
                row,
                "draft_replicate",
                fallbacks=("draft_template_replicate", "draft_rep"),
            )

            draft_key = (combo_id, draft_tpl, draft_llm, draft_rep)
            draft = draft_entries.get(draft_key)
            if draft is None:
                draft = DraftPlanEntry(
                    combo_id=combo_id,
                    template_id=draft_tpl,
                    llm_model_id=draft_llm,
                    replicate=draft_rep,
                )
                draft_entries[draft_key] = draft

            essay_tpl = _require_str(row, "essay_template")
            essay_llm = _require_str(row, "essay_llm")
            essay_rep = _replicate_index(
                row,
                "essay_replicate",
                fallbacks=("essay_template_replicate", "essay_rep"),
            )

            essay_key = (draft_key, essay_tpl, essay_llm, essay_rep)
            essay = essay_entries.get(essay_key)
            if essay is None:
                essay = EssayPlanEntry(
                    draft=draft,
                    template_id=essay_tpl,
                    llm_model_id=essay_llm,
                    replicate=essay_rep,
                )
                essay_entries[essay_key] = essay

            evaluation_tpl = _require_str(row, "evaluation_template")
            evaluation_llm = _require_str(row, "evaluation_llm")
            evaluation_rep = _replicate_index(
                row,
                "evaluation_replicate",
                fallbacks=(
                    "evaluation_template_replicate",
                    "evaluation_llm_replicate",
                    "evaluation_rep",
                ),
            )
            evaluation_key = (essay_key, evaluation_tpl, evaluation_llm, evaluation_rep)
            if evaluation_key not in evaluation_entries:
                evaluation_entries[evaluation_key] = EvaluationPlanEntry(
                    essay=essay,
                    template_id=evaluation_tpl,
                    llm_model_id=evaluation_llm,
                    replicate=evaluation_rep,
                )

        return cls(
            drafts=list(draft_entries.values()),
            essays=list(essay_entries.values()),
            evaluations=list(evaluation_entries.values()),
        )

    def iter_bundles(self) -> Iterable[tuple[DraftPlanEntry, EssayPlanEntry, EvaluationPlanEntry]]:
        for evaluation in self.evaluations:
            yield (evaluation.essay.draft, evaluation.essay, evaluation)


@dataclass(frozen=True)
class CohortPlanAllowlists:
    combos: tuple[str, ...]
    draft_templates: tuple[str, ...]
    essay_templates: tuple[str, ...]
    evaluation_templates: tuple[str, ...]
    generation_models: tuple[str, ...]
    evaluation_models: tuple[str, ...]


def build_allowlists_from_plan(plan: CohortPlan | None) -> CohortPlanAllowlists:
    if plan is None:
        return CohortPlanAllowlists((), (), (), (), (), ())

    draft_templates = {entry.template_id for entry in plan.drafts}
    essay_templates = {entry.template_id for entry in plan.essays}
    evaluation_templates = {entry.template_id for entry in plan.evaluations}
    generation_models = {entry.llm_model_id for entry in plan.drafts}
    evaluation_models = {entry.llm_model_id for entry in plan.evaluations}
    combos = {entry.combo_id for entry in plan.drafts}

    return CohortPlanAllowlists(
        combos=tuple(sorted(combos)),
        draft_templates=tuple(sorted(draft_templates)),
        essay_templates=tuple(sorted(essay_templates)),
        evaluation_templates=tuple(sorted(evaluation_templates)),
        generation_models=tuple(sorted(generation_models)),
        evaluation_models=tuple(sorted(evaluation_models)),
    )


def compile_cohort_plan(
    spec: ExperimentSpec,
    *,
    catalogs: Mapping[str, Any] | None = None,
    seed: int | None = None,
) -> CohortPlan:
    rows = compile_design(spec, catalogs=catalogs, seed=seed)
    return CohortPlan.from_design_rows(rows)


def load_cohort_plan(
    path: str | Path,
    *,
    catalogs: Mapping[str, Any] | None = None,
    seed: int | None = None,
) -> CohortPlan:
    spec = load_spec(path)
    return compile_cohort_plan(spec, catalogs=catalogs, seed=seed)


__all__ = [
    "CohortPlan",
    "DraftPlanEntry",
    "EssayPlanEntry",
    "EvaluationPlanEntry",
    "CohortPlanAllowlists",
    "build_allowlists_from_plan",
    "compile_cohort_plan",
    "load_cohort_plan",
]
