"""Pure helpers for cohort membership generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, Mapping, Protocol, Sequence

from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.ids import (
    compute_deterministic_gen_id,
    draft_signature,
    evaluation_signature,
    essay_signature,
)

from .spec_planner import CohortDefinition

try:  # pragma: no cover - pandas optional during pure unit tests
    import pandas as _pd  # type: ignore
except Exception:  # pragma: no cover - best effort fallback
    _pd = None


def _normalize_int(value, *, default: int = 1) -> int:
    try:
        if value is None:
            return default
        if _pd is not None and _pd.isna(value):  # type: ignore[arg-type]
            return default
    except Exception:  # pragma: no cover - defensive when pandas missing
        if value is None:
            return default
    try:
        return int(value)
    except Exception:
        return default


@dataclass(frozen=True)
class CohortCatalog:
    """Normalized snapshot of available cohort resources."""

    combos: frozenset[str] = field(default_factory=frozenset)
    draft_templates: frozenset[str] = field(default_factory=frozenset)
    essay_templates: frozenset[str] = field(default_factory=frozenset)
    evaluation_templates: frozenset[str] = field(default_factory=frozenset)
    draft_llms: frozenset[str] = field(default_factory=frozenset)
    essay_llms: frozenset[str] = field(default_factory=frozenset)
    evaluation_llms: frozenset[str] = field(default_factory=frozenset)
    replication_config: Mapping[str, int] = field(default_factory=dict)

    @classmethod
    def from_catalogs(
        cls,
        catalogs: Mapping[str, Sequence[str]] | None,
        *,
        replication_config: Mapping[str, int] | None = None,
    ) -> "CohortCatalog":
        catalogs = catalogs or {}

        def _collect(key: str) -> frozenset[str]:
            values = catalogs.get(key, ())
            normalized = (
                str(item).strip()
                for item in values
                if item is not None and str(item).strip()
            )
            return frozenset(normalized)

        return cls(
            combos=_collect("combo_id"),
            draft_templates=_collect("draft_template"),
            essay_templates=_collect("essay_template"),
            evaluation_templates=_collect("evaluation_template"),
            draft_llms=_collect("draft_llm"),
            essay_llms=_collect("essay_llm"),
            evaluation_llms=_collect("evaluation_llm"),
            replication_config=dict(replication_config or {}),
        )


@dataclass(frozen=True)
class MembershipRow:
    """Normalized cohort membership row with consistent defaults."""

    stage: str
    gen_id: str
    origin_cohort_id: str
    parent_gen_id: str = ""
    combo_id: str = ""
    template_id: str = ""
    llm_model_id: str = ""
    replicate: int = 1

    def to_dict(self) -> Dict[str, object]:
        return {
            "stage": self.stage,
            "gen_id": self.gen_id,
            "origin_cohort_id": self.origin_cohort_id,
            "parent_gen_id": self.parent_gen_id,
            "combo_id": self.combo_id,
            "template_id": self.template_id,
            "llm_model_id": self.llm_model_id,
            "replicate": int(self.replicate),
        }


MEMBERSHIP_COLUMNS = [
    "stage",
    "gen_id",
    "origin_cohort_id",
    "parent_gen_id",
    "combo_id",
    "template_id",
    "llm_model_id",
    "replicate",
]


class GenerationRegistry(Protocol):
    """Minimal interface for reserving generation identifiers."""

    def reserve_draft_id(
        self,
        *,
        combo_id: str,
        template_id: str,
        llm_model_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str: ...

    def reserve_essay_id(
        self,
        *,
        draft_gen_id: str,
        template_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str: ...

    def reserve_evaluation_id(
        self,
        *,
        essay_gen_id: str,
        template_id: str,
        llm_model_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str: ...


def _draft_row(
    *,
    cohort_id: str,
    combo_id: str,
    template_id: str,
    llm_model_id: str,
    gen_id: str,
    replicate: int | str,
) -> MembershipRow:
    return MembershipRow(
        stage="draft",
        gen_id=gen_id,
        origin_cohort_id=str(cohort_id),
        combo_id=str(combo_id),
        template_id=str(template_id),
        llm_model_id=str(llm_model_id),
        replicate=_normalize_int(replicate, default=1),
    )


def _essay_row(
    *,
    cohort_id: str,
    combo_id: str,
    template_id: str,
    llm_model_id: str,
    gen_id: str,
    parent_gen_id: str,
    replicate: int | str,
) -> MembershipRow:
    return MembershipRow(
        stage="essay",
        gen_id=gen_id,
        origin_cohort_id=str(cohort_id),
        parent_gen_id=str(parent_gen_id),
        combo_id=str(combo_id),
        template_id=str(template_id),
        llm_model_id=str(llm_model_id),
        replicate=_normalize_int(replicate, default=1),
    )


def _evaluation_row(
    *,
    cohort_id: str,
    combo_id: str,
    template_id: str,
    llm_model_id: str,
    gen_id: str,
    parent_gen_id: str,
    replicate: int | str,
) -> MembershipRow:
    return MembershipRow(
        stage="evaluation",
        gen_id=gen_id,
        origin_cohort_id=str(cohort_id),
        parent_gen_id=str(parent_gen_id),
        combo_id=str(combo_id),
        template_id=str(template_id),
        llm_model_id=str(llm_model_id),
        replicate=_normalize_int(replicate, default=1),
    )


def generate_membership(
    definition: CohortDefinition,
    *,
    cohort_id: str,
    registry: GenerationRegistry,
) -> list[MembershipRow]:
    """Return membership rows for a compiled cohort definition."""

    if definition is None:
        return []

    cohort_id_str = str(cohort_id)
    rows: list[MembershipRow] = []

    draft_ids: Dict[tuple[str, str, str, int], str] = {}
    for draft_entry in definition.drafts:
        replicate = _normalize_int(draft_entry.replicate, default=1)
        gen_id = registry.reserve_draft_id(
            combo_id=draft_entry.combo_id,
            template_id=draft_entry.template_id,
            llm_model_id=draft_entry.llm_model_id,
            cohort_id=cohort_id_str,
            replicate=replicate,
        )
        draft_ids[draft_entry.key()] = gen_id
        rows.append(
            _draft_row(
                cohort_id=cohort_id_str,
                combo_id=draft_entry.combo_id,
                template_id=draft_entry.template_id,
                llm_model_id=draft_entry.llm_model_id,
                gen_id=gen_id,
                replicate=replicate,
            )
        )

    essay_ids: Dict[tuple[tuple[str, str, str, int], str, str, int], str] = {}
    for essay_entry in definition.essays:
        draft_key = essay_entry.draft.key()
        draft_gen_id = draft_ids.get(draft_key)
        if draft_gen_id is None:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "missing_draft_for_essay",
                    "draft": draft_key,
                    "essay_template": essay_entry.template_id,
                },
            )

        replicate = _normalize_int(essay_entry.replicate, default=1)
        essay_gen_id = registry.reserve_essay_id(
            draft_gen_id=draft_gen_id,
            template_id=essay_entry.template_id,
            cohort_id=cohort_id_str,
            replicate=replicate,
        )
        essay_ids[essay_entry.key()] = essay_gen_id
        rows.append(
            _essay_row(
                cohort_id=cohort_id_str,
                combo_id=essay_entry.draft.combo_id,
                template_id=essay_entry.template_id,
                llm_model_id=essay_entry.llm_model_id,
                gen_id=essay_gen_id,
                parent_gen_id=draft_gen_id,
                replicate=replicate,
            )
        )

    for evaluation_entry in definition.evaluations:
        essay_key = evaluation_entry.essay.key()
        essay_gen_id = essay_ids.get(essay_key)
        if essay_gen_id is None:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "missing_essay_for_evaluation",
                    "evaluation_template": evaluation_entry.template_id,
                },
            )

        replicate = _normalize_int(evaluation_entry.replicate, default=1)
        evaluation_gen_id = registry.reserve_evaluation_id(
            essay_gen_id=essay_gen_id,
            template_id=evaluation_entry.template_id,
            llm_model_id=evaluation_entry.llm_model_id,
            cohort_id=cohort_id_str,
            replicate=replicate,
        )
        rows.append(
            _evaluation_row(
                cohort_id=cohort_id_str,
                combo_id=evaluation_entry.essay.draft.combo_id,
                template_id=evaluation_entry.template_id,
                llm_model_id=evaluation_entry.llm_model_id,
                gen_id=evaluation_gen_id,
                parent_gen_id=essay_gen_id,
                replicate=replicate,
            )
        )

    return rows


class InMemoryGenerationRegistry(GenerationRegistry):
    """Simple registry that mirrors deterministic IDs without filesystem access."""

    def reserve_draft_id(
        self,
        *,
        combo_id: str,
        template_id: str,
        llm_model_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        return compute_deterministic_gen_id(
            "draft",
            draft_signature(combo_id, template_id, llm_model_id, replicate),
        )

    def reserve_essay_id(
        self,
        *,
        draft_gen_id: str,
        template_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        return compute_deterministic_gen_id(
            "essay",
            essay_signature(draft_gen_id, template_id, replicate),
        )

    def reserve_evaluation_id(
        self,
        *,
        essay_gen_id: str,
        template_id: str,
        llm_model_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        return compute_deterministic_gen_id(
            "evaluation",
            evaluation_signature(essay_gen_id, template_id, llm_model_id, replicate),
        )


class GensDataLayerRegistry(GenerationRegistry):
    """Registry backed by ``GensDataLayer`` for reserving deterministic IDs."""

    def __init__(self, data_layer: GensDataLayer) -> None:
        self._data_layer = data_layer

    def reserve_draft_id(
        self,
        *,
        combo_id: str,
        template_id: str,
        llm_model_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        gen_id = self._data_layer.reserve_draft_id(
            combo_id=combo_id,
            template_id=template_id,
            llm_model_id=llm_model_id,
            cohort_id=cohort_id,
            replicate=replicate,
        )
        self._data_layer.reserve_generation("draft", gen_id, create=True)
        return gen_id

    def reserve_essay_id(
        self,
        *,
        draft_gen_id: str,
        template_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        gen_id = self._data_layer.reserve_essay_id(
            draft_gen_id=draft_gen_id,
            template_id=template_id,
            cohort_id=cohort_id,
            replicate=replicate,
        )
        self._data_layer.reserve_generation("essay", gen_id, create=True)
        return gen_id

    def reserve_evaluation_id(
        self,
        *,
        essay_gen_id: str,
        template_id: str,
        llm_model_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        gen_id = self._data_layer.reserve_evaluation_id(
            essay_gen_id=essay_gen_id,
            template_id=template_id,
            llm_model_id=llm_model_id,
            cohort_id=cohort_id,
            replicate=replicate,
        )
        self._data_layer.reserve_generation("evaluation", gen_id, create=True)
        return gen_id
