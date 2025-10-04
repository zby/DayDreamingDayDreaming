from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Sequence

import pandas as pd

from pydantic import BaseModel, ConfigDict, Field

from daydreaming_dagster.spec_dsl import compile_design, load_spec
from daydreaming_dagster.spec_dsl.models import ExperimentSpec
from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.raw_readers import (
    read_llm_models,
    read_replication_config,
    read_templates,
)


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


class CohortDefinition(BaseModel):
    drafts: list[DraftPlanEntry] = Field(default_factory=list)
    essays: list[EssayPlanEntry] = Field(default_factory=list)
    evaluations: list[EvaluationPlanEntry] = Field(default_factory=list)

    model_config = ConfigDict(frozen=True)

    @classmethod
    def from_design_rows(cls, rows: Sequence[Mapping[str, Any]]) -> "CohortDefinition":
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
class CohortDefinitionAllowlists:
    combos: tuple[str, ...]
    draft_templates: tuple[str, ...]
    essay_templates: tuple[str, ...]
    evaluation_templates: tuple[str, ...]
    generation_models: tuple[str, ...]
    evaluation_models: tuple[str, ...]

    def has_evaluation_axes(self) -> bool:
        return bool(self.evaluation_templates and self.evaluation_models)

    def evaluation_templates_list(self) -> list[str]:
        return list(self.evaluation_templates)

    def evaluation_models_list(self) -> list[str]:
        return list(self.evaluation_models)


def build_allowlists_from_definition(
    definition: CohortDefinition | None,
) -> CohortDefinitionAllowlists:
    if definition is None:
        return CohortDefinitionAllowlists((), (), (), (), (), ())

    draft_templates = {entry.template_id for entry in definition.drafts}
    essay_templates = {entry.template_id for entry in definition.essays}
    evaluation_templates = {entry.template_id for entry in definition.evaluations}
    generation_models = {entry.llm_model_id for entry in definition.drafts}
    evaluation_models = {entry.llm_model_id for entry in definition.evaluations}
    combos = {entry.combo_id for entry in definition.drafts}

    return CohortDefinitionAllowlists(
        combos=tuple(sorted(combos)),
        draft_templates=tuple(sorted(draft_templates)),
        essay_templates=tuple(sorted(essay_templates)),
        evaluation_templates=tuple(sorted(evaluation_templates)),
        generation_models=tuple(sorted(generation_models)),
        evaluation_models=tuple(sorted(evaluation_models)),
    )


def load_cohort_allowlists(
    *,
    data_root: Path,
    cohort_id: str,
    compile_definition: Callable[..., CohortDefinition],
    catalogs: Mapping[str, Any] | None = None,
    definition: CohortDefinition | None = None,
    require_evaluation_axes: bool = True,
    **compile_kwargs: Any,
) -> CohortDefinitionAllowlists:
    """Load cohort allowlists, enforcing spec presence and evaluation axes."""

    cohort_str = str(cohort_id)

    if definition is None:
        spec_dir = Path(data_root) / "cohorts" / cohort_str / "spec"
        if not spec_dir.exists():
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "cohort_spec_required",
                    "cohort_id": cohort_str,
                    "path": str(spec_dir),
                },
            )
        plan = compile_definition(
            path=spec_dir,
            catalogs=catalogs or {},
            **compile_kwargs,
        )
    else:
        plan = definition

    allowlists = build_allowlists_from_definition(plan)

    if require_evaluation_axes and not allowlists.has_evaluation_axes():
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "cohort_spec_missing_evaluation_axes",
                "cohort_id": cohort_str,
            },
        )

    return allowlists


def build_spec_catalogs(data_root: Path) -> dict[str, list[str]]:
    """Hydrate spec catalogs (templates, llms, combos) from the data root."""

    catalogs: dict[str, list[str]] = {}

    def _template_ids(kind: str) -> list[str]:
        df = read_templates(data_root, kind)
        if df.empty:
            return []
        values = {
            str(value).strip()
            for value in df["template_id"].dropna().tolist()
            if str(value).strip()
        }
        return sorted(values)

    drafts = _template_ids("draft")
    if drafts:
        catalogs["draft_template"] = drafts

    essays = _template_ids("essay")
    if essays:
        catalogs["essay_template"] = essays

    evaluations = _template_ids("evaluation")
    if evaluations:
        catalogs["evaluation_template"] = evaluations

    llm_df = read_llm_models(data_root)
    if not llm_df.empty:
        generation_llms = {
            str(value).strip()
            for value in llm_df[llm_df["for_generation"] == True]["id"].dropna().tolist()
            if str(value).strip()
        }
        evaluation_llms = {
            str(value).strip()
            for value in llm_df[llm_df["for_evaluation"] == True]["id"].dropna().tolist()
            if str(value).strip()
        }
        if generation_llms:
            sorted_generation = sorted(generation_llms)
            catalogs["draft_llm"] = sorted_generation
            catalogs.setdefault("essay_llm", sorted_generation)
        if evaluation_llms:
            catalogs["evaluation_llm"] = sorted(evaluation_llms)

    if "essay_llm" in catalogs:
        values = set(catalogs["essay_llm"])
        values.add("None")
        catalogs["essay_llm"] = sorted(values)

    combos: set[str] = set()
    combo_path = data_root / "combo_mappings.csv"
    if combo_path.exists():
        try:
            combo_df = pd.read_csv(combo_path, usecols=["combo_id"])
        except Exception:  # pragma: no cover - best-effort catalog hydration
            combo_df = pd.DataFrame()
        if not combo_df.empty:
            combos.update(
                str(value).strip()
                for value in combo_df["combo_id"].dropna().tolist()
                if str(value).strip()
            )

    if combos:
        catalogs["combo_id"] = sorted(combos)

    return catalogs


def _read_templates_safe(
    data_root: Path,
    stage: str,
    *,
    allowlist: Sequence[str] | None = None,
) -> pd.DataFrame:
    df = read_templates(data_root, stage)
    if df.empty:
        return pd.DataFrame()
    if allowlist:
        allowed = {str(item).strip() for item in allowlist if str(item).strip()}
        if allowed:
            df = df[df["template_id"].astype(str).str.strip().isin(allowed)]
    return df


def _template_mode_map(df: pd.DataFrame, *, default: str = "llm") -> Dict[str, str]:
    if df is None or getattr(df, "empty", True):
        return {}

    mode_map: Dict[str, str] = {}
    for _, row in df.iterrows():
        template_id = _normalize_str(row.get("template_id") or row.get("id"))
        if not template_id:
            continue
        raw_mode = row.get("generator") if "generator" in row.index else None
        mode = (raw_mode or default)
        if isinstance(mode, str):
            mode = mode.strip().lower() or default
        else:
            mode = str(mode).strip().lower() or default
        mode_map[template_id] = mode
    return mode_map


def _require_replication_config(data_root: Path) -> dict[str, int]:
    rep_cfg = read_replication_config(data_root)
    if not isinstance(rep_cfg, dict):
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "replication_config_missing"},
        )
    for stage in ("draft", "essay", "evaluation"):
        value = rep_cfg.get(stage)
        if not isinstance(value, int) or value < 1:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "invalid_replication_config",
                    "stage": stage,
                    "value": value,
                },
            )
    return rep_cfg


def _normalize_str(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class CohortSpecContext:
    definition: CohortDefinition
    allowlists: CohortDefinitionAllowlists
    template_modes: dict[str, Dict[str, str]]
    replication_config: dict[str, int]
    catalogs: Mapping[str, Any]


def load_cohort_context(
    *,
    data_root: Path,
    cohort_id: str,
    compile_definition: Callable[..., CohortDefinition],
    catalogs: Mapping[str, Any] | None = None,
    definition: CohortDefinition | None = None,
    require_evaluation_axes: bool = True,
    **compile_kwargs: Any,
) -> CohortSpecContext:
    """Return compiled spec details, allowlists, template modes, and replication config."""

    cohort_str = str(cohort_id)
    catalogs = catalogs or build_spec_catalogs(data_root)

    if definition is None:
        spec_dir = Path(data_root) / "cohorts" / cohort_str / "spec"
        if not spec_dir.exists():
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "cohort_spec_required",
                    "cohort_id": cohort_str,
                    "path": str(spec_dir),
                },
            )
        definition = compile_definition(
            path=spec_dir,
            catalogs=catalogs,
            **compile_kwargs,
        )

    allowlists = load_cohort_allowlists(
        data_root=data_root,
        cohort_id=cohort_str,
        compile_definition=compile_definition,
        catalogs=catalogs,
        definition=definition,
        require_evaluation_axes=require_evaluation_axes,
        **compile_kwargs,
    )

    template_modes = {
        "draft": _template_mode_map(
            _read_templates_safe(data_root, "draft", allowlist=allowlists.draft_templates)
        ),
        "essay": _template_mode_map(
            _read_templates_safe(data_root, "essay", allowlist=allowlists.essay_templates)
        ),
        "evaluation": _template_mode_map(
            _read_templates_safe(
                data_root, "evaluation", allowlist=allowlists.evaluation_templates
            )
        ),
    }

    replication_config = _require_replication_config(data_root)

    return CohortSpecContext(
        definition=definition,
        allowlists=allowlists,
        template_modes=template_modes,
        replication_config=replication_config,
        catalogs=catalogs,
    )


def compile_cohort_definition(
    spec: ExperimentSpec,
    *,
    catalogs: Mapping[str, Any] | None = None,
    seed: int | None = None,
) -> CohortDefinition:
    rows = compile_design(spec, catalogs=catalogs, seed=seed)
    return CohortDefinition.from_design_rows(rows)


def load_cohort_definition(
    source: str | Path | ExperimentSpec,
    *,
    catalogs: Mapping[str, Any] | None = None,
    seed: int | None = None,
) -> CohortDefinition:
    if isinstance(source, ExperimentSpec):
        spec = source
    else:
        spec_path = Path(source)
        if spec_path.is_dir():
            spec_path = spec_path / "config.yaml"
        spec = load_spec(spec_path)
    return compile_cohort_definition(spec, catalogs=catalogs, seed=seed)


__all__ = [
    "CohortDefinition",
    "DraftPlanEntry",
    "EssayPlanEntry",
    "EvaluationPlanEntry",
    "CohortDefinitionAllowlists",
    "build_allowlists_from_definition",
    "load_cohort_allowlists",
    "compile_cohort_definition",
    "load_cohort_definition",
]
