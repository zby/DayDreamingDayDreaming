from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple, Literal
from pathlib import Path
import pandas as pd
from daydreaming_dagster.types import Stage
from daydreaming_dagster.utils.raw_readers import read_templates


PROMPT_REQUIRED_BY_STAGE: Dict[Stage, list[str]] = {
    "draft": ["template_id", "combo_id"],
    "essay": ["template_id", "parent_gen_id"],
    "evaluation": ["template_id", "parent_gen_id"],
}

RESPONSE_REQUIRED_BY_STAGE: Dict[Stage, list[str]] = {
    "draft": ["llm_model_id"],
    "essay": ["template_id", "parent_gen_id"],
    "evaluation": ["llm_model_id", "template_id", "parent_gen_id"],
}


def parent_stage_of(stage: Stage) -> Optional[Stage]:
    if stage == "essay":
        return "draft"
    if stage == "evaluation":
        return "essay"
    return None


def exp_config_for(context, stage: Stage) -> tuple[Optional[int], Optional[int]]:
    cfg = getattr(context.resources, "experiment_config", object())
    max_key = "evaluation_max_tokens" if stage == "evaluation" else f"{stage}_generation_max_tokens"
    max_tokens = getattr(cfg, max_key, None)
    min_lines = int(getattr(cfg, "min_draft_lines", 3)) if stage == "draft" else None
    return max_tokens, min_lines


@dataclass
class MembershipFields:
    template_id: str
    llm_model_id: Optional[str]
    parent_gen_id: Optional[str]
    combo_id: Optional[str]


def read_membership_fields(row) -> MembershipFields:
    def _s(v):
        return str(v).strip() if isinstance(v, str) else (str(v).strip() if v is not None else None)

    return MembershipFields(
        template_id=_s(row.get("template_id")) or "",
        llm_model_id=_s(row.get("llm_model_id")) or None,
        parent_gen_id=_s(row.get("parent_gen_id")) or None,
        combo_id=_s(row.get("combo_id")) or None,
    )


@dataclass
class StageSpec:
    prompt_fields: list[str]
    response_fields: list[str]
    parent_stage: Optional[Stage]
    supports_copy_response: bool
    tokens_and_min_lines: Callable[[Any], Tuple[Optional[int], Optional[int]]]
    build_prompt_values: Callable[[Any, str, MembershipFields, Optional[list]], Tuple[dict, dict, Optional[str]]]


def _build_prompt_values_draft(context, gen_id: str, mf: MembershipFields, content_combinations) -> Tuple[dict, dict, Optional[str]]:
    if content_combinations is None:
        raise ValueError("content_combinations is required for draft prompts")
    combo_id = str(mf.combo_id or "")
    content_combination = next((c for c in content_combinations if getattr(c, "combo_id", None) == combo_id), None)
    if content_combination is None:
        raise ValueError(f"Content combination '{combo_id}' not found in combinations database")
    values = {"concepts": content_combination.contents}
    extras = {"combo_id": combo_id}
    return values, extras, None


def _build_prompt_values_essay(context, gen_id: str, mf: MembershipFields, _content_combinations) -> Tuple[dict, dict, Optional[str]]:
    from daydreaming_dagster.assets._helpers import load_parent_parsed_text

    parent_gen, parent_text = load_parent_parsed_text(context, "essay", gen_id, failure_fn_name="essay_prompt")
    values = {"draft_block": parent_text, "links_block": parent_text}
    extras = {"draft_line_count": sum(1 for ln in parent_text.splitlines() if ln.strip())}
    return values, extras, parent_gen


def _build_prompt_values_evaluation(context, gen_id: str, mf: MembershipFields, _content_combinations) -> Tuple[dict, dict, Optional[str]]:
    from daydreaming_dagster.assets._helpers import load_parent_parsed_text

    parent_gen, parent_text = load_parent_parsed_text(context, "evaluation", gen_id, failure_fn_name="evaluation_prompt")
    values = {"response": parent_text}
    extras: dict = {}
    return values, extras, parent_gen


_SPECS: Dict[Stage, StageSpec] = {
    "draft": StageSpec(
        prompt_fields=PROMPT_REQUIRED_BY_STAGE["draft"],
        response_fields=RESPONSE_REQUIRED_BY_STAGE["draft"],
        parent_stage=None,
        supports_copy_response=False,
        tokens_and_min_lines=lambda ctx: exp_config_for(ctx, "draft"),
        build_prompt_values=_build_prompt_values_draft,
    ),
    "essay": StageSpec(
        prompt_fields=PROMPT_REQUIRED_BY_STAGE["essay"],
        response_fields=RESPONSE_REQUIRED_BY_STAGE["essay"],
        parent_stage="draft",
        supports_copy_response=True,
        tokens_and_min_lines=lambda ctx: exp_config_for(ctx, "essay"),
        build_prompt_values=_build_prompt_values_essay,
    ),
    "evaluation": StageSpec(
        prompt_fields=PROMPT_REQUIRED_BY_STAGE["evaluation"],
        response_fields=RESPONSE_REQUIRED_BY_STAGE["evaluation"],
        parent_stage="essay",
        supports_copy_response=True,
        tokens_and_min_lines=lambda ctx: exp_config_for(ctx, "evaluation"),
        build_prompt_values=_build_prompt_values_evaluation,
    ),
}


def get_stage_spec(stage: Stage) -> StageSpec:
    try:
        return _SPECS[stage]
    except KeyError:
        raise ValueError(f"Unsupported stage: {stage}")


# ---- Centralized parser policy ----
def effective_parser_name(
    data_root: Path,
    stage: Stage,
    template_id: str,
    override: Optional[str] = None,
) -> Optional[str]:
    """Determine the effective parser name for a stage/template with uniform rules.

    Policy:
    - Any configuration error (missing CSV, bad parse, unknown stage, missing template row)
      raises an exception (no silent fallbacks).
    - Explicit override wins when provided and non-empty.
    - All stages: if CSV parser is empty/missing, default to "identity".
    """
    if isinstance(override, str) and override.strip():
        return override.strip()

    kind: str = str(stage)
    if kind not in {"draft", "essay", "evaluation"}:
        raise ValueError(f"Unsupported stage for parser resolution: {stage}")

    df = read_templates(Path(data_root), kind, filter_active=False)
    row = df[df["template_id"].astype(str) == str(template_id)]
    if row.empty:
        raise ValueError(f"Template '{template_id}' not found in {kind}_templates.csv")

    raw_val = row.iloc[0].get("parser")
    # Treat NaN/None as empty
    if raw_val is None or (isinstance(raw_val, float) and pd.isna(raw_val)):
        parsed_name = ""
    else:
        parsed_name = str(raw_val).strip()

    # Uniform: default to identity when empty
    return parsed_name or "identity"


# ---- Generator mode policy (moved from assets._helpers) ----
def resolve_generator_mode(
    *,
    kind: Literal["draft", "essay", "evaluation"],
    data_root: Path,
    template_id: str,
    override_from_prompt: Optional[str] = None,
    filter_active: Optional[bool] = None,
) -> Literal["llm", "copy"]:
    """Parametrized resolver for generator modes across all stages.

    Uses data/1_raw/<kind>_templates.csv and the 'generator' column. Accepts an
    override via prompt prefix 'COPY_MODE'. By default does not filter by active,
    matching unified behavior.
    """
    if isinstance(override_from_prompt, str) and override_from_prompt.strip().upper().startswith("COPY_MODE"):
        return "copy"

    if filter_active is None:
        filter_active = False

    df = read_templates(Path(data_root), str(kind), filter_active=bool(filter_active))

    if df.empty:
        raise ValueError(
            f"{str(kind).capitalize()} templates table is empty; cannot resolve generator mode"
        )
    if "generator" not in df.columns:
        raise ValueError(
            f"{str(kind).capitalize()} templates CSV missing required 'generator' column"
        )

    row = df[df["template_id"].astype(str) == str(template_id)]
    if row.empty:
        raise ValueError(f"{str(kind).capitalize()} template not found: {template_id}")
    val = row.iloc[0].get("generator")
    if not isinstance(val, str) or not val.strip():
        raise ValueError(
            f"{str(kind).capitalize()} template has empty/invalid generator value"
        )
    mode = val.strip().lower()
    if mode not in ("llm", "copy"):
        raise ValueError(
            f"{str(kind).capitalize()} template declares unsupported generator '{mode}'"
        )
    return mode  # type: ignore[return-value]
