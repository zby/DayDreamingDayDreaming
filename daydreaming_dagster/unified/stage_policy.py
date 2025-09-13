from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Literal, Optional

Stage = Literal["draft", "essay", "evaluation"]


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

