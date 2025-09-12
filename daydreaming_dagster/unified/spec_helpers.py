from __future__ import annotations

"""
Spec builders and prompt helpers to reduce duplication in assets.

These functions encapsulate common membership lookups and spec construction,
using centralized stage_services for execution.

Usage notes:
- Keep validations/policy in stage_services or assets where appropriate.
- Assets can call these helpers to avoid repeated membership plumbing.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional

from dagster import Failure, MetadataValue

from .stage_services import render_template
from ..utils.membership_lookup import find_membership_row_by_gen


Stage = Literal["draft", "essay", "evaluation"]


@dataclass
class PromptContext:
    data_root: Path
    experiment_config: Any


def build_prompt_from_membership(
    *,
    stage: Stage,
    gen_id: str,
    data_root: Path,
    runner: Optional[object] = None,
    values: Optional[dict[str, Any]] = None,
) -> str:
    """Render a prompt using the shared StrictUndefined Jinja (stage_services), based on membership.

    For draft, the caller should pass values including concept blocks.
    For essay/evaluation, this function expects the caller to pass a values dict
    that already contains the upstream text under the expected keys
    (e.g., {"draft_block": ..., "links_block": ...} or {"response": ...}).
    """
    if not isinstance(gen_id, str) or not gen_id:
        raise Failure(description="Invalid gen_id", metadata={"function": MetadataValue.text("build_prompt_from_membership")})
    mrow, _cohort = find_membership_row_by_gen(data_root, stage, str(gen_id))
    if mrow is None:
        raise Failure(
            description=f"Cohort membership row not found for {stage} gen_id",
            metadata={
                "function": MetadataValue.text("build_prompt_from_membership"),
                "stage": MetadataValue.text(stage),
                "gen_id": MetadataValue.text(str(gen_id)),
            },
        )
    template_id = str(mrow.get("template_id") or "").strip()
    if not template_id:
        raise Failure(
            description=f"Missing template_id for {stage} task",
            metadata={
                "function": MetadataValue.text("build_prompt_from_membership"),
                "stage": MetadataValue.text(stage),
                "gen_id": MetadataValue.text(str(gen_id)),
            },
        )
    # BACKCOMPAT: accept runner param but ignore; use shared render_template
    return render_template(stage, template_id, values or {})


def build_generation_spec_from_membership(
    *,
    stage: Stage,
    gen_id: str,
    data_root: Path,
) -> "MembershipInfo":
    """Resolve unified membership fields for a given stage/gen_id.

    Returns MembershipInfo with common fields only (stage, gen_id, template_id,
    llm_model_id, parent_gen_id, cohort_id when available). Stage-specific policy
    (generator mode, parser, token caps, validations) is left to assets or stage_services.
    """
    if not isinstance(gen_id, str) or not gen_id:
        raise Failure(description="Invalid gen_id", metadata={"function": MetadataValue.text("resolve_membership_common")})

    mrow, cohort_id = find_membership_row_by_gen(data_root, stage, str(gen_id))
    if mrow is None:
        raise Failure(
            description=f"Cohort membership row not found for {stage} gen_id",
            metadata={
                "function": MetadataValue.text("resolve_membership_common"),
                "stage": MetadataValue.text(stage),
                "gen_id": MetadataValue.text(str(gen_id)),
            },
        )

    template_id = str(mrow.get("template_id") or "").strip()
    llm_model_id = str(mrow.get("llm_model_id") or "").strip() or None
    parent_gen_id = str(mrow.get("parent_gen_id") or "").strip() or None

    return MembershipInfo(
        stage=stage,
        gen_id=str(gen_id),
        template_id=template_id,
        llm_model_id=llm_model_id,
        parent_gen_id=parent_gen_id,
        cohort_id=str(cohort_id) if isinstance(cohort_id, str) and cohort_id else None,
    )


@dataclass
class MembershipInfo:
    stage: Stage
    gen_id: str
    template_id: str
    llm_model_id: Optional[str]
    parent_gen_id: Optional[str]
    cohort_id: Optional[str] = None

