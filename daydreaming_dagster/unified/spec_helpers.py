from __future__ import annotations

"""
Spec builders and prompt helpers to reduce duplication in assets.

These functions encapsulate common membership lookups and spec construction,
while keeping StageRunner.run() pure and focused on execution.

Usage notes:
- Keep validations/policy in StageRunner where possible.
- Assets can call these helpers to avoid repeated membership plumbing.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional

from dagster import Failure, MetadataValue

from .stage_runner import StageRunSpec, StageRunner
from ..utils.membership_lookup import find_membership_row_by_gen
from ..utils.raw_readers import read_essay_templates
from ..utils.evaluation_parsing_config import load_parser_map, require_parser_for_template
from ..constants import DRAFT, ESSAY


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
    runner: Optional[StageRunner] = None,
    values: Optional[dict[str, Any]] = None,
) -> str:
    """Render a prompt using StageRunner's StrictUndefined Jinja, based on membership.

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
    r = runner or StageRunner()
    return r.render_template(stage, template_id, values or {})


def build_generation_spec_from_membership(
    *,
    stage: Stage,
    gen_id: str,
    data_root: Path,
    out_dir: Path,
    experiment_config: Any,
    values: Optional[dict[str, Any]] = None,
    prompt_text: Optional[str] = None,
) -> StageRunSpec:
    """Construct a StageRunSpec from cohort membership and configuration.

    - draft: resolves template_id and llm_model_id; sets min_lines and max_tokens.
    - essay: resolves parent_gen_id and template_id; determines generator mode
      from essay_templates.csv; if copy, sets pass_through_from path; if llm,
      sets llm_model_id and max_tokens.
    - evaluation: resolves parent_gen_id, template_id, llm_model_id; sets parser_name
      via evaluation_templates.csv; sets max_tokens.
    """
    if not isinstance(gen_id, str) or not gen_id:
        raise Failure(description="Invalid gen_id", metadata={"function": MetadataValue.text("build_generation_spec_from_membership")})

    mrow, _cohort = find_membership_row_by_gen(data_root, stage, str(gen_id))
    if mrow is None:
        raise Failure(
            description=f"Cohort membership row not found for {stage} gen_id",
            metadata={
                "function": MetadataValue.text("build_generation_spec_from_membership"),
                "stage": MetadataValue.text(stage),
                "gen_id": MetadataValue.text(str(gen_id)),
            },
        )

    # Common fields
    template_id = str(mrow.get("template_id") or "").strip()
    model_id = str(mrow.get("llm_model_id") or "").strip()

    if stage == "draft":
        if not model_id:
            raise Failure(
                description="Missing generation model for draft task",
                metadata={"function": MetadataValue.text("build_generation_spec_from_membership"), "gen_id": MetadataValue.text(str(gen_id))},
            )
        return StageRunSpec(
            stage="draft",
            gen_id=str(gen_id),
            template_id=template_id,
            values=values or {},
            out_dir=out_dir,
            mode="llm",
            model=model_id,
            max_tokens=getattr(experiment_config, "draft_generation_max_tokens", None),
            prompt_text=prompt_text,
            min_lines=int(getattr(experiment_config, "min_draft_lines", 0) or 0),
        )

    if stage == "essay":
        parent_gen_id = str(mrow.get("parent_gen_id") or "").strip()
        if not parent_gen_id:
            raise Failure(
                description="Missing parent_gen_id for essay task",
                metadata={"function": MetadataValue.text("build_generation_spec_from_membership"), "gen_id": MetadataValue.text(str(gen_id))},
            )
        # Determine generator mode from essay_templates.csv
        df = read_essay_templates(data_root, filter_active=False)
        mode = "llm"
        if not df.empty and "generator" in df.columns:
            row = df[df["template_id"].astype(str) == template_id]
            if not row.empty:
                v = str(row.iloc[0].get("generator") or "").strip().lower()
                if v in ("llm", "copy"):
                    mode = v
        spec = StageRunSpec(
            stage="essay",
            gen_id=str(gen_id),
            template_id=template_id,
            values=values or {},
            out_dir=out_dir,
            mode=mode,
            model=(model_id or None),
            max_tokens=getattr(experiment_config, "essay_generation_max_tokens", None),
            prompt_text=prompt_text,
            parent_gen_id=parent_gen_id,
        )
        if mode == "copy":
            spec.pass_through_from = Path(out_dir) / DRAFT / parent_gen_id / "parsed.txt"
        return spec

    if stage == "evaluation":
        parent_gen_id = str(mrow.get("parent_gen_id") or "").strip()
        if not parent_gen_id:
            raise Failure(
                description="Missing parent_gen_id for evaluation task",
                metadata={"function": MetadataValue.text("build_generation_spec_from_membership"), "gen_id": MetadataValue.text(str(gen_id))},
            )
        if not model_id:
            raise Failure(
                description="Missing evaluator model for evaluation task",
                metadata={"function": MetadataValue.text("build_generation_spec_from_membership"), "gen_id": MetadataValue.text(str(gen_id))},
            )
        parser_map = load_parser_map(data_root)
        parser_name = require_parser_for_template(template_id, parser_map)
        return StageRunSpec(
            stage="evaluation",
            gen_id=str(gen_id),
            template_id=template_id,
            values=values or {},
            out_dir=out_dir,
            mode="llm",
            model=model_id,
            parser_name=parser_name,
            max_tokens=getattr(experiment_config, "evaluation_max_tokens", None),
            prompt_text=prompt_text,
            parent_gen_id=parent_gen_id,
        )

    raise Failure(description=f"Unsupported stage: {stage}", metadata={"function": MetadataValue.text("build_generation_spec_from_membership")})

