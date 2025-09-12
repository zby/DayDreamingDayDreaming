"""
Group: generation_draft

Asset definitions for the draft (Phase‑1) generation stage.
"""

from dagster import Failure, MetadataValue
from ._decorators import asset_with_boundary
from pathlib import Path
 
import os
from .partitions import draft_gens_partitions
from ..unified.stage_services import execute_draft_llm
from ._helpers import require_membership_row, emit_standard_output_metadata, get_run_id
from ..unified.stage_services import render_template
from ..utils.membership_lookup import find_membership_row_by_gen
from ..constants import DRAFT

 


@asset_with_boundary(
    stage="draft",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="draft_prompt_io_manager",
)
def draft_prompt(
    context,
    content_combinations,
) -> str:
    """Generate Phase 1 prompts for draft generation."""
    gen_id = context.partition_key

    # Read membership to resolve combo/template
    data_root = Path(getattr(context.resources, "data_root", "data"))
    row, cohort = find_membership_row_by_gen(data_root, "draft", str(gen_id))
    if row is None:
        raise Failure(
            description="Cohort membership row not found for draft gen_id",
            metadata={
                "function": MetadataValue.text("draft_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Materialize cohort_id,cohort_membership to register this gen_id; use that partition key"),
            },
        )
    else:
        combo_id = str(row.get("combo_id") or "")
        template_name = str(row.get("template_id") or row.get("draft_template") or "")

    # Resolve content combination; curated combos are provided via content_combinations
    content_combination = next((c for c in content_combinations if c.combo_id == combo_id), None)
    if content_combination is None:
        available_combos = [combo.combo_id for combo in content_combinations[:5]]
        raise Failure(
            description=f"Content combination '{combo_id}' not found in combinations database",
            metadata={
                "combo_id": MetadataValue.text(combo_id),
                "available_combinations_sample": MetadataValue.text(str(available_combos)),
                "total_combinations": MetadataValue.int(len(content_combinations)),
            },
        )

    try:
        prompt = render_template("draft", template_name, {"concepts": content_combination.contents})
    except FileNotFoundError as e:
        raise Failure(
            description=f"Draft template '{template_name}' not found",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("draft"),
                "error": MetadataValue.text(str(e)),
                "resolution": MetadataValue.text("Ensure the template exists in data/1_raw/templates/draft/"),
            },
        )
    except Exception as e:
        templates_root = Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/templates"))
        template_path = templates_root / "draft" / f"{template_name}.txt"
        raise Failure(
            description=f"Error rendering draft template '{template_name}'",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("draft"),
                "template_path": MetadataValue.path(str(template_path)),
                "jinja_message": MetadataValue.text(str(e)),
            },
        ) from e

    context.log.info(f"Generated draft prompt for gen {gen_id} using template {template_name}")
    return prompt


def _draft_response_impl(context, draft_prompt, **_kwargs) -> str:
    """Generate Phase 1 LLM responses for drafts (core implementation)."""
    gen_id = context.partition_key
    # Resolve model/template from cohort membership (required columns enforced)
    data_root = Path(getattr(context.resources, "data_root", "data"))
    row, cohort = require_membership_row(
        context,
        "draft",
        str(gen_id),
        require_columns=["llm_model_id"],
    )
    model_id = str(row.get("llm_model_id") or "").strip()
    template_id = str(row.get("template_id") or row.get("draft_template") or "").strip()
    if not model_id:
        raise Failure(
            description="Missing generation model for draft task",
            metadata={
                "function": MetadataValue.text("draft_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text(
                    "Ensure cohort membership contains llm_model_id for this draft"
                ),
            },
        )

    # Execute via stage_services (writes files; parses via CSV fallback when configured)
    result = execute_draft_llm(
        llm=context.resources.openrouter_client,
        out_dir=data_root / "gens",
        gen_id=str(gen_id),
        template_id=template_id,
        prompt_text=str(draft_prompt) if isinstance(draft_prompt, str) else "",
        model=model_id,
        data_root=data_root,
        max_tokens=getattr(context.resources.experiment_config, "draft_generation_max_tokens", None),
        min_lines=int(getattr(context.resources.experiment_config, "min_draft_lines", 3)),
        fail_on_truncation=True,
        parser_name=None,
        parent_gen_id=None,
        metadata_extra={
            "function": "draft_response",
            "cohort_id": str(cohort) if isinstance(cohort, str) and cohort else None,
            "combo_id": str(row.get("combo_id") or "") or None,
            "run_id": get_run_id(context),
        },
    )

    # Emit standard Dagster metadata for UI
    emit_standard_output_metadata(
        context,
        function="draft_response",
        gen_id=str(gen_id),
        result=result,
        extras={
            "parser_name": (result.metadata or {}).get("parser_name"),
        },
    )

    return str(result.parsed_text or result.raw_text or "")


@asset_with_boundary(
    stage="draft",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="draft_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def draft_response(context, draft_prompt) -> str:
    """Generate Phase 1 LLM responses for drafts.

    Uses cohort_membership for config. Keeps CSV fallback internally (no asset dep).
    """
    parsed = _draft_response_impl(context, draft_prompt)
    return parsed
