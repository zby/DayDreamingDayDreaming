"""
Group: generation_essays

Asset definitions for the essay (Phase‑2) generation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import essay_gens_partitions
from ..unified.stage_services import render_template, execute_essay_copy, execute_essay_llm
from ._helpers import (
    require_membership_row,
    load_generation_parsed_text,
    resolve_essay_generator_mode,
    emit_standard_output_metadata,
    get_run_id,
)
from ..constants import DRAFT, ESSAY



def _essay_prompt_impl(context) -> str:
    """Generate Phase‑2 prompts based on Phase‑1 drafts."""
    gen_id = context.partition_key
    row, _cohort = require_membership_row(context, "essay", str(gen_id), require_columns=["template_id", "parent_gen_id"])
    template_name = str(row.get("template_id") or row.get("essay_template") or "")
    parent_gen_id = row.get("parent_gen_id")

    generator_mode = resolve_essay_generator_mode(Path(context.resources.data_root), template_name)
    if generator_mode == "copy":
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_prompt"),
                "mode": MetadataValue.text(generator_mode),
                "essay_template": MetadataValue.text(template_name),
            }
        )
        return f"{generator_mode.upper()}_MODE: no prompt needed"

    # Resolve Phase‑1 text strictly via parent_gen_id (gen-id required)
    if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
        raise Failure(
            description="Missing parent_doc_id for essay doc",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure essay row exists in cohort membership with a valid parent_gen_id"),
            },
        )
    draft_text = load_generation_parsed_text(context, "draft", str(parent_gen_id), failure_fn_name="essay_prompt")
    used_source = "draft_gens_parent"
    draft_lines = [line.strip() for line in draft_text.split("\n") if line.strip()]
    # Enforce non-empty upstream draft text to avoid empty prompts
    # experiment_config is required via asset definition
    min_lines = int(context.resources.experiment_config.min_draft_lines)
    if len(draft_lines) < max(1, min_lines):
        data_root = Path(getattr(context.resources, "data_root", "data"))
        draft_dir = data_root / "gens" / DRAFT / str(parent_gen_id)
        raise Failure(
            description="Upstream draft text is empty/too short for essay prompt",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "draft_line_count": MetadataValue.int(len(draft_lines)),
                "min_required_lines": MetadataValue.int(min_lines),
                "draft_gen_dir": MetadataValue.path(str(draft_dir)),
            },
        )

    try:
        prompt = render_template("essay", template_name, {"draft_block": draft_text, "links_block": draft_text})
    except FileNotFoundError as e:
        raise Failure(
            description=f"Essay template '{template_name}' not found",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "essay_template": MetadataValue.text(template_name),
                "error": MetadataValue.text(str(e)),
            },
        )
    except Exception as e:
        raise Failure(
            description=f"Error rendering essay template '{template_name}'",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "essay_template": MetadataValue.text(template_name),
                "jinja_message": MetadataValue.text(str(e)),
            },
        )
    context.add_output_metadata(
        {
            "function": MetadataValue.text("essay_prompt"),
            "gen_id": MetadataValue.text(str(gen_id)),
            "essay_template": MetadataValue.text(template_name),
            "draft_line_count": MetadataValue.int(len(draft_lines)),
            "phase1_source": MetadataValue.text(used_source),
        }
    )
    return prompt


@asset(
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
)
def essay_prompt(context) -> str:
    return _essay_prompt_impl(context)


def _essay_response_impl(context, essay_prompt) -> str:
    """Generate Phase‑2 essay responses.

    Always resolves and loads the Phase‑1 (draft) text via parent_gen_id first,
    regardless of mode, and then applies the selected generation path.
    """
    gen_id = context.partition_key
    row, _cohort = require_membership_row(context, "essay", str(gen_id), require_columns=["template_id", "parent_gen_id"])
    template_name = str(row.get("template_id") or row.get("essay_template") or "")
    parent_gen_id = row.get("parent_gen_id")
    # Allow explicit COPY_MODE hint from caller/tests to bypass template lookup
    if isinstance(essay_prompt, str) and essay_prompt.strip().upper().startswith("COPY_MODE"):
        mode = "copy"
    else:
        mode = resolve_essay_generator_mode(Path(context.resources.data_root), template_name)

    # Load Phase‑1 draft text; prefer index when available, otherwise fall back to IO/filesystem
    if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
        raise Failure(
            description="Missing parent_gen_id for essay doc",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure essay row exists in cohort membership with a valid parent_gen_id"),
            },
        )
    draft_text = load_generation_parsed_text(context, "draft", str(parent_gen_id), failure_fn_name="_essay_response_impl")
    used_source = "draft_gens_parent"
    # Ensure upstream draft has content even in copy mode
    # experiment_config is required by the caller asset
    min_lines = int(context.resources.experiment_config.min_draft_lines)
    dlines = [line.strip() for line in str(draft_text).split("\n") if line.strip()]
    if len(dlines) < max(1, min_lines):
        data_root = Path(getattr(context.resources, "data_root", "data"))
        draft_dir = data_root / "gens" / DRAFT / str(parent_gen_id)
        raise Failure(
            description="Upstream draft text is empty/too short for essay generation",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "draft_line_count": MetadataValue.int(len(dlines)),
                "min_required_lines": MetadataValue.int(min_lines),
                "draft_gen_dir": MetadataValue.path(str(draft_dir)),
            },
        )

    values = {"draft_block": draft_text, "links_block": draft_text}
    data_root = Path(getattr(context.resources, "data_root", "data"))
    if mode == "copy":
        result = execute_essay_copy(
            out_dir=data_root / "gens",
            gen_id=str(gen_id),
            template_id=template_name,
            parent_gen_id=str(parent_gen_id),
            pass_through_from=(data_root / "gens" / DRAFT / str(parent_gen_id) / "parsed.txt"),
            metadata_extra={
                "function": "essay_response",
                "cohort_id": str(_cohort) if isinstance(_cohort, str) and _cohort else None,
                "run_id": get_run_id(context),
            },
        )
        emit_standard_output_metadata(
            context,
            function="essay_response",
            gen_id=str(gen_id),
            result=result,
            extras={"mode": "copy", "parent_gen_id": str(parent_gen_id)},
        )
        return result.parsed_text or draft_text
    # LLM path
    model_id = str(row.get("llm_model_id") or "").strip()
    if not model_id:
        raise Failure(
            description="Missing generation model for essay task",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure cohort membership includes an llm_model_id for this essay"),
            },
        )
    result = execute_essay_llm(
        llm=context.resources.openrouter_client,
        out_dir=data_root / "gens",
        gen_id=str(gen_id),
        template_id=template_name,
        prompt_text=str(essay_prompt) if isinstance(essay_prompt, str) else render_template("essay", template_name, values),
        model=model_id,
        max_tokens=getattr(context.resources.experiment_config, "essay_generation_max_tokens", None),
        parent_gen_id=str(parent_gen_id),
        metadata_extra={
            "function": "essay_response",
            "cohort_id": str(_cohort) if isinstance(_cohort, str) and _cohort else None,
            "run_id": get_run_id(context),
        },
    )
    emit_standard_output_metadata(
        context,
        function="essay_response",
        gen_id=str(gen_id),
        result=result,
    )
    return result.raw_text or ""


@asset(
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def essay_response(context, essay_prompt) -> str:
    return _essay_response_impl(context, essay_prompt)