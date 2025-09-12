"""
Group: generation_essays

Asset definitions for the essay (Phase‑2) generation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import essay_gens_partitions
from ..utils.raw_readers import read_essay_templates
from ..utils.membership_lookup import find_membership_row_by_gen
from ..utils.generation import Generation
from ..unified.stage_runner import StageRunner, StageRunSpec
from ..unified.stage_services import render_template, execute_essay_copy, execute_essay_llm
from ._helpers import (
    load_generation_parsed_text,
    resolve_essay_generator_mode,
    emit_standard_output_metadata,
    get_run_id,
)
from ..constants import DRAFT, ESSAY


def _get_essay_generator_mode(data_root: str | Path, template_id: str) -> str:
    """Return normalized generator mode for an essay template.

    Allowed values: 'llm' or 'copy'. Any other value (including missing) raises Failure.
    """
    df = read_essay_templates(Path(data_root), filter_active=False)
    if df.empty:
        raise Failure(
            description="Essay templates table is empty; cannot resolve generator mode",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "data_root": MetadataValue.path(str(data_root)),
                "resolution": MetadataValue.text("Ensure data/1_raw/essay_templates.csv contains active templates with a 'generator' column"),
            },
        )
    if "generator" not in df.columns:
        raise Failure(
            description="Essay templates CSV missing required 'generator' column",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "data_root": MetadataValue.path(str(data_root)),
                "resolution": MetadataValue.text("Add a 'generator' column with values 'llm' or 'copy'"),
            },
        )
    row = df[df["template_id"] == template_id]
    if row.empty:
        raise Failure(
            description=f"Essay template not found: {template_id}",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text("Add the template row to essay_templates.csv or correct the essay_template id in tasks"),
            },
        )
    val = row.iloc[0].get("generator")
    if not isinstance(val, str) or not val.strip():
        raise Failure(
            description="Essay template has empty/invalid generator value",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text("Set generator to 'llm' or 'copy'"),
            },
        )
    mode = val.strip().lower()
    if mode not in ("llm", "copy"):
        raise Failure(
            description=f"Essay template declares unsupported generator '{mode}'",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text("Set generator to 'llm' or 'copy' in data/1_raw/essay_templates.csv"),
            },
        )
    return mode


    


def _load_phase1_text_by_parent_doc(context, parent_gen_id: str) -> tuple[str, str]:
    """Load Phase‑1 text by parent_gen_id directly from the filesystem.

    Returns (normalized_text, source_label).
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    gens_root = data_root / "gens"
    base = gens_root / DRAFT / str(parent_gen_id)
    try:
        gen = Generation.load(gens_root, DRAFT, str(parent_gen_id))
    except Exception as e:
        raise Failure(
            description="Parent draft document not found",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_parent_doc"),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "error": MetadataValue.text(str(e)),
            },
        )
    if not isinstance(gen.parsed_text, str) or not gen.parsed_text:
        raise Failure(
            description="Missing or unreadable parsed.txt for parent draft document",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_parent_doc"),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "draft_gen_dir": MetadataValue.path(str(base)),
            },
        )
    return str(gen.parsed_text).replace("\r\n", "\n"), "draft_gens_parent"


# Note: combo/model-based resolution removed — parent_gen_id is required for essays.


def _essay_prompt_impl(context) -> str:
    """Generate Phase‑2 prompts based on Phase‑1 drafts."""
    gen_id = context.partition_key
    mrow, _cohort = find_membership_row_by_gen(getattr(context.resources, "data_root", "data"), "essay", str(gen_id))
    template_name = str(mrow.get("template_id") or mrow.get("essay_template") or "") if mrow is not None else ""
    parent_gen_id = mrow.get("parent_gen_id") if mrow is not None else None

    generator_mode = _get_essay_generator_mode(context.resources.data_root, template_name)
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
    draft_text, used_source = (load_generation_parsed_text(context, "draft", str(parent_gen_id), failure_fn_name="essay_prompt"), "draft_gens_parent")
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
    mrow, _cohort = find_membership_row_by_gen(getattr(context.resources, "data_root", "data"), "essay", str(gen_id))
    if mrow is not None:
        template_name = str(mrow.get("template_id") or mrow.get("essay_template") or "")
        parent_gen_id = mrow.get("parent_gen_id")
    else:
        template_name = ""
        parent_gen_id = None
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
    draft_text, used_source = _load_phase1_text_by_parent_doc(context, str(parent_gen_id))
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
    model_id = str(mrow.get("llm_model_id") or "").strip() if mrow is not None else ""
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
