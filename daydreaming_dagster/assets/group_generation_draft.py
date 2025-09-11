"""
Group: generation_draft

Asset definitions for the draft (Phase‑1) generation stage.
"""

from dagster import asset, Failure, MetadataValue
import pandas as pd
from pathlib import Path
from jinja2 import Environment, TemplateSyntaxError
import os
from .partitions import draft_gens_partitions
from ..utils.template_loader import load_generation_template
from ..utils.raw_readers import read_draft_templates
from ..utils.draft_parsers import get_draft_parser
from ..unified.stage_runner import StageRunner, StageRunSpec
from ..utils.membership_lookup import find_membership_row_by_gen
from ..utils.generation import Generation
from ..utils.metadata import build_generation_metadata
from ..constants import DRAFT, FILE_RAW

# Reuse a single Jinja environment
JINJA = Environment()


@asset(
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

    # Load draft template (phase 'draft')
    try:
        template_content = load_generation_template(template_name, "draft")
    except FileNotFoundError as e:
        raise Failure(
            description=f"Draft template '{template_name}' not found",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("draft"),
                "error": MetadataValue.text(str(e)),
                "resolution": MetadataValue.text(
                    "Ensure the template exists in data/1_raw/templates/draft/"
                ),
            },
        )

    # Compile template with explicit error reporting
    try:
        template = JINJA.from_string(template_content)
    except TemplateSyntaxError as e:
        # Reconstruct expected template path for better diagnostics
        templates_root = Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/templates"))
        template_path = templates_root / "draft" / f"{template_name}.txt"
        preview = template_content[:300]
        raise Failure(
            description=f"Jinja template syntax error in draft template '{template_name}'",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("draft"),
                "template_path": MetadataValue.path(str(template_path)),
                "jinja_message": MetadataValue.text(str(e)),
                "error_line": MetadataValue.int(getattr(e, "lineno", 0) or 0),
                "template_preview": MetadataValue.text(preview),
            },
        ) from e

    prompt = template.render(concepts=content_combination.contents)

    context.log.info(f"Generated draft prompt for gen {gen_id} using template {template_name}")
    return prompt


def _draft_response_impl(context, draft_prompt, **_kwargs) -> str:
    """Generate Phase 1 LLM responses for drafts (core implementation)."""
    gen_id = context.partition_key
    # Resolve model id from cohort membership
    data_root = Path(getattr(context.resources, "data_root", "data"))
    row, cohort = find_membership_row_by_gen(data_root, "draft", str(gen_id))
    if row is None:
        raise Failure(
            description="Cohort membership row not found for draft gen_id",
            metadata={
                "function": MetadataValue.text("draft_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure cohort membership includes this draft gen_id"),
            },
        )
    # Use model_id from membership; LLM client maps id -> provider internally
    model_id = str(row.get("llm_model_id") or "").strip()
    if not model_id:
        raise Failure(
            description="Missing generation model for draft task",
            metadata={
                "function": MetadataValue.text("draft_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure cohort membership contains llm_model_id for this draft"),
            },
        )

    # Run via unified runner using pre-rendered prompt
    data_root = Path(getattr(context.resources, "data_root", "data"))
    runner = StageRunner()
    spec = StageRunSpec(
        stage="draft",
        gen_id=str(gen_id),
        template_id=str(row.get("template_id") or row.get("draft_template") or ""),
        values={},  # prompt provided below
        out_dir=data_root / "gens",
        mode="llm",
        model=model_id,
        parser_name=None,  # set below if CSV defines it
        max_tokens=context.resources.experiment_config.draft_generation_max_tokens,
        prompt_text=str(draft_prompt) if isinstance(draft_prompt, str) else "",
    )

    # Parse RAW response according to draft template's parser (identity if unspecified)
    draft_template = None
    try:
        draft_template = str(row.get("template_id") or row.get("draft_template") or "").strip()
    except Exception:
        draft_template = None
    parser_name = None
    try:
        df = read_draft_templates(Path(data_root), filter_active=False)
        if not df.empty and "parser" in df.columns and isinstance(draft_template, str):
            row = df[df["template_id"] == draft_template]
            if not row.empty:
                val = row.iloc[0].get("parser")
                if val is not None and not pd.isna(val):
                    s = val.strip() if isinstance(val, str) else str(val).strip()
                    if s:
                        parser_name = s
    except Exception:
        parser_name = None
    # Set parser into spec to let runner try parsing
    spec.parser_name = parser_name
    result = runner.run(spec, llm_client=context.resources.openrouter_client)
    raw_text = result.get("raw") or ""
    parsed_text = result.get("parsed") or raw_text
    # Validate min lines after writing RAW
    response_lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
    min_lines = int(getattr(context.resources.experiment_config, "min_draft_lines", 3))
    if len(response_lines) < min_lines:
        raise Failure(
            description=f"Draft response insufficient for gen {gen_id}",
            metadata={
                "function": MetadataValue.text("draft_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "model_id": MetadataValue.text(model_id),
                "response_line_count": MetadataValue.int(len(response_lines)),
                "minimum_required": MetadataValue.int(min_lines),
                "response_content_preview": MetadataValue.text(
                    raw_text[:200] + "..." if len(raw_text) > 200 else raw_text
                ),
            },
        )
    # If parser requested but failed to produce output, raise
    if parser_name and (result.get("parsed") is None):
        raise Failure(
            description="Parser returned empty/invalid text",
            metadata={
                "function": MetadataValue.text("draft_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "draft_template": MetadataValue.text(str(draft_template)),
                "parser": MetadataValue.text(str(parser_name)),
            },
        )

    # Final metadata and output
    context.log.info(
        f"Generated draft response for gen {gen_id} ({len(response_lines)} raw lines); parser={parser_name or 'identity'}"
    )
    meta = {
        "function": MetadataValue.text("draft_response"),
        "raw_line_count": MetadataValue.int(len(response_lines)),
        # Intentionally omit provider model from reports to simplify
        "max_tokens": MetadataValue.int(int(max_tokens) if isinstance(max_tokens, (int, float)) else 0),
        "parser": MetadataValue.text(parser_name or "identity"),
        "parsed_chars": MetadataValue.int(len(parsed_text)),
    }
    if raw_path_str:
        meta.update(
            {
                "raw_chars": MetadataValue.int(len(normalized)),
                "raw_path": MetadataValue.path(raw_path_str),
            }
        )
    context.add_output_metadata(meta)
    return parsed_text


@asset(
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
