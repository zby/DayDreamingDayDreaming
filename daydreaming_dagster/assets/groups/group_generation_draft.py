"""
Group: generation_draft

Asset definitions for the draft (Phase‑1) generation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from jinja2 import Environment, TemplateSyntaxError
import os
from ..partitions import draft_tasks_partitions
from ...utils.template_loader import load_generation_template
from ...utils.dataframe_helpers import get_task_row

# Reuse a single Jinja environment
JINJA = Environment()


@asset(
    partitions_def=draft_tasks_partitions,
    group_name="generation_draft",
    io_manager_key="draft_prompt_io_manager",
)
def draft_prompt(
    context,
    draft_generation_tasks,
    content_combinations,
) -> str:
    """Generate Phase 1 prompts for draft generation."""
    task_id = context.partition_key

    task_row = get_task_row(
        draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks"
    )
    combo_id = task_row["combo_id"]
    template_name = task_row["draft_template"]

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
                    "Ensure the template exists in data/1_raw/generation_templates/draft/"
                ),
            },
        )

    # Compile template with explicit error reporting
    try:
        template = JINJA.from_string(template_content)
    except TemplateSyntaxError as e:
        # Reconstruct expected template path for better diagnostics
        templates_root = Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/generation_templates"))
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

    context.log.info(f"Generated draft prompt for task {task_id} using template {template_name}")
    return prompt


@asset(
    partitions_def=draft_tasks_partitions,
    group_name="generation_draft",
    io_manager_key="draft_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config"},
)
def draft_response(context, draft_prompt, draft_generation_tasks) -> str:
    """Generate Phase 1 LLM responses for drafts."""
    task_id = context.partition_key
    task_row = get_task_row(draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks")
    model_name = task_row["generation_model_name"]

    llm_client = context.resources.openrouter_client
    experiment_config = context.resources.experiment_config
    max_tokens = getattr(experiment_config, "draft_generation_max_tokens", None) or 20480
    response = llm_client.generate(draft_prompt, model=model_name, max_tokens=max_tokens)

    # Normalize newlines and validate minimum lines
    normalized = str(response).replace("\r\n", "\n")
    response_lines = [line.strip() for line in normalized.split("\n") if line.strip()]
    min_lines = getattr(experiment_config, "min_draft_lines", 3)
    if len(response_lines) < int(min_lines):
        raise Failure(
            description=f"Draft response insufficient for task {task_id}",
            metadata={
                "function": MetadataValue.text("draft_response"),
                "task_id": MetadataValue.text(task_id),
                "model_name": MetadataValue.text(model_name),
                "response_line_count": MetadataValue.int(len(response_lines)),
                "minimum_required": MetadataValue.int(int(min_lines)),
                "response_content_preview": MetadataValue.text(
                    normalized[:200] + "..." if len(normalized) > 200 else normalized
                ),
            },
        )

    context.log.info(
        f"Generated draft response for task {task_id} using model {model_name} ({len(response_lines)} lines)"
    )
    context.add_output_metadata(
        {
            "function": MetadataValue.text("draft_response"),
            "response_line_count": MetadataValue.int(len(response_lines)),
            "model_used": MetadataValue.text(model_name),
            "max_tokens": MetadataValue.int(int(max_tokens) if isinstance(max_tokens, (int, float)) else 0),
        }
    )
    return normalized
