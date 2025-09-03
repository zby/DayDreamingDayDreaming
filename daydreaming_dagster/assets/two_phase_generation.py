from dagster import asset, Failure, MetadataValue
from pathlib import Path
from jinja2 import Environment
from .partitions import essay_tasks_partitions, draft_tasks_partitions
from ..utils.template_loader import load_generation_template
from ..utils.raw_readers import read_essay_templates, read_draft_templates
from ..utils.link_parsers import get_link_parser
from ..utils.dataframe_helpers import get_task_row
from ..utils.shared_context import MockLoadContext


# Reuse a single Jinja environment
JINJA = Environment()

def _get_essay_generator_mode(data_root: str | Path, template_id: str) -> str | None:
    """Lookup essay template generator mode (llm/parser/copy)."""
    df = read_essay_templates(Path(data_root), filter_active=False)
    if df.empty or "generator" not in df.columns:
        return None
    row = df[df["template_id"] == template_id]
    if row.empty:
        return None
    val = row.iloc[0].get("generator")
    return str(val) if val else None

def _load_phase1_text(context, draft_task_id: str) -> tuple[str, str]:
    """Load Phase‑1 (draft) text with legacy fallbacks.

    Order:
      1) draft_response_io_manager
      2) data/3_generation/draft_responses/{id}.txt
      3) data/3_generation/links_responses/{id}.txt (legacy)
    Returns (text, source_label).
    """
    # 1) IO manager if available
    draft_io = getattr(context.resources, "draft_response_io_manager", None)
    if draft_io is not None:
        try:
            return draft_io.load_input(MockLoadContext(draft_task_id)), "draft_response"
        except Exception:
            pass

    # 2) Direct file under draft_responses
    draft_fp = Path(context.resources.data_root) / "3_generation" / "draft_responses" / f"{draft_task_id}.txt"
    if draft_fp.exists():
        return draft_fp.read_text(encoding="utf-8"), "draft_file"

    # 3) Legacy file under links_responses
    legacy_fp = Path(context.resources.data_root) / "3_generation" / "links_responses" / f"{draft_task_id}.txt"
    if legacy_fp.exists():
        return legacy_fp.read_text(encoding="utf-8"), "links_file"

    raise Failure(
        description="Draft response not found",
        metadata={
            "draft_task_id": MetadataValue.text(draft_task_id),
            "expected_draft_path": MetadataValue.path(draft_fp),
            "legacy_links_path": MetadataValue.path(legacy_fp),
            "resolution": MetadataValue.text(
                "Ensure Phase 1 draft exists under draft_responses or copy legacy file from links_responses"
            ),
        },
    )



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
    """
    Generate Phase 1 prompts for draft generation (renamed from links).
    """
    task_id = context.partition_key

    task_row = get_task_row(draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks")
    combo_id = task_row["combo_id"]
    template_name = task_row["draft_template"]

    # Resolve content combination
    content_combination = None
    for combo in content_combinations:
        if combo.combo_id == combo_id:
            content_combination = combo
            break
    if content_combination is None:
        available_combos = [combo.combo_id for combo in content_combinations[:5]]
        context.log.error(f"No ContentCombination found for combo_id: {combo_id}")
        raise Failure(
            description=f"Content combination '{combo_id}' not found in combinations database",
            metadata={
                "combo_id": MetadataValue.text(combo_id),
                "available_combinations_sample": MetadataValue.text(str(available_combos)),
                "total_combinations": MetadataValue.int(len(content_combinations)),
            },
        )

    # Load draft template (prefers draft/, falls back to links/ for transition)
    try:
        template_content = load_generation_template(template_name, "draft")
    except FileNotFoundError as e:
        context.log.error(f"Draft template '{template_name}' not found")
        raise Failure(
            description=f"Draft template '{template_name}' not found",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("draft"),
                "error": MetadataValue.text(str(e)),
                "resolution": MetadataValue.text(
                    "Ensure the template exists in data/1_raw/generation_templates/draft/ or links/"
                ),
            },
        )

    env = Environment()
    template = env.from_string(template_content)
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
    """
    Generate Phase 1 LLM responses for drafts.
    """
    task_id = context.partition_key
    task_row = get_task_row(draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks")
    model_name = task_row["generation_model_name"]

    llm_client = context.resources.openrouter_client
    experiment_config = context.resources.experiment_config
    response = llm_client.generate(draft_prompt, model=model_name, max_tokens=experiment_config.link_generation_max_tokens)

    response_lines = [line.strip() for line in response.split("\n") if line.strip()]
    if len(response_lines) < 3:
        raise Failure(
            description=f"Draft response insufficient for task {task_id}",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "model_name": MetadataValue.text(model_name),
                "response_line_count": MetadataValue.int(len(response_lines)),
                "minimum_required": MetadataValue.int(3),
                "response_content_preview": MetadataValue.text(response[:200] + "..." if len(response) > 200 else response),
                "resolution_1": MetadataValue.text("Check prompt quality or model parameters"),
                "resolution_2": MetadataValue.text("Consider retrying with different model or prompt"),
            },
        )

    context.log.info(
        f"Generated draft response for task {task_id} using model {model_name} ({len(response_lines)} lines)"
    )
    context.add_output_metadata(
        {"response_line_count": MetadataValue.int(len(response_lines)), "model_used": MetadataValue.text(model_name)}
    )
    return response


def _essay_prompt_impl(context, essay_generation_tasks) -> str:
    """
    Generate Phase 2 prompts for essay generation based on Phase 1 drafts.
    The Phase‑1 content is loaded from draft_responses with a legacy fallback to links_responses.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    template_name = task_row["essay_template"]

    # If this essay template is parser-driven, skip heavy prompt rendering
    generator_mode = _get_essay_generator_mode(context.resources.data_root, template_name)
    if generator_mode in ("parser", "copy"):
        context.log.info(
            f"Essay template '{template_name}' uses generator='{generator_mode}'; returning placeholder prompt."
        )
        return "PARSER_MODE: no prompt needed"

    # Load Phase‑1 text (draft) using FK to draft_task_id (with legacy fallback)
    draft_task_id = task_row.get("draft_task_id") or task_row.get("link_task_id")
    draft_text, used_source = _load_phase1_text(context, draft_task_id)
    draft_lines = [line.strip() for line in draft_text.split('\n') if line.strip()]
    context.log.info(f"Using {len(draft_lines)} draft lines from {used_source} for task {task_id}")
    
    # Load the essay phase template
    try:
        template_content = load_generation_template(template_name, "essay")
    except FileNotFoundError as e:
        context.log.error(f"Essay template '{template_name}' not found")
        raise Failure(
            description=f"Essay template '{template_name}' not found",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("essay"),
                "error": MetadataValue.text(str(e)),
                "resolution": MetadataValue.text("Ensure the template exists in data/1_raw/generation_templates/essay/")
            }
        )
    
    # Render essay template with draft block (passed as links_block for backward compatibility)
    template = JINJA.from_string(template_content)
    prompt = template.render(links_block=draft_text)
    
    context.log.info(f"Generated essay prompt for task {task_id} with {len(draft_lines)} draft lines")
    context.add_output_metadata({
        "draft_line_count": MetadataValue.int(len(draft_lines)),
        "fk_relationship": MetadataValue.text(f"essay_prompt:{task_id} -> {used_source}:{draft_task_id}")
    })
    
    return prompt


@asset(
    partitions_def=essay_tasks_partitions,
    group_name="generation_essays",
    io_manager_key="essay_prompt_io_manager",
    required_resource_keys={"data_root"},
)
def essay_prompt(
    context,
    essay_generation_tasks,
) -> str:
    return _essay_prompt_impl(context, essay_generation_tasks)


def _essay_response_impl(context, essay_prompt, essay_generation_tasks) -> str:
    """
    Generate Phase 2 LLM responses for essays.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    
    template_name = task_row["essay_template"]
    generator_mode = _get_essay_generator_mode(context.resources.data_root, template_name)

    if generator_mode == "parser":
        # Parser mode: read draft response, extract final idea, return it
        draft_task_id = task_row.get("draft_task_id") or task_row.get("link_task_id")
        # Prefer draft_template; fallback to legacy link_template
        draft_template = task_row.get("draft_template") or task_row.get("link_template")

        draft_text, _ = _load_phase1_text(context, draft_task_id)

        # Determine parser from draft_templates.csv (no in-code mapping)
        data_root = context.resources.data_root
        link_templates_df = read_draft_templates(Path(data_root), filter_active=False)
        parser_name = None
        if not link_templates_df.empty and "parser" in link_templates_df.columns:
            row = link_templates_df[link_templates_df["template_id"] == draft_template]
            if not row.empty:
                parser_name = str(row.iloc[0].get("parser") or "").strip() or None

        if not parser_name:
            raise Failure(
                description=(
                    "Parser not specified for draft template in CSV (required in parser mode)"
                ),
                metadata={
                    "draft_task_id": MetadataValue.text(draft_task_id),
                    "draft_template": MetadataValue.text(str(draft_template)),
                    "csv_column": MetadataValue.text("parser"),
                    "resolution": MetadataValue.text(
                        "Add a 'parser' value to data/1_raw/draft_templates.csv for this template, or switch essay template generator mode."
                    ),
                },
            )

        parser_fn = get_link_parser(parser_name)
        if parser_fn is None:
            raise Failure(
                description=(
                    f"Parser '{parser_name}' not found in registry (CSV refers to unknown parser)"
                ),
                metadata={
                    "draft_task_id": MetadataValue.text(draft_task_id),
                    "draft_template": MetadataValue.text(str(draft_template)),
                    "parser": MetadataValue.text(str(parser_name)),
                    "resolution": MetadataValue.text(
                        "Use a supported parser name in CSV or register a new parser in utils/link_parsers.py"
                    ),
                },
            )

        try:
            parsed_text = parser_fn(draft_text)
            parser_fallback = None
        except ValueError:
            # Graceful fallback for tests or non-compliant outputs
            parsed_text = draft_text.strip()
            parser_fallback = "no_essay_idea_tags"

        context.log.info(
            f"Parsed essay from draft for task {task_id} (parser={parser_name}, draft_template={draft_template})"
        )
        context.add_output_metadata(
            {
                "mode": MetadataValue.text("parser"),
                "parser": MetadataValue.text(parser_name or "unknown"),
                "parser_fallback": MetadataValue.text(parser_fallback or ""),
                "source_link_task_id": MetadataValue.text(draft_task_id),
                "chars": MetadataValue.int(len(parsed_text)),
                "lines": MetadataValue.int(sum(1 for _ in parsed_text.splitlines())),
            }
        )
        return parsed_text

    if generator_mode == "copy":
        # Copy mode: read draft response and return verbatim
        draft_task_id = task_row.get("draft_task_id") or task_row.get("link_task_id")
        draft_text, _ = _load_phase1_text(context, draft_task_id)

        context.log.info(
            f"Copied essay from draft for task {task_id} (mode=copy)"
        )
        context.add_output_metadata(
            {
                "mode": MetadataValue.text("copy"),
                "source_link_task_id": MetadataValue.text(draft_task_id),
                "chars": MetadataValue.int(len(draft_text)),
                "lines": MetadataValue.int(sum(1 for _ in draft_text.splitlines())),
            }
        )
        return draft_text

    # Default: LLM generation path
    model_name = task_row["generation_model_name"]
    llm_client = context.resources.openrouter_client
    response = llm_client.generate(essay_prompt, model=model_name)
    context.log.info(f"Generated essay response for task {task_id} using model {model_name}")
    return response


@asset(
    partitions_def=essay_tasks_partitions,
    group_name="generation_essays",
    io_manager_key="essay_response_io_manager",
    required_resource_keys={"openrouter_client", "data_root"},
)
def essay_response(context, essay_prompt, essay_generation_tasks) -> str:
    return _essay_response_impl(context, essay_prompt, essay_generation_tasks)
