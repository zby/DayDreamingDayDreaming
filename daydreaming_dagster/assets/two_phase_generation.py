from dagster import asset, Failure, MetadataValue
from pathlib import Path
import pandas as pd
from jinja2 import Environment
from .partitions import link_tasks_partitions, essay_tasks_partitions
from ..utils.template_loader import load_generation_template
from ..utils.raw_readers import read_essay_templates, read_link_templates
from ..utils.link_parsers import get_link_parser
from ..utils.dataframe_helpers import get_task_row
from ..utils.shared_context import MockLoadContext


@asset(
    partitions_def=link_tasks_partitions,
    group_name="generation_links",
    io_manager_key="links_prompt_io_manager",
)
def links_prompt(
    context,
    link_generation_tasks,
    content_combinations,
) -> str:
    """
    Generate Phase 1 prompts for concept link generation.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = get_task_row(link_generation_tasks, "link_task_id", task_id, context, "link_generation_tasks")
    combo_id = task_row["combo_id"]
    template_name = task_row["link_template"]
    
    # Find the ContentCombination for this combo_id
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
            }
        )
    
    # Load the links phase template
    try:
        template_content = load_generation_template(template_name, "links")
    except FileNotFoundError as e:
        context.log.error(f"Links template '{template_name}' not found")
        raise Failure(
            description=f"Links template '{template_name}' not found",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("links"),
                "error": MetadataValue.text(str(e)),
                "resolution": MetadataValue.text("Ensure the template exists in data/1_raw/generation_templates/links/")
            }
        )
    
    # Render template using ContentCombination.contents
    env = Environment()
    template = env.from_string(template_content)
    prompt = template.render(concepts=content_combination.contents)
    
    context.log.info(f"Generated links prompt for task {task_id} using template {template_name}")
    return prompt


@asset(
    partitions_def=link_tasks_partitions,
    group_name="generation_links",
    io_manager_key="links_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config"},
)
def links_response(context, links_prompt, link_generation_tasks) -> str:
    """
    Generate Phase 1 LLM responses for concept links.
    Validates that the response contains at least 3 lines of content.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = get_task_row(link_generation_tasks, "link_task_id", task_id, context, "link_generation_tasks")
    
    # Use the model name directly from the task
    model_name = task_row["generation_model_name"]
    
    # Generate using Dagster LLM resource with link generation token limit
    llm_client = context.resources.openrouter_client
    experiment_config = context.resources.experiment_config
    response = llm_client.generate(
        links_prompt, 
        model=model_name,
        max_tokens=experiment_config.link_generation_max_tokens
    )
    
    # Validate response quality - fail immediately if insufficient
    response_lines = [line.strip() for line in response.split('\n') if line.strip()]
    if len(response_lines) < 3:
        raise Failure(
            description=f"Links response insufficient for task {task_id}",
            metadata={
                "task_id": MetadataValue.text(task_id),
                "model_name": MetadataValue.text(model_name),
                "response_line_count": MetadataValue.int(len(response_lines)),
                "minimum_required": MetadataValue.int(3),
                "response_content_preview": MetadataValue.text(response[:200] + "..." if len(response) > 200 else response),
                "resolution_1": MetadataValue.text("Check prompt quality or model parameters"),
                "resolution_2": MetadataValue.text("Consider retrying with different model or prompt"),
            }
        )
    
    context.log.info(f"Generated links response for task {task_id} using model {model_name} ({len(response_lines)} lines)")
    context.add_output_metadata({
        "response_line_count": MetadataValue.int(len(response_lines)),
        "model_used": MetadataValue.text(model_name),
    })
    
    return response


def _essay_prompt_impl(context, essay_generation_tasks) -> str:
    """
    Generate Phase 2 prompts for essay generation based on Phase 1 links.
    Links are guaranteed to be valid by the links_response asset.
    """
    task_id = context.partition_key
    
    # Get the specific task for this partition
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    template_name = task_row["essay_template"]

    # If this essay template is parser-driven, skip heavy prompt rendering
    data_root = context.resources.data_root
    essay_templates_df = read_essay_templates(Path(data_root), filter_active=False)
    generator_mode = None
    if not essay_templates_df.empty and "generator" in essay_templates_df.columns:
        row = essay_templates_df[essay_templates_df["template_id"] == template_name]
        if not row.empty:
            generator_mode = str(row.iloc[0].get("generator") or "llm")
    if generator_mode in ("parser", "copy"):
        context.log.info(
            f"Essay template '{template_name}' uses generator='{generator_mode}'; returning placeholder prompt."
        )
        return "PARSER_MODE: no prompt needed"

    # Load links_response using FK to link_task_id
    link_task_id = task_row["link_task_id"]
    links_io = context.resources.links_response_io_manager

    links_context = MockLoadContext(link_task_id)
    links_content = links_io.load_input(links_context)
    links_lines = [line.strip() for line in links_content.split('\n') if line.strip()]
    context.log.info(f"Using {len(links_lines)} validated links from links_response for task {task_id}")
    
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
    
    # Render essay template with links block
    env = Environment()
    template = env.from_string(template_content)
    prompt = template.render(links_block=links_content)
    
    context.log.info(f"Generated essay prompt for task {task_id} with {len(links_lines)} links")
    context.add_output_metadata({
        "links_line_count": MetadataValue.int(len(links_lines)),
        "fk_relationship": MetadataValue.text(f"essay_prompt:{task_id} -> links_response:{task_id}")
    })
    
    return prompt


@asset(
    partitions_def=essay_tasks_partitions,
    group_name="generation_essays",
    io_manager_key="essay_prompt_io_manager",
    required_resource_keys={"links_response_io_manager", "data_root"},
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
    data_root = context.resources.data_root
    essay_templates_df = read_essay_templates(Path(data_root), filter_active=False)
    generator_mode = None
    if not essay_templates_df.empty and "generator" in essay_templates_df.columns:
        row = essay_templates_df[essay_templates_df["template_id"] == template_name]
        if not row.empty:
            generator_mode = str(row.iloc[0].get("generator") or "llm")

    if generator_mode == "parser":
        # Parser mode: read links response, extract final idea, return it
        link_task_id = task_row["link_task_id"]
        link_template = task_row.get("link_template")

        links_io = context.resources.links_response_io_manager
        links_context = MockLoadContext(link_task_id)
        links_content = links_io.load_input(links_context)

        # Determine parser from link_templates.csv (no in-code mapping)
        data_root = context.resources.data_root
        link_templates_df = read_link_templates(Path(data_root), filter_active=False)
        parser_name = None
        if not link_templates_df.empty and "parser" in link_templates_df.columns:
            row = link_templates_df[link_templates_df["template_id"] == link_template]
            if not row.empty:
                parser_name = str(row.iloc[0].get("parser") or "").strip() or None

        if not parser_name:
            raise Failure(
                description=(
                    "Parser not specified for link template in CSV (required in parser mode)"
                ),
                metadata={
                    "link_task_id": MetadataValue.text(link_task_id),
                    "link_template": MetadataValue.text(str(link_template)),
                    "csv_column": MetadataValue.text("parser"),
                    "resolution": MetadataValue.text(
                        "Add a 'parser' value to data/1_raw/link_templates.csv for this template, or switch essay template generator mode."
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
                    "link_task_id": MetadataValue.text(link_task_id),
                    "link_template": MetadataValue.text(str(link_template)),
                    "parser": MetadataValue.text(str(parser_name)),
                    "resolution": MetadataValue.text(
                        "Use a supported parser name in CSV or register a new parser in utils/link_parsers.py"
                    ),
                },
            )

        try:
            parsed_text = parser_fn(links_content)
            parser_fallback = None
        except ValueError:
            # Graceful fallback for tests or non-compliant outputs
            parsed_text = links_content.strip()
            parser_fallback = "no_essay_idea_tags"

        context.log.info(
            f"Parsed essay from links for task {task_id} (parser={parser_name}, link_template={link_template})"
        )
        context.add_output_metadata(
            {
                "mode": MetadataValue.text("parser"),
                "parser": MetadataValue.text(parser_name or "unknown"),
                "parser_fallback": MetadataValue.text(parser_fallback or ""),
                "source_link_task_id": MetadataValue.text(link_task_id),
                "chars": MetadataValue.int(len(parsed_text)),
                "lines": MetadataValue.int(sum(1 for _ in parsed_text.splitlines())),
            }
        )
        return parsed_text

    if generator_mode == "copy":
        # Copy mode: read links response and return verbatim
        link_task_id = task_row["link_task_id"]
        links_io = context.resources.links_response_io_manager
        links_context = MockLoadContext(link_task_id)
        links_content = links_io.load_input(links_context)

        context.log.info(
            f"Copied essay from links for task {task_id} (mode=copy)"
        )
        context.add_output_metadata(
            {
                "mode": MetadataValue.text("copy"),
                "source_link_task_id": MetadataValue.text(link_task_id),
                "chars": MetadataValue.int(len(links_content)),
                "lines": MetadataValue.int(sum(1 for _ in links_content.splitlines())),
            }
        )
        return links_content

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
    required_resource_keys={"openrouter_client", "links_response_io_manager", "data_root"},
)
def essay_response(context, essay_prompt, essay_generation_tasks) -> str:
    return _essay_response_impl(context, essay_prompt, essay_generation_tasks)
