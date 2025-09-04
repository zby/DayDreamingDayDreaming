"""
Group: generation_essays

Asset definitions for the essay (Phase‑2) generation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from jinja2 import Environment
from ..partitions import essay_tasks_partitions
from ...utils.template_loader import load_generation_template
from ...utils.raw_readers import read_essay_templates, read_draft_templates
from ...utils.link_parsers import get_draft_parser
from ...utils.dataframe_helpers import get_task_row
from ...utils.shared_context import MockLoadContext

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
    """Load Phase‑1 (draft) text with versioned/unversioned lookup.

    Returns (normalized_text, source_label).
    """
    draft_io = getattr(context.resources, "draft_response_io_manager", None)
    if draft_io is not None:
        try:
            text = draft_io.load_input(MockLoadContext(draft_task_id))
            return str(text).replace("\r\n", "\n"), "draft_response"
        except Exception:
            pass

    draft_dir = Path(context.resources.data_root) / "3_generation" / "draft_responses"
    try:
        import os, re
        _V_RE = re.compile(rf"^{re.escape(draft_task_id)}_v(\d+)\.txt$")
        best_ver = -1
        best_name = None
        for name in os.listdir(draft_dir):
            m = _V_RE.match(name)
            if not m:
                continue
            try:
                v = int(m.group(1))
            except Exception:
                continue
            if v > best_ver:
                best_ver = v
                best_name = name
        if best_name:
            draft_fp = draft_dir / best_name
            if draft_fp.exists():
                return draft_fp.read_text(encoding="utf-8").replace("\r\n", "\n"), "draft_file_v"
    except Exception:
        pass
    draft_fp_legacy = draft_dir / f"{draft_task_id}.txt"
    if draft_fp_legacy.exists():
        return draft_fp_legacy.read_text(encoding="utf-8").replace("\r\n", "\n"), "draft_file"

    raise Failure(
        description="Draft response not found",
        metadata={
            "draft_task_id": MetadataValue.text(draft_task_id),
            "function": MetadataValue.text("_load_phase1_text"),
            "expected_dir": MetadataValue.path(str(draft_dir)),
            "resolution": MetadataValue.text(
                "Materialize 'draft_response' or write a draft file under data/3_generation/draft_responses/"
            ),
        },
    )


def _essay_prompt_impl(context, essay_generation_tasks) -> str:
    """Generate Phase‑2 prompts based on Phase‑1 drafts."""
    task_id = context.partition_key
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    template_name = task_row["essay_template"]

    generator_mode = _get_essay_generator_mode(context.resources.data_root, template_name)
    if generator_mode in ("parser", "copy"):
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_prompt"),
                "mode": MetadataValue.text(generator_mode),
                "essay_template": MetadataValue.text(template_name),
            }
        )
        return f"{generator_mode.upper()}_MODE: no prompt needed"

    draft_task_id = task_row.get("draft_task_id")
    if not isinstance(draft_task_id, str) or not draft_task_id:
        raise Failure(
            description="Missing draft_task_id for essay task",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "essay_task_id": MetadataValue.text(task_id),
                "essay_template": MetadataValue.text(template_name),
            },
        )
    draft_text, used_source = _load_phase1_text(context, draft_task_id)
    draft_lines = [line.strip() for line in draft_text.split("\n") if line.strip()]

    try:
        template_content = load_generation_template(template_name, "essay")
    except FileNotFoundError as e:
        raise Failure(
            description=f"Essay template '{template_name}' not found",
            metadata={
                "template_name": MetadataValue.text(template_name),
                "phase": MetadataValue.text("essay"),
                "error": MetadataValue.text(str(e)),
            },
        )

    template = JINJA.from_string(template_content)
    prompt = template.render(draft_block=draft_text, links_block=draft_text)

    context.add_output_metadata({
        "function": MetadataValue.text("essay_prompt"),
        "draft_line_count": MetadataValue.int(len(draft_lines)),
        "fk_relationship": MetadataValue.text(f"essay_prompt:{task_id} -> {used_source}:{draft_task_id}"),
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
    """Generate Phase‑2 LLM responses (llm/parser/copy modes)."""
    task_id = context.partition_key
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    template_name = task_row["essay_template"]
    generator_mode = _get_essay_generator_mode(context.resources.data_root, template_name)

    if generator_mode == "parser":
        draft_task_id = task_row.get("draft_task_id")
        if not isinstance(draft_task_id, str) or not draft_task_id:
            raise Failure(
                description="Missing draft_task_id for essay task (parser mode)",
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "essay_task_id": MetadataValue.text(task_id),
                    "essay_template": MetadataValue.text(template_name),
                },
            )
        draft_template = task_row.get("draft_template")
        draft_text, _ = _load_phase1_text(context, draft_task_id)
        data_root = context.resources.data_root
        draft_templates_df = read_draft_templates(Path(data_root), filter_active=False)
        parser_name = None
        if not draft_templates_df.empty and "parser" in draft_templates_df.columns:
            row = draft_templates_df[draft_templates_df["template_id"] == draft_template]
            if not row.empty:
                parser_name = str(row.iloc[0].get("parser") or "").strip() or None
        if not parser_name:
            raise Failure(
                description=("Parser not specified for draft template in CSV (required in parser mode)"),
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "draft_task_id": MetadataValue.text(draft_task_id),
                    "draft_template": MetadataValue.text(str(draft_template)),
                    "csv_column": MetadataValue.text("parser"),
                },
            )
        parser_fn = get_draft_parser(parser_name)
        if parser_fn is None:
            raise Failure(
                description=(f"Parser '{parser_name}' not found in registry (CSV refers to unknown parser)"),
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "draft_task_id": MetadataValue.text(draft_task_id),
                    "draft_template": MetadataValue.text(str(draft_template)),
                    "parser": MetadataValue.text(str(parser_name)),
                },
            )
        parsed_text = parser_fn(draft_text)
        if not isinstance(parsed_text, str) or not parsed_text.strip():
            raise Failure(
                description="Parser returned empty/invalid text",
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "draft_task_id": MetadataValue.text(draft_task_id),
                    "draft_template": MetadataValue.text(str(draft_template)),
                    "parser": MetadataValue.text(str(parser_name)),
                },
            )
        context.add_output_metadata(
            {
                "mode": MetadataValue.text("parser"),
                "parser": MetadataValue.text(parser_name or "unknown"),
                "function": MetadataValue.text("essay_response"),
                "source_draft_task_id": MetadataValue.text(draft_task_id),
                "chars": MetadataValue.int(len(parsed_text)),
                "lines": MetadataValue.int(sum(1 for _ in parsed_text.splitlines())),
            }
        )
        return parsed_text

    if generator_mode == "copy":
        draft_task_id = task_row.get("draft_task_id")
        if not isinstance(draft_task_id, str) or not draft_task_id:
            raise Failure(
                description="Missing draft_task_id for essay task (copy mode)",
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "essay_task_id": MetadataValue.text(task_id),
                    "essay_template": MetadataValue.text(template_name),
                },
            )
        draft_text, _ = _load_phase1_text(context, draft_task_id)
        if not draft_text or not draft_text.strip():
            raise Failure(
                description="Draft text empty/whitespace in copy mode",
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "draft_task_id": MetadataValue.text(draft_task_id),
                    "essay_task_id": MetadataValue.text(task_id),
                },
            )
        context.add_output_metadata(
            {
                "mode": MetadataValue.text("copy"),
                "function": MetadataValue.text("essay_response"),
                "source_draft_task_id": MetadataValue.text(draft_task_id),
                "chars": MetadataValue.int(len(draft_text)),
                "lines": MetadataValue.int(sum(1 for _ in draft_text.splitlines())),
            }
        )
        return draft_text

    model_name = task_row["generation_model_name"]
    llm_client = context.resources.openrouter_client
    max_tokens = getattr(getattr(context.resources, "experiment_config", None), "essay_generation_max_tokens", None) or 4096
    response = llm_client.generate(essay_prompt, model=model_name, max_tokens=max_tokens)
    context.add_output_metadata(
        {
            "mode": MetadataValue.text("llm"),
            "function": MetadataValue.text("essay_response"),
            "model_used": MetadataValue.text(model_name),
            "max_tokens": MetadataValue.int(int(max_tokens) if isinstance(max_tokens, (int, float)) else 0),
        }
    )
    return response


@asset(
    partitions_def=essay_tasks_partitions,
    group_name="generation_essays",
    io_manager_key="essay_response_io_manager",
    required_resource_keys={"openrouter_client", "data_root"},
)
def essay_response(context, essay_prompt, essay_generation_tasks) -> str:
    return _essay_response_impl(context, essay_prompt, essay_generation_tasks)

