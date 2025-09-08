"""
Group: generation_essays

Asset definitions for the essay (Phase‑2) generation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from jinja2 import Environment
from .partitions import essay_tasks_partitions
from ..utils.template_loader import load_generation_template
from ..utils.raw_readers import read_essay_templates
from ..utils.dataframe_helpers import get_task_row
from ..utils.raw_write import save_versioned_raw_text
from ..utils.ids import (
    doc_dir as build_doc_dir,
)
from ..utils.filesystem_rows import (
    get_row_by_doc_id as fs_get_row_by_doc_id,
    read_parsed as fs_read_parsed,
    read_raw as fs_read_raw,
)
from ..utils.document import Document

# Reuse a single Jinja environment
JINJA = Environment()


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


    


def _load_phase1_text_by_parent_doc(context, parent_doc_id: str) -> tuple[str, str]:
    """Load Phase‑1 text by parent_doc_id directly from the filesystem.

    Returns (normalized_text, source_label).
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    docs_root = data_root / "docs"
    row = fs_get_row_by_doc_id(docs_root, "draft", str(parent_doc_id))
    if not row:
        raise Failure(
            description="Parent draft document not found",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_parent_doc"),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
            },
        )
    try:
        text = fs_read_parsed(row)
    except Exception:
        text = fs_read_raw(row)
    return str(text).replace("\r\n", "\n"), "draft_fs_parent"


def _load_phase1_text_by_draft_task(context, draft_task_id: str) -> tuple[str, str]:
    """Deprecated: task-based resolution removed — doc-id required."""
    raise Failure(
        description="draft_task_id-based resolution is disabled; provide parent_doc_id",
        metadata={
            "function": MetadataValue.text("_load_phase1_text_by_draft_task"),
            "draft_task_id": MetadataValue.text(str(draft_task_id)),
            "resolution": MetadataValue.text("Add parent_doc_id to essay_generation_tasks.csv for this essay_task_id"),
        },
    )


def _load_phase1_text_by_combo_model(context, combo_id: str, model_id: str) -> tuple[str, str]:
    raise Failure(
        description="combo/model-based resolution is not supported; provide parent_doc_id",
        metadata={
            "function": MetadataValue.text("_load_phase1_text_by_combo_model"),
            "combo_id": MetadataValue.text(str(combo_id)),
            "model_id": MetadataValue.text(str(model_id)),
            "resolution": MetadataValue.text("Use parent_doc_id (draft doc id) in essay_generation_tasks.csv"),
        },
    )


def _essay_prompt_impl(context, essay_generation_tasks) -> str:
    """Generate Phase‑2 prompts based on Phase‑1 drafts."""
    task_id = context.partition_key
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    template_name = task_row["essay_template"]

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

    # Resolve Phase‑1 text strictly via parent_doc_id (doc-id required)
    parent_doc_id = task_row.get("parent_doc_id")
    if not (isinstance(parent_doc_id, str) and parent_doc_id.strip()):
        raise Failure(
            description="Missing parent_doc_id for essay task",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "essay_task_id": MetadataValue.text(task_id),
                "resolution": MetadataValue.text("Provide parent_doc_id (draft doc id) in essay_generation_tasks.csv"),
            },
        )
    draft_text, used_source = _load_phase1_text_by_parent_doc(context, str(parent_doc_id))
    draft_lines = [line.strip() for line in draft_text.split("\n") if line.strip()]
    # Enforce non-empty upstream draft text to avoid empty prompts
    # experiment_config is required via asset definition
    min_lines = int(context.resources.experiment_config.min_draft_lines)
    if len(draft_lines) < max(1, min_lines):
        data_root = Path(getattr(context.resources, "data_root", "data"))
        draft_dir = data_root / "docs" / "draft" / str(parent_doc_id)
        raise Failure(
            description="Upstream draft text is empty/too short for essay prompt",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "essay_task_id": MetadataValue.text(task_id),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
                "draft_line_count": MetadataValue.int(len(draft_lines)),
                "min_required_lines": MetadataValue.int(min_lines),
                "draft_doc_dir": MetadataValue.path(str(draft_dir)),
            },
        )

    try:
        tmpl = load_generation_template(template_name, "essay")
    except FileNotFoundError as e:
        raise Failure(
            description=f"Essay template '{template_name}' not found",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "essay_task_id": MetadataValue.text(task_id),
                "essay_template": MetadataValue.text(template_name),
                "error": MetadataValue.text(str(e)),
            },
        )
    # Provide both preferred and legacy variables to templates
    # - draft_lines: list[str] of non-empty lines
    # - draft_block: entire upstream draft text as a block
    # - links_block: legacy alias used by older templates
    env = JINJA
    template = env.from_string(tmpl)
    draft_block = draft_text
    prompt = template.render(
        draft_lines=draft_lines,
        draft_block=draft_block,
        links_block=draft_block,
    )
    context.add_output_metadata(
        {
            "function": MetadataValue.text("essay_prompt"),
            "essay_task_id": MetadataValue.text(task_id),
            "essay_template": MetadataValue.text(template_name),
            "draft_line_count": MetadataValue.int(len(draft_lines)),
            "phase1_source": MetadataValue.text(used_source),
        }
    )
    return prompt


@asset(
    partitions_def=essay_tasks_partitions,
    group_name="generation_essays",
    io_manager_key="essay_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
)
def essay_prompt(context, essay_generation_tasks) -> str:
    return _essay_prompt_impl(context, essay_generation_tasks)


def _essay_response_impl(context, essay_prompt, essay_generation_tasks) -> str:
    """Generate Phase‑2 essay responses.

    Always resolves and loads the Phase‑1 (draft) text via parent_doc_id first,
    regardless of mode, and then applies the selected generation path.
    """
    task_id = context.partition_key
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    template_name = task_row["essay_template"]
    mode = _get_essay_generator_mode(context.resources.data_root, template_name)

    # Load Phase‑1 draft text; prefer index when available, otherwise fall back to IO/filesystem
    parent_doc_id = task_row.get("parent_doc_id")
    if not (isinstance(parent_doc_id, str) and parent_doc_id.strip()):
        raise Failure(
            description="Missing parent_doc_id for essay task",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "essay_task_id": MetadataValue.text(task_id),
                "resolution": MetadataValue.text("Provide parent_doc_id (draft doc id) in essay_generation_tasks.csv"),
            },
        )
    draft_text, used_source = _load_phase1_text_by_parent_doc(context, str(parent_doc_id))
    # Ensure upstream draft has content even in copy mode
    # experiment_config is required by the caller asset
    min_lines = int(context.resources.experiment_config.min_draft_lines)
    dlines = [line.strip() for line in str(draft_text).split("\n") if line.strip()]
    if len(dlines) < max(1, min_lines):
        data_root = Path(getattr(context.resources, "data_root", "data"))
        draft_dir = data_root / "docs" / "draft" / str(parent_doc_id)
        raise Failure(
            description="Upstream draft text is empty/too short for essay generation",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "essay_task_id": MetadataValue.text(task_id),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
                "draft_line_count": MetadataValue.int(len(dlines)),
                "min_required_lines": MetadataValue.int(min_lines),
                "draft_doc_dir": MetadataValue.path(str(draft_dir)),
            },
        )

    if mode == "copy":
        text = draft_text
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_response"),
                "mode": MetadataValue.text("copy"),
                "essay_task_id": MetadataValue.text(task_id),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
                "source": MetadataValue.text(used_source),
                "chars": MetadataValue.int(len(text)),
                "lines": MetadataValue.int(sum(1 for _ in text.splitlines())),
            }
        )
        return text
    if mode == "parser":
        raise Failure(
            description="Essay generator mode 'parser' is not supported",
            metadata={
                "function": MetadataValue.text("essay_response"),
                "essay_task_id": MetadataValue.text(task_id),
                "essay_template": MetadataValue.text(template_name),
                "resolution": MetadataValue.text("Set generator to 'llm' or 'copy' in data/1_raw/essay_templates.csv"),
            },
        )

    # Default LLM path (with RAW side-write + truncation guard)
    model_name = task_row["generation_model_name"]
    llm_client = context.resources.openrouter_client
    max_tokens = context.resources.experiment_config.essay_generation_max_tokens
    text, info = llm_client.generate_with_info(essay_prompt, model=model_name, max_tokens=max_tokens)

    normalized = str(text).replace("\r\n", "\n")

    # Side-write RAW essay response with versioning
    experiment_config = getattr(context.resources, "experiment_config", None)
    save_raw = bool(getattr(experiment_config, "save_raw_essay_enabled", True))
    raw_dir_override = getattr(experiment_config, "raw_essay_dir_override", None)
    data_root = Path(getattr(context.resources, "data_root", "data"))
    raw_dir = Path(raw_dir_override) if raw_dir_override else data_root / "3_generation" / "essay_responses_raw"
    raw_path_str = None
    if save_raw:
        raw_path_str = save_versioned_raw_text(raw_dir, task_id, normalized, logger=context.log)

    # Truncation detection: explicit flag or finish_reason=length
    finish_reason = (info or {}).get("finish_reason") if isinstance(info, dict) else None
    was_truncated = bool((info or {}).get("truncated") if isinstance(info, dict) else False)
    usage = (info or {}).get("usage") if isinstance(info, dict) else None
    completion_tokens = usage.get("completion_tokens") if isinstance(usage, dict) else None
    requested_max = usage.get("max_tokens") if isinstance(usage, dict) else None
    if was_truncated:
        meta = {
            "function": MetadataValue.text("essay_response"),
            "essay_task_id": MetadataValue.text(task_id),
            "model_used": MetadataValue.text(model_name),
            "max_tokens": MetadataValue.int(int(max_tokens) if isinstance(max_tokens, (int, float)) else 0),
            "finish_reason": MetadataValue.text(str(finish_reason)),
            "truncated": MetadataValue.bool(True),
        }
        if isinstance(completion_tokens, int):
            meta["completion_tokens"] = MetadataValue.int(completion_tokens)
        if isinstance(requested_max, int):
            meta["requested_max_tokens"] = MetadataValue.int(requested_max)
        if raw_path_str:
            meta["raw_path"] = MetadataValue.path(raw_path_str)
            meta["raw_chars"] = MetadataValue.int(len(normalized))
        raise Failure(description=f"Essay response truncated for task {task_id}", metadata=meta)

    context.add_output_metadata(
        {
            "function": MetadataValue.text("essay_response"),
            "essay_task_id": MetadataValue.text(task_id),
            "model_used": MetadataValue.text(model_name),
            "chars": MetadataValue.int(len(normalized)),
            "truncated": MetadataValue.bool(False),
            **({"raw_path": MetadataValue.path(raw_path_str)} if raw_path_str else {}),
        }
    )
    return normalized


@asset(
    partitions_def=essay_tasks_partitions,
    group_name="generation_essays",
    io_manager_key="essay_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def essay_response(context, essay_prompt, essay_generation_tasks) -> str:
    text = _essay_response_impl(context, essay_prompt, essay_generation_tasks)

    # Write to filesystem (docs)
    import time
    from pathlib import Path as _Path

    task_id = context.partition_key
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    essay_template = task_row.get("essay_template")
    model_id = task_row.get("generation_model") or task_row.get("generation_model_id")
    generator_mode = (_get_essay_generator_mode(context.resources.data_root, essay_template) or "llm").lower()

    parent_doc_id = task_row.get("parent_doc_id")
    if not (isinstance(parent_doc_id, str) and parent_doc_id.strip()):
        raise Failure(
            description="Missing parent_doc_id for essay task",
            metadata={
                "function": MetadataValue.text("essay_response"),
                "essay_task_id": MetadataValue.text(task_id),
                "resolution": MetadataValue.text("Provide parent_doc_id (draft doc id) in essay_generation_tasks.csv"),
            },
        )
    doc_id = task_row.get("doc_id")
    if not (isinstance(doc_id, str) and doc_id.strip()):
        raise Failure(
            description="Missing doc_id for essay task",
            metadata={
                "function": MetadataValue.text("essay_response"),
                "essay_task_id": MetadataValue.text(task_id),
                "resolution": MetadataValue.text("Ensure essay_generation_tasks.csv includes a doc_id column"),
            },
        )

    docs_root = Path(getattr(context.resources, "data_root", "data")) / "docs"
    # Build document using helper
    metadata = {
        "task_id": task_id,
        "essay_template": essay_template,
        "template_id": essay_template,
        "model_id": model_id,
        "parent_doc_id": parent_doc_id,
        "function": "essay_response",
    }
    prompt_text = essay_prompt if (generator_mode == "llm" and isinstance(essay_prompt, str)) else None

    doc = Document(
        stage="essay",
        doc_id=doc_id,
        parent_doc_id=parent_doc_id,
        raw_text=text,
        parsed_text=text,
        prompt_text=prompt_text,
        metadata=metadata,
    )
    target_dir = doc.write_files(docs_root)

    context.add_output_metadata(
        {
            "doc_id": MetadataValue.text(doc_id),
            "doc_dir": MetadataValue.path(str(target_dir)),
            "parent_doc_id": MetadataValue.text(str(parent_doc_id) if parent_doc_id else ""),
        }
    )

    return text
