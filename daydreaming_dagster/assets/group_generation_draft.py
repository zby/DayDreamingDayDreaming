"""
Group: generation_draft

Asset definitions for the draft (Phase‑1) generation stage.
"""

from dagster import asset, Failure, MetadataValue
import pandas as pd
from pathlib import Path
from jinja2 import Environment, TemplateSyntaxError
from .partitions import draft_tasks_partitions
from ..utils.template_loader import load_generation_template
from ..utils.raw_readers import read_draft_templates
from ..utils.draft_parsers import get_draft_parser
from ..utils.dataframe_helpers import get_task_row
from ..utils.raw_write import save_versioned_raw_text
from ..utils.document import Document

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


def _draft_response_impl(context, draft_prompt, draft_generation_tasks) -> str:
    """Generate Phase 1 LLM responses for drafts (core implementation)."""
    task_id = context.partition_key
    task_row = get_task_row(draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks")
    model_name = task_row["generation_model_name"]

    llm_client = context.resources.openrouter_client
    experiment_config = context.resources.experiment_config
    max_tokens = experiment_config.draft_generation_max_tokens
    # Unified client path
    text, info = llm_client.generate_with_info(draft_prompt, model=model_name, max_tokens=max_tokens)

    # Normalize newlines and save RAW immediately (before any validation) to aid debugging
    normalized = str(text).replace("\r\n", "\n")
    save_raw = bool(getattr(experiment_config, "save_raw_draft_enabled", True))
    raw_dir_override = getattr(experiment_config, "raw_draft_dir_override", None)
    data_root = Path(getattr(context.resources, "data_root", "data"))
    raw_dir = Path(raw_dir_override) if raw_dir_override else data_root / "3_generation" / "draft_responses_raw"
    raw_path_str = None
    if save_raw:
        raw_path_str = save_versioned_raw_text(raw_dir, task_id, normalized, logger=context.log)

    # Validate minimum lines after ensuring RAW is persisted
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
                **({"raw_path": MetadataValue.path(raw_path_str)} if raw_path_str else {}),
            },
        )

    # If the response was truncated (e.g., hit token limit), mark as failure after saving RAW
    finish_reason = (info or {}).get("finish_reason") if isinstance(info, dict) else None
    was_truncated = bool((info or {}).get("truncated") if isinstance(info, dict) else False)
    if was_truncated:
        usage = (info or {}).get("usage") if isinstance(info, dict) else None
        completion_tokens = None
        requested_max = None
        if isinstance(usage, dict):
            completion_tokens = usage.get("completion_tokens")
            requested_max = usage.get("max_tokens")
        raise Failure(
            description=f"Draft response truncated for task {task_id}",
            metadata={
                "function": MetadataValue.text("draft_response"),
                "task_id": MetadataValue.text(task_id),
                "model_name": MetadataValue.text(model_name),
                "finish_reason": MetadataValue.text(str(finish_reason)),
                "truncated": MetadataValue.bool(True),
                "raw_chars": MetadataValue.int(len(normalized)),
                **({"completion_tokens": MetadataValue.int(int(completion_tokens))} if isinstance(completion_tokens, int) else {}),
                **({"requested_max_tokens": MetadataValue.int(int(requested_max))} if isinstance(requested_max, int) else {}),
                **({"raw_path": MetadataValue.path(raw_path_str)} if raw_path_str else {}),
            },
        )

    # Parse RAW response according to draft template's parser (identity if unspecified)
    draft_template = task_row.get("draft_template")
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

    parsed_text = normalized
    parser_used = parser_name or "identity"
    if parser_name:
        parser_fn = get_draft_parser(parser_name)
        if parser_fn is None:
            raise Failure(
                description=(f"Parser '{parser_name}' not found in registry for draft template"),
                metadata={
                    "function": MetadataValue.text("draft_response"),
                    "draft_task_id": MetadataValue.text(task_id),
                    "draft_template": MetadataValue.text(str(draft_template)),
                    "parser": MetadataValue.text(str(parser_name)),
                    "resolution": MetadataValue.text("Use a registered parser name in data/1_raw/draft_templates.csv or leave blank for identity."),
                },
            )
        try:
            parsed_text = parser_fn(normalized)
        except Exception as e:
            desc = "Parser raised an exception while processing RAW draft"
            if raw_path_str:
                desc += f" (RAW: {raw_path_str})"
            meta = {
                "function": MetadataValue.text("draft_response"),
                "draft_task_id": MetadataValue.text(task_id),
                "draft_template": MetadataValue.text(str(draft_template)),
                "parser": MetadataValue.text(str(parser_name)),
                "error": MetadataValue.text(str(e)),
            }
            if raw_path_str:
                meta["raw_path"] = MetadataValue.path(raw_path_str)
                meta["raw_chars"] = MetadataValue.int(len(normalized))
            raise Failure(
                description=desc,
                metadata=meta,
            ) from e
        if not isinstance(parsed_text, str) or not parsed_text.strip():
            raise Failure(
                description="Parser returned empty/invalid text",
                metadata={
                    "function": MetadataValue.text("draft_response"),
                    "draft_task_id": MetadataValue.text(task_id),
                    "draft_template": MetadataValue.text(str(draft_template)),
                    "parser": MetadataValue.text(str(parser_name)),
                },
            )

    # Final metadata and output
    context.log.info(
        f"Generated draft response for task {task_id} using model {model_name} ({len(response_lines)} raw lines); parser={parser_used}"
    )
    meta = {
        "function": MetadataValue.text("draft_response"),
        "raw_line_count": MetadataValue.int(len(response_lines)),
        "model_used": MetadataValue.text(model_name),
        "max_tokens": MetadataValue.int(int(max_tokens) if isinstance(max_tokens, (int, float)) else 0),
        "parser": MetadataValue.text(parser_used),
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
    partitions_def=draft_tasks_partitions,
    group_name="generation_draft",
    io_manager_key="draft_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def draft_response(context, draft_prompt, draft_generation_tasks) -> str:
    """Generate Phase 1 LLM responses for drafts."""
    parsed = _draft_response_impl(context, draft_prompt, draft_generation_tasks)

    # Write doc files under data_root/docs/draft using Document helper
    import time
    from pathlib import Path as _Path

    task_id = context.partition_key
    # Pull task row again to avoid plumb-through changes
    task_row = get_task_row(draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks")
    combo_id = task_row.get("combo_id")
    draft_template = task_row.get("draft_template")
    model_id = task_row.get("generation_model") or task_row.get("generation_model_id")
    doc_id = task_row.get("doc_id")
    if not (isinstance(doc_id, str) and doc_id.strip()):
        raise Failure(
            description="Missing doc_id for draft task",
            metadata={
                "function": MetadataValue.text("draft_response"),
                "draft_task_id": MetadataValue.text(task_id),
                "resolution": MetadataValue.text("Ensure draft_generation_tasks.csv includes a doc_id column"),
            },
        )

    # FALLBACK(OPS): prefer RAW from versioned raw dir if present; if missing, use parsed text.
    # This is a developer convenience; prefer ensuring RAW side-write exists for reproducibility.
    raw_text = parsed
    try:
        data_root = _Path(getattr(context.resources, "data_root", "data"))
        raw_dir = data_root / "3_generation" / "draft_responses_raw"
        best = None
        best_ver = -1
        prefix = f"{task_id}_v"
        if raw_dir.exists():
            for name in raw_dir.iterdir():
                if not name.name.startswith(prefix) or name.suffix != ".txt":
                    continue
                try:
                    v = int(name.stem.split("_v")[-1])
                except Exception:
                    continue
                if v > best_ver:
                    best_ver = v
                    best = name
        if best and best.exists():
            raw_text = best.read_text(encoding="utf-8")
    except Exception:
        pass

    # Build document and write files
    docs_root = _Path(getattr(context.resources, "data_root", "data")) / "docs"
    metadata_json = {
        "task_id": task_id,
        "combo_id": combo_id,
        "draft_template": draft_template,
        "template_id": draft_template,
        "model_id": model_id,
        "usage": None,
        "function": "draft_response",
    }
    # Copy the prompt alongside the document for traceability when available
    prompt_text = draft_prompt if isinstance(draft_prompt, str) else None
    doc = Document(
        stage="draft",
        doc_id=doc_id,
        parent_doc_id=None,
        raw_text=raw_text,
        parsed_text=parsed,
        prompt_text=prompt_text,
        metadata=metadata_json,
    )
    target_dir = doc.write_files(docs_root)
    context.add_output_metadata(
        {
            "doc_id": MetadataValue.text(doc_id),
            "doc_dir": MetadataValue.path(str(target_dir)),
        }
    )

    return parsed
