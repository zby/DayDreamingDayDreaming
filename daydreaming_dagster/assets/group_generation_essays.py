"""
Group: generation_essays

Asset definitions for the essay (Phase‑2) generation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from jinja2 import Environment
import os
from .partitions import essay_tasks_partitions
from ..utils.template_loader import load_generation_template
from ..utils.raw_readers import read_essay_templates, read_draft_templates
from ..utils.draft_parsers import get_draft_parser
from ..utils.dataframe_helpers import get_task_row
from ..utils.shared_context import MockLoadContext
from ..utils.raw_write import save_versioned_raw_text

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
    env = JINJA
    template = env.from_string(tmpl)
    prompt = template.render(draft_lines=draft_lines)
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
)
def essay_prompt(context, essay_generation_tasks) -> str:
    return _essay_prompt_impl(context, essay_generation_tasks)


def _essay_response_impl(context, essay_prompt, essay_generation_tasks) -> str:
    """Generate Phase‑2 essay responses using LLM or parser/copy modes."""
    task_id = context.partition_key
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    template_name = task_row["essay_template"]
    mode = _get_essay_generator_mode(context.resources.data_root, template_name) or "llm"

    if mode == "copy":
        draft_task_id = task_row.get("draft_task_id")
        text, _ = _load_phase1_text(context, draft_task_id)
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_response"),
                "mode": MetadataValue.text("copy"),
                "essay_task_id": MetadataValue.text(task_id),
                "source_draft_task_id": MetadataValue.text(str(draft_task_id)),
                "chars": MetadataValue.int(len(text)),
                "lines": MetadataValue.int(sum(1 for _ in text.splitlines())),
            }
        )
        return text
    if mode == "parser":
        draft_task_id = task_row.get("draft_task_id")
        raw_text, _ = _load_phase1_text(context, draft_task_id)
        parser_name = None
        try:
            ddf = read_draft_templates(Path(context.resources.data_root), filter_active=False)
            if not ddf.empty and "parser" in ddf.columns:
                r = ddf[ddf["template_id"] == task_row.get("draft_template")]
                if not r.empty:
                    val = r.iloc[0].get("parser")
                    if isinstance(val, str) and val.strip():
                        parser_name = val.strip()
        except Exception:
            parser_name = None
        parser_fn = get_draft_parser(parser_name) if parser_name else None
        if not parser_fn:
            raise Failure(
                description="Parser mode requested but parser not configured",
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "essay_task_id": MetadataValue.text(task_id),
                    "essay_template": MetadataValue.text(template_name),
                    "draft_template": MetadataValue.text(str(task_row.get("draft_template"))),
                },
            )
        try:
            parsed = parser_fn(raw_text)
        except Exception as e:
            raise Failure(
                description="Parser raised while producing essay text",
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "essay_task_id": MetadataValue.text(task_id),
                    "parser": MetadataValue.text(str(parser_name)),
                    "error": MetadataValue.text(str(e)),
                },
            ) from e
        if not isinstance(parsed, str) or not parsed.strip():
            raise Failure(
                description="Parser returned empty/invalid text",
                metadata={
                    "function": MetadataValue.text("essay_response"),
                    "essay_task_id": MetadataValue.text(task_id),
                    "parser": MetadataValue.text(str(parser_name)),
                },
            )
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_response"),
                "mode": MetadataValue.text("parser"),
                "essay_task_id": MetadataValue.text(task_id),
                "parser": MetadataValue.text(str(parser_name)),
            }
        )
        return parsed

    # Default LLM path (with RAW side-write + truncation guard)
    model_name = task_row["generation_model_name"]
    llm_client = context.resources.openrouter_client
    max_tokens = getattr(context.resources.experiment_config, "essay_generation_max_tokens", None) or 40960
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
    required_resource_keys={"openrouter_client", "experiment_config", "data_root", "draft_response_io_manager"},
)
def essay_response(context, essay_prompt, essay_generation_tasks) -> str:
    return _essay_response_impl(context, essay_prompt, essay_generation_tasks)
