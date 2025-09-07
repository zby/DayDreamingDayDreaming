"""
Group: generation_essays

Asset definitions for the essay (Phase‑2) generation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from jinja2 import Environment
from .partitions import essay_tasks_partitions
from ..utils.template_loader import load_generation_template
from ..utils.raw_readers import read_essay_templates, read_draft_templates
from ..utils.draft_parsers import get_draft_parser
from ..utils.dataframe_helpers import get_task_row
from ..utils.shared_context import MockLoadContext
from ..utils.raw_write import save_versioned_raw_text
from ..utils.ids import (
    compute_logical_key_id_draft,
    compute_logical_key_id_essay,
    new_doc_id,
    doc_dir as build_doc_dir,
)
from ..utils.documents_index import DocumentRow

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


    


def _load_phase1_text_by_parent_doc(context, parent_doc_id: str) -> tuple[str, str]:
    """Load Phase‑1 text by parent_doc_id using the documents index.

    Returns (normalized_text, source_label).
    """
    idx_res = context.resources.documents_index
    if not idx_res:
        raise Failure(
            description="Documents index not available to resolve draft by parent_doc_id",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_parent_doc"),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
            },
        )
    idx = idx_res.get_index()
    row = idx.get_by_doc_id_and_stage(str(parent_doc_id), "draft")
    if not row:
        raise Failure(
            description="Parent draft document not found in index",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_parent_doc"),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
            },
        )
    try:
        text = idx.read_parsed(row)
    except Exception:
        text = idx.read_raw(row)
    return str(text).replace("\r\n", "\n"), "draft_db_parent"


def _load_phase1_text_by_draft_task(context, draft_task_id: str) -> tuple[str, str]:
    """Load Phase‑1 text by draft_task_id using the documents index only.

    Returns (normalized_text, source_label).
    """
    if not (isinstance(draft_task_id, str) and draft_task_id):
        raise Failure(
            description="Missing draft_task_id for essay task",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_draft_task"),
            },
        )
    idx_res = context.resources.documents_index
    if not idx_res:
        raise Failure(
            description="Documents index not available to resolve draft by task id",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_draft_task"),
                "draft_task_id": MetadataValue.text(str(draft_task_id)),
            },
        )
    idx = idx_res.get_index()
    row = idx.get_latest_by_task("draft", str(draft_task_id))
    if not row:
        raise Failure(
            description="Draft not found in documents index for task id",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_draft_task"),
                "draft_task_id": MetadataValue.text(str(draft_task_id)),
            },
        )
    try:
        text = idx.read_parsed(row)
    except Exception:
        text = idx.read_raw(row)
    return str(text).replace("\r\n", "\n"), "draft_db_task"


def _load_phase1_text_by_combo_model(context, combo_id: str, model_id: str) -> tuple[str, str]:
    idx_res = context.resources.documents_index
    if not idx_res:
        raise Failure(
            description="Documents index not available to resolve draft by combo/model",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_combo_model"),
                "combo_id": MetadataValue.text(str(combo_id)),
                "model_id": MetadataValue.text(str(model_id)),
            },
        )
    idx = idx_res.get_index()
    like = f"{combo_id}_%_{model_id}"
    row = idx.connect().execute(
        "SELECT * FROM documents WHERE stage='draft' AND task_id LIKE ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (like,),
    ).fetchone()
    if row:
        try:
            text = idx.read_parsed(row)
        except Exception:
            text = idx.read_raw(row)
        return str(text).replace("\r\n", "\n"), "draft_db_combo_model"
    raise Failure(
        description="Draft response not found by combo/model prefix rule",
        metadata={
            "function": MetadataValue.text("_load_phase1_text_by_combo_model"),
            "combo_id": MetadataValue.text(str(combo_id)),
            "model_id": MetadataValue.text(str(model_id)),
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

    # Resolve Phase‑1 text strictly via draft_task_id using the documents index
    draft_task_id = task_row.get("draft_task_id")
    draft_text, used_source = _load_phase1_text_by_draft_task(context, draft_task_id)
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
    required_resource_keys={"data_root", "documents_index"},
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
    mode = _get_essay_generator_mode(context.resources.data_root, template_name) or "llm"

    # Load Phase‑1 draft text; prefer index when available, otherwise fall back to IO/filesystem
    draft_task_id = task_row.get("draft_task_id")
    # DB-only resolution of Phase-1 text
    draft_text, used_source = _load_phase1_text_by_draft_task(context, draft_task_id)

    if mode == "copy":
        text = draft_text
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_response"),
                "mode": MetadataValue.text("copy"),
                "essay_task_id": MetadataValue.text(task_id),
                "source_draft_task_id": MetadataValue.text(str(draft_task_id)),
                # Back-compat with older naming in tests/reports
                "source_link_task_id": MetadataValue.text(str(draft_task_id)),
                "source": MetadataValue.text(used_source),
                "chars": MetadataValue.int(len(text)),
                "lines": MetadataValue.int(sum(1 for _ in text.splitlines())),
            }
        )
        return text
    if mode == "parser":
        raw_text = draft_text
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
                "source": MetadataValue.text(used_source),
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
    required_resource_keys={"openrouter_client", "experiment_config", "data_root", "documents_index"},
)
def essay_response(context, essay_prompt, essay_generation_tasks) -> str:
    text = _essay_response_impl(context, essay_prompt, essay_generation_tasks)

    # Write to documents index
    idx_res = context.resources.documents_index
    import json, time, hashlib
    from pathlib import Path as _Path

    task_id = context.partition_key
    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
    essay_template = task_row.get("essay_template")
    model_id = task_row.get("generation_model") or task_row.get("generation_model_id")
    draft_task_id = task_row.get("draft_task_id")
    generator_mode = (_get_essay_generator_mode(context.resources.data_root, essay_template) or "llm").lower()

    idx = idx_res.get_index()
    # Derive parent (draft) doc from draft_task_id via the index (fail fast if missing)
    parent_row = idx.get_latest_by_task("draft", str(draft_task_id))
    if not parent_row:
        raise Failure(
            description="Draft not found in documents index for essay parent",
            metadata={
                "function": MetadataValue.text("essay_response"),
                "essay_task_id": MetadataValue.text(task_id),
                "draft_task_id": MetadataValue.text(str(draft_task_id)),
            },
        )
    parent_doc_id = parent_row.get("doc_id")
    logical_key_id = compute_logical_key_id_essay(str(parent_doc_id), str(essay_template), str(model_id))

    run_id = getattr(context, "run_id", "run")
    attempt = int(time.time_ns())
    doc_id = new_doc_id(logical_key_id, run_id, attempt)

    docs_root = _Path(idx.docs_root)
    stage = "essay"
    target_dir = build_doc_dir(docs_root, stage, logical_key_id, doc_id)
    target_dir.mkdir(parents=True, exist_ok=True)

    def _write_atomic(path: _Path, data: str):
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(data, encoding="utf-8")
        tmp.replace(path)

    raw_text = text
    _write_atomic(target_dir / "raw.txt", raw_text)
    _write_atomic(target_dir / "parsed.txt", text)
    try:
        # Only write prompt.txt for LLM-generated essays (skip for copy/parser modes)
        if generator_mode == "llm" and getattr(idx_res, "prompt_copy_enabled", True) and isinstance(essay_prompt, str):
            _write_atomic(target_dir / "prompt.txt", essay_prompt)
    except Exception:
        pass

    metadata = {
        "task_id": task_id,
        "essay_template": essay_template,
        "model_id": model_id,
        "parent_doc_id": parent_doc_id,
    }
    _write_atomic(target_dir / "metadata.json", json.dumps(metadata, ensure_ascii=False, indent=2))

    content_hash = hashlib.sha256((raw_text or "").encode("utf-8")).hexdigest()
    rel_dir = target_dir.relative_to(docs_root)
    row = DocumentRow(
        doc_id=doc_id,
        logical_key_id=logical_key_id,
        stage=stage,
        task_id=task_id,
        parent_doc_id=parent_doc_id,
        template_id=str(essay_template),
        model_id=str(model_id),
        run_id=str(run_id),
        parser=None,
        status="ok",
        doc_dir=str(rel_dir),
        raw_chars=len(raw_text or ""),
        parsed_chars=len(text or ""),
        content_hash=content_hash,
        meta_small={"function": "essay_response"},
        lineage_prev_doc_id=None,
    )
    try:
        idx.insert_document(row)
        context.add_output_metadata(
            {
                "doc_id": MetadataValue.text(doc_id),
                "logical_key_id": MetadataValue.text(logical_key_id),
                "doc_dir": MetadataValue.path(str(target_dir)),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id) if parent_doc_id else ""),
            }
        )
    except Exception as e:
        context.log.warning(f"DocumentsIndex insert failed for essay_response {task_id}: {e}")

    return text
