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
from ..utils.ids import (
    compute_logical_key_id_draft,
    new_doc_id,
    doc_dir as build_doc_dir,
)
from ..utils.documents_index import DocumentRow

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
    max_tokens = getattr(experiment_config, "draft_generation_max_tokens", None) or 20480
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

    # Dual-write to documents index (Phase 1) under feature flag
    try:
        idx_res = context.resources.documents_index
    except Exception:
        idx_res = None

    if idx_res:
        import json, time, hashlib
        from pathlib import Path as _Path

        task_id = context.partition_key
        # Pull task row again to avoid plumb-through changes
        task_row = get_task_row(draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks")
        combo_id = task_row.get("combo_id")
        draft_template = task_row.get("draft_template")
        model_id = task_row.get("generation_model") or task_row.get("generation_model_id")

        # Compute IDs
        logical_key_id = compute_logical_key_id_draft(str(combo_id), str(draft_template), str(model_id))
        run_id = getattr(context, "run_id", "run")
        attempt = int(time.time_ns())
        doc_id = new_doc_id(logical_key_id, run_id, attempt)

        # Build doc_dir and write files atomically
        idx = idx_res.get_index()
        docs_root = _Path(idx.docs_root)
        stage = "draft"
        target_dir = build_doc_dir(docs_root, stage, logical_key_id, doc_id)
        target_dir.mkdir(parents=True, exist_ok=True)

        def _write_atomic(path: _Path, data: str):
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(data, encoding="utf-8")
            tmp.replace(path)

        # Prefer RAW from versioned raw dir if present; fallback to parsed text
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

        _write_atomic(target_dir / "raw.txt", raw_text)
        _write_atomic(target_dir / "parsed.txt", parsed)

        # Copy prompt if enabled
        try:
            if getattr(idx_res, "prompt_copy_enabled", True) and isinstance(draft_prompt, str):
                _write_atomic(target_dir / "prompt.txt", draft_prompt)
        except Exception:
            pass

        # Build metadata.json
        meta_small = {
            "function": "draft_response",
            "legacy_write_ok": True if getattr(idx_res, "legacy_write_enabled", True) else False,
        }
        info = {}
        try:
            # Not available here; left empty for now
            info = {}
        except Exception:
            info = {}
        metadata = {
            "task_id": task_id,
            "combo_id": combo_id,
            "draft_template": draft_template,
            "model_id": model_id,
            "usage": info.get("usage") if isinstance(info, dict) else None,
        }
        _write_atomic(target_dir / "metadata.json", json.dumps(metadata, ensure_ascii=False, indent=2))

        # Insert DB row
        content_hash = hashlib.sha256((raw_text or "").encode("utf-8")).hexdigest()
        rel_dir = target_dir.relative_to(docs_root)
        row = DocumentRow(
            doc_id=doc_id,
            logical_key_id=logical_key_id,
            stage=stage,
            task_id=task_id,
            parent_doc_id=None,
            template_id=str(draft_template),
            model_id=str(model_id),
            run_id=str(run_id),
            parser=None,
            status="ok",
            usage_prompt_tokens=None,
            usage_completion_tokens=None,
            usage_max_tokens=None,
            doc_dir=str(rel_dir),
            raw_chars=len(raw_text or ""),
            parsed_chars=len(parsed or ""),
            content_hash=content_hash,
            meta_small=meta_small,
            lineage_prev_doc_id=None,
        )
        try:
            idx.insert_document(row)
            context.add_output_metadata(
                {
                    "doc_id": MetadataValue.text(doc_id),
                    "logical_key_id": MetadataValue.text(logical_key_id),
                    "doc_dir": MetadataValue.path(str(target_dir)),
                }
            )
        except Exception as e:
            # Do not fail the asset in Phase 1; just log. The legacy path remains authoritative.
            context.log.warning(f"DocumentsIndex insert failed for draft_response {task_id}: {e}")

    return parsed
