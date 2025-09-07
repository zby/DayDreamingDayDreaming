"""
Group: evaluation

Asset definitions for the evaluation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import evaluation_tasks_partitions
from ..utils.documents_index import DocumentRow
from ..utils.ids import (
    compute_logical_key_id_evaluation,
)
import pandas as pd
import logging
from jinja2 import Environment
from ..utils.dataframe_helpers import get_task_row
from ..utils.raw_readers import read_evaluation_templates
from .raw_data import EVALUATION_TEMPLATES_KEY

logger = logging.getLogger(__name__)


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"data_root", "documents_index"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def evaluation_prompt(context, evaluation_tasks) -> str:
    task_id = context.partition_key
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    # DB-first: resolve target document by task id/stage
    doc_text = None
    used_source = None
    idx_res = getattr(context.resources, "documents_index", None)
    if idx_res and getattr(idx_res, "index_enabled", False):
        idx = idx_res.get_index()
        document_id = task_row.get("document_id")
        # Robust stage inference: many curated CSVs lack an explicit 'stage' column
        stage_raw = str(task_row.get("stage") or "").strip().lower()
        if stage_raw:
            stage = "essay" if stage_raw.startswith("essay") else "draft"
        else:
            # Infer from presence of essay_task_id/source_dir/file_path
            source_dir = str(task_row.get("source_dir") or "").lower()
            file_path = str(task_row.get("file_path") or "").lower()
            if isinstance(task_row.get("essay_task_id"), str) and task_row.get("essay_task_id"):
                stage = "essay"
            elif "essay" in source_dir or "essay_responses" in file_path:
                stage = "essay"
            else:
                stage = "draft"
        context.log.info(
            f"evaluation_prompt: resolving target from index stage={stage} document_id={document_id}"
        )
        key = str(document_id)
        row = idx.get_latest_by_task(stage, key)
        if row:
            try:
                doc_text = idx.read_parsed(row)
            except Exception:
                doc_text = idx.read_raw(row)
            used_source = f"{stage}_db"
        else:
            # Emit helpful diagnostics
            essay_count = idx.connect().execute(
                "SELECT COUNT(1) AS c FROM documents WHERE stage='essay' AND task_id=?",
                (key,),
            ).fetchone()
            draft_count = idx.connect().execute(
                "SELECT COUNT(1) AS c FROM documents WHERE stage='draft' AND task_id=?",
                (key,),
            ).fetchone()
            context.log.warning(
                "evaluation_prompt: no row found for stage=%s, task_id=%s (essay_count=%s, draft_count=%s)",
                stage,
                key,
                getattr(essay_count, "get", lambda k: essay_count["c"] if isinstance(essay_count, dict) else essay_count)("c") if essay_count else None,
                getattr(draft_count, "get", lambda k: draft_count["c"] if isinstance(draft_count, dict) else draft_count)("c") if draft_count else None,
            )
            # Try the opposite stage just in case inference is wrong
            alt_stage = "draft" if stage == "essay" else "essay"
            alt_row = idx.get_latest_by_task(alt_stage, key)
            if alt_row:
                try:
                    doc_text = idx.read_parsed(alt_row)
                except Exception:
                    doc_text = idx.read_raw(alt_row)
                used_source = f"{alt_stage}_db"
                context.log.info(
                    f"evaluation_prompt: resolved via alternate stage={alt_stage} for task_id={key}"
                )

    if doc_text is None:
        file_path = task_row.get("file_path")
        # If legacy writes are disabled and DB did not resolve, fail fast
        import os as _os
        legacy_enabled = _os.getenv("DD_DOCS_LEGACY_WRITE_ENABLED", "1") in ("1", "true", "True")
        if not legacy_enabled:
            raise Failure(
                description="Evaluation target not found in documents index and legacy filesystem fallback is disabled",
                metadata={
                    "evaluation_task_id": MetadataValue.text(task_id),
                    "document_id": MetadataValue.text(str(task_row.get("document_id"))),
                },
            )
        if not isinstance(file_path, str) or not len(file_path):
            raise Failure(
                description=f"Missing or invalid file_path for evaluation task '{task_id}'",
                metadata={
                    "evaluation_task_id": MetadataValue.text(task_id),
                    "file_path": MetadataValue.text(str(file_path)),
                },
            )
        expected_path = Path(file_path)
        try:
            doc = expected_path.read_text(encoding="utf-8")
            if not doc:
                raise ValueError(f"Empty document content loaded for {file_path}")
            doc_text = doc
            used_source = "file"
        except FileNotFoundError as e:
            raise Failure(
                description=f"Missing document for evaluation task '{task_id}'",
                metadata={
                    "evaluation_task_id": MetadataValue.text(task_id),
                    "expected_file_path": MetadataValue.path(str(expected_path)),
                    "source_asset": MetadataValue.text(str(task_row.get("source_asset"))),
                    "source_dir": MetadataValue.text(str(task_row.get("source_dir"))),
                },
            ) from e

    eval_df = read_evaluation_templates(Path(context.resources.data_root))
    evaluation_templates_dict: dict[str, str] = {}
    if "content" in eval_df.columns:
        evaluation_templates_dict = eval_df.set_index("template_id")["content"].to_dict()
    else:
        templates_base = Path(context.resources.data_root) / "1_raw" / "evaluation_templates"
        for _, row in eval_df.iterrows():
            template_id = row.get("template_id")
            if not isinstance(template_id, str) or not len(template_id):
                continue
            fp = templates_base / f"{template_id}.txt"
            try:
                evaluation_templates_dict[template_id] = fp.read_text(encoding="utf-8")
            except FileNotFoundError:
                context.log.warning(f"Evaluation template file not found: {fp}")

    template_id = task_row["evaluation_template"]
    template_content = evaluation_templates_dict[template_id]
    env = Environment()
    template = env.from_string(template_content)
    eval_prompt = template.render(response=doc_text)
    context.add_output_metadata(
        {
            "evaluation_task_id": MetadataValue.text(task_id),
            "source_stage": MetadataValue.text(str(task_row.get("stage"))),
            "document_id": MetadataValue.text(str(task_row.get("document_id"))),
            "document_content_length": MetadataValue.int(len(doc_text or "")),
            "evaluation_prompt_length": MetadataValue.int(len(eval_prompt)),
            **({"target_path": MetadataValue.path(str(task_row.get("file_path")))} if task_row.get("file_path") else {}),
            "source_used": MetadataValue.text(used_source or ""),
            "template_used": MetadataValue.text(task_row["evaluation_template"]),
            "model_planned": MetadataValue.text(task_row["evaluation_model_name"]),
        }
    )
    return eval_prompt


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_response_io_manager",
    required_resource_keys={"openrouter_client"},
    deps=["evaluation_prompt", "evaluation_tasks"],
)
def evaluation_response(context, evaluation_prompt, evaluation_tasks) -> str:
    task_id = context.partition_key
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    model_name = task_row["evaluation_model_name"]
    llm_client = context.resources.openrouter_client
    text, info = llm_client.generate_with_info(evaluation_prompt, model=model_name)
    context.add_output_metadata({
        "evaluation_task_id": MetadataValue.text(task_id),
        "model_used": MetadataValue.text(model_name),
        "finish_reason": MetadataValue.text(str((info or {}).get("finish_reason"))),
    })
    context.log.info(f"Generated evaluation response for task {task_id} using model {model_name}")
    # Dual-write to documents index under flag
    try:
        idx_res = context.resources.documents_index
    except Exception:
        idx_res = None

    if idx_res and getattr(idx_res, "index_enabled", False):
        import json, time, hashlib
        from pathlib import Path as _Path

        idx = idx_res.get_index()
        document_id = task_row.get("document_id")
        stage_raw = str(task_row.get("stage") or "")
        target_stage = "essay" if stage_raw.startswith("essay") else "draft"
        parent_doc_id = None
        try:
            parent = idx.get_latest_by_task(target_stage, str(document_id))
            if parent:
                parent_doc_id = parent.get("doc_id")
        except Exception:
            parent_doc_id = None

        evaluation_template = task_row.get("evaluation_template")
        model_id = task_row.get("evaluation_model") or task_row.get("evaluation_model_id")

        # Compute logical key
        logical_key_id = compute_logical_key_id_evaluation(str(parent_doc_id or document_id), str(evaluation_template), str(model_id))
        run_id = getattr(context, "run_id", "run")
        attempt = int(time.time_ns())
        from ..utils.ids import new_doc_id, doc_dir as build_doc_dir
        doc_id = new_doc_id(logical_key_id, run_id, attempt)

        docs_root = _Path(idx.docs_root)
        stage = "evaluation"
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
            if getattr(idx_res, "prompt_copy_enabled", True) and isinstance(evaluation_prompt, str):
                _write_atomic(target_dir / "prompt.txt", evaluation_prompt)
        except Exception:
            pass

        metadata = {
            "task_id": task_id,
            "evaluation_template": evaluation_template,
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
            template_id=str(evaluation_template),
            model_id=str(model_id),
            run_id=str(run_id),
            parser=None,
            status="ok",
            doc_dir=str(rel_dir),
            raw_chars=len(raw_text or ""),
            parsed_chars=len(text or ""),
            content_hash=content_hash,
            meta_small={"function": "evaluation_response"},
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
            context.log.warning(f"DocumentsIndex insert failed for evaluation_response {task_id}: {e}")

    return text
