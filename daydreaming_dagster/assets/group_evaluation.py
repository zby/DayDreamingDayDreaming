"""
Group: evaluation

Asset definitions for the evaluation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import evaluation_tasks_partitions
from ..utils.documents_index import DocumentRow
from ..utils.document import Document
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
    # DB-only resolution (no filesystem fallback)
    idx_res = context.resources.documents_index
    if not idx_res:
        raise Failure(
            description="Documents index is required for evaluation_prompt",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "resolution": MetadataValue.text("Ensure documents_index resource is configured and DB is backfilled"),
            },
        )
    idx = idx_res.get_index()
    document_id = task_row.get("document_id")
    stage_raw = str(task_row.get("stage") or "").strip().lower()
    if stage_raw:
        stage = "essay" if stage_raw.startswith("essay") else "draft"
    else:
        stage = "essay" if (isinstance(task_row.get("essay_task_id"), str) and task_row.get("essay_task_id")) else "draft"
    key = str(document_id)
    row = idx.get_latest_by_task(stage, key)
    if not row:
        raise Failure(
            description="Target document not found in documents index",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "stage": MetadataValue.text(stage),
                "document_task_id": MetadataValue.text(key),
            },
        )
    try:
        doc_text = idx.read_parsed(row)
    except Exception:
        doc_text = idx.read_raw(row)
    used_source = f"{stage}_db"

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
    required_resource_keys={"openrouter_client", "documents_index"},
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
        import time
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
        from ..utils.ids import new_doc_id
        doc_id = new_doc_id(logical_key_id, run_id, attempt)

        docs_root = _Path(idx.docs_root)
        metadata = {
            "task_id": task_id,
            "evaluation_template": evaluation_template,
            "model_id": model_id,
            "parent_doc_id": parent_doc_id,
            "function": "evaluation_response",
        }
        prompt_text = evaluation_prompt if getattr(idx_res, "prompt_copy_enabled", True) and isinstance(evaluation_prompt, str) else None
        doc = Document(
            stage="evaluation",
            logical_key_id=logical_key_id,
            doc_id=doc_id,
            parent_doc_id=parent_doc_id,
            raw_text=text,
            parsed_text=text,
            prompt_text=prompt_text,
            metadata=metadata,
        )
        target_dir = doc.write_files(docs_root)

        row = doc.to_index_row(
            docs_root,
            task_id=task_id,
            template_id=str(evaluation_template) if evaluation_template is not None else None,
            model_id=str(model_id) if model_id is not None else None,
            run_id=str(run_id),
            parser=None,
            status="ok",
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
