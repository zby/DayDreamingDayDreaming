"""
Group: evaluation

Asset definitions for the evaluation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import evaluation_tasks_partitions
from ..utils.document import Document
from ..utils.filesystem_rows import (
    get_row_by_doc_id as fs_get_row_by_doc_id,
    read_parsed as fs_read_parsed,
    read_raw as fs_read_raw,
)

import pandas as pd
import logging
from jinja2 import Environment
from ..utils.dataframe_helpers import get_task_row
from ..utils.raw_readers import read_evaluation_templates
from .raw_data import EVALUATION_TEMPLATES_KEY
from ..utils.evaluation_parsing_config import load_parser_map, require_parser_for_template
from ..utils.eval_response_parser import parse_llm_response

logger = logging.getLogger(__name__)


@asset(
    partitions_def=evaluation_tasks_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"data_root"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def evaluation_prompt(context, evaluation_tasks) -> str:
    task_id = context.partition_key
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    parent_doc_id = task_row.get("parent_doc_id")
    if not (isinstance(parent_doc_id, str) and parent_doc_id.strip()):
        raise Failure(
            description="Missing parent_doc_id for evaluation task",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "evaluation_task_id": MetadataValue.text(task_id),
                "resolution": MetadataValue.text("Provide parent_doc_id (essay doc id) in evaluation_tasks.csv"),
            },
        )
    data_root = Path(getattr(context.resources, "data_root", "data"))
    docs_root = data_root / "docs"
    row = fs_get_row_by_doc_id(docs_root, "essay", str(parent_doc_id))
    if not row:
        raise Failure(
            description="Target essay document not found",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
            },
        )
    try:
        doc_text = fs_read_parsed(row)
    except Exception as e:
        base = docs_root / "essay" / str(parent_doc_id)
        raise Failure(
            description="Missing or unreadable parsed.txt for target essay document",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
                "essay_doc_dir": MetadataValue.path(str(base)),
                "error": MetadataValue.text(str(e)),
            },
        )
    used_source = "essay_fs"

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
            "parent_doc_id": MetadataValue.text(str(parent_doc_id)),
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
    required_resource_keys={"openrouter_client", "data_root", "experiment_config"},
    deps=["evaluation_prompt", "evaluation_tasks"],
)
def evaluation_response(context, evaluation_prompt, evaluation_tasks) -> str:
    task_id = context.partition_key
    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
    model_name = task_row["evaluation_model_name"]
    llm_client = context.resources.openrouter_client
    max_tokens = getattr(context.resources.experiment_config, "evaluation_max_tokens", None)
    text, info = llm_client.generate_with_info(evaluation_prompt, model=model_name, max_tokens=max_tokens)
    context.add_output_metadata({
        "evaluation_task_id": MetadataValue.text(task_id),
        "model_used": MetadataValue.text(model_name),
        "finish_reason": MetadataValue.text(str((info or {}).get("finish_reason"))),
    })
    context.log.info(f"Generated evaluation response for task {task_id} using model {model_name}")
    # Write to filesystem docs/evaluation
    import time
    from pathlib import Path as _Path
    parent_doc_id = task_row.get("parent_doc_id")
    evaluation_template = task_row.get("evaluation_template")
    model_id = task_row.get("evaluation_model") or task_row.get("evaluation_model_id")
    doc_id = task_row.get("doc_id")
    if not (isinstance(doc_id, str) and doc_id.strip()):
        raise Failure(
            description="Missing doc_id for evaluation task",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "evaluation_task_id": MetadataValue.text(task_id),
                "resolution": MetadataValue.text("Ensure evaluation_tasks.csv includes a doc_id column"),
            },
        )
    docs_root = _Path(getattr(context.resources, "data_root", "data")) / "docs"
    # Parse evaluation response using configured parser; write numeric-only parsed.txt
    normalized = str(text).replace("\r\n", "\n")
    parsed_out = None
    score_val = None
    try:
        parser_map = load_parser_map(_Path(getattr(context.resources, "data_root", "data")))
        strategy = require_parser_for_template(str(evaluation_template), parser_map)
        res = parse_llm_response(normalized, strategy)
        score_val = res.get("score")
        if isinstance(score_val, (int, float)):
            parsed_out = f"{float(score_val)}\n"
    except Exception:
        # If parsing fails, do not synthesize a SCORE line; downstream will surface the error.
        score_val = None
    metadata = {
        "task_id": task_id,
        "evaluation_template": evaluation_template,
        "template_id": evaluation_template,
        "model_id": model_id,
        "parent_doc_id": parent_doc_id,
        "function": "evaluation_response",
    }
    prompt_text = evaluation_prompt if isinstance(evaluation_prompt, str) else None
    doc = Document(
        stage="evaluation",
        doc_id=doc_id,
        parent_doc_id=parent_doc_id,
        raw_text=normalized,
        parsed_text=parsed_out,  # do not write parsed.txt when parsing fails
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
