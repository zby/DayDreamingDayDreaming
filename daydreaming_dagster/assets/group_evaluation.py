"""
Group: evaluation

Asset definitions for the evaluation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import evaluation_gens_partitions
from ..utils.membership_lookup import find_membership_row_by_gen
from .raw_data import EVALUATION_TEMPLATES_KEY
from ..utils.evaluation_parsing_config import load_parser_map, require_parser_for_template
from ..unified.stage_services import render_template, execute_evaluation_llm
from ._helpers import (
    load_generation_parsed_text,
    require_membership_row,
    emit_standard_output_metadata,
    get_run_id,
)
from ..constants import ESSAY, EVALUATION

 


@asset(
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_prompt_io_manager",
    required_resource_keys={"data_root"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def evaluation_prompt(context) -> str:

    gen_id = context.partition_key
    mrow, _cohort = find_membership_row_by_gen(getattr(context.resources, "data_root", "data"), "evaluation", str(gen_id))
    parent_gen_id = mrow.get("parent_gen_id") if mrow is not None else None
    evaluation_template = mrow.get("template_id") if mrow is not None else None
    if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
        raise Failure(
            description="Missing parent_gen_id for evaluation task",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure evaluation row exists in cohort membership with a valid parent_gen_id"),
            },
        )
    # Load target essay parsed text via shared helper (emits helpful Failure metadata)
    doc_text = load_generation_parsed_text(context, "essay", str(parent_gen_id), failure_fn_name="evaluation_prompt")
    used_source = "essay_gens"

    # Render via shared stage_services (StrictUndefined semantics preserved)
    try:
        eval_prompt = render_template("evaluation", str(evaluation_template), {"response": doc_text})
    except FileNotFoundError as e:
        raise Failure(
            description=f"Evaluation template '{evaluation_template}' not found",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "template_used": MetadataValue.text(str(evaluation_template)),
                "error": MetadataValue.text(str(e)),
            },
        )
    except Exception as e:
        raise Failure(
            description=f"Error rendering evaluation template '{evaluation_template}'",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "template_used": MetadataValue.text(str(evaluation_template)),
                "jinja_message": MetadataValue.text(str(e)),
            },
        )
    context.add_output_metadata(
        {
            "gen_id": MetadataValue.text(str(gen_id)),
            "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
            "document_content_length": MetadataValue.int(len(doc_text or "")),
            "evaluation_prompt_length": MetadataValue.int(len(eval_prompt)),
            "source_used": MetadataValue.text(used_source or ""),
            "template_used": MetadataValue.text(str(evaluation_template) if evaluation_template else ""),
        }
    )
    return eval_prompt


@asset(
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_response_io_manager",
    required_resource_keys={"openrouter_client", "data_root", "experiment_config"},
    deps=["evaluation_prompt"],
)
def evaluation_response(context, evaluation_prompt) -> str:

    gen_id = context.partition_key
    data_root = Path(getattr(context.resources, "data_root", "data"))
    row, _cohort = require_membership_row(
        context,
        "evaluation",
        str(gen_id),
        require_columns=["llm_model_id", "template_id", "parent_gen_id"],
    )
    model_name = str(row.get("llm_model_id") or "").strip()
    evaluation_template = str(row.get("template_id") or "").strip()
    parent_gen_id = str(row.get("parent_gen_id") or "").strip()

    # Resolve parser for evaluation
    parser_map = load_parser_map(data_root)
    parser_name = require_parser_for_template(evaluation_template, parser_map)

    # Execute via stage_services; prefer prompt text passed from dependency
    result = execute_evaluation_llm(
        llm=context.resources.openrouter_client,
        out_dir=data_root / "gens",
        gen_id=str(gen_id),
        template_id=evaluation_template,
        prompt_text=str(evaluation_prompt) if isinstance(evaluation_prompt, str) else render_template("evaluation", evaluation_template, {"response": load_generation_parsed_text(context, "essay", parent_gen_id, failure_fn_name="evaluation_response")}),
        model=model_name,
        parser_name=parser_name,
        max_tokens=getattr(context.resources.experiment_config, "evaluation_max_tokens", None),
        parent_gen_id=parent_gen_id,
        metadata_extra={
            "function": "evaluation_response",
            "run_id": get_run_id(context),
        },
    )

    # Emit standardized metadata for Dagster UI
    emit_standard_output_metadata(context, function="evaluation_response", gen_id=str(gen_id), result=result)
    context.log.info(f"Generated evaluation response for gen {gen_id}")

    return result.raw_text or ""
