"""
Group: evaluation

Asset definitions for the evaluation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from .partitions import evaluation_gens_partitions
from ..utils.generation import Generation
from ..utils.metadata import build_generation_metadata
 
from jinja2 import Environment
from ..utils.dataframe_helpers import get_task_row, resolve_llm_model_id
from ..utils.membership_lookup import find_membership_row_by_gen
from ..utils.raw_readers import read_evaluation_templates
from .raw_data import EVALUATION_TEMPLATES_KEY
from ..utils.evaluation_parsing_config import load_parser_map, require_parser_for_template
from ..unified.stage_runner import StageRunSpec, StageRunner
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
    data_root = Path(getattr(context.resources, "data_root", "data"))
    gens_root = data_root / "gens"
    base = gens_root / ESSAY / str(parent_gen_id)
    try:
        gen = Generation.load(gens_root, ESSAY, str(parent_gen_id))
    except Exception as e:
        raise Failure(
            description="Target essay document not found",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "error": MetadataValue.text(str(e)),
            },
        )
    if not isinstance(gen.parsed_text, str) or not gen.parsed_text:
        raise Failure(
            description="Missing or unreadable parsed.txt for target essay document",
            metadata={
                "function": MetadataValue.text("evaluation_prompt"),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "essay_gen_dir": MetadataValue.path(str(base)),
            },
        )
    doc_text = gen.parsed_text
    used_source = "essay_gens"

    eval_df = read_evaluation_templates(Path(context.resources.data_root))
    evaluation_templates_dict: dict[str, str] = {}
    templates_base = Path(context.resources.data_root) / "1_raw" / "templates" / "evaluation"
    for _, row in eval_df.iterrows():
        template_id = row.get("template_id")
        if not isinstance(template_id, str) or not len(template_id):
            continue
        fp = templates_base / f"{template_id}.txt"
        try:
            evaluation_templates_dict[template_id] = fp.read_text(encoding="utf-8")
        except FileNotFoundError:
            context.log.warning(f"Evaluation template file not found: {fp}")

    template_id = str(evaluation_template)
    template_content = evaluation_templates_dict[template_id]
    env = Environment()
    template = env.from_string(template_content)
    eval_prompt = template.render(response=doc_text)
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
    mrow, _cohort = find_membership_row_by_gen(data_root, "evaluation", str(gen_id))
    if mrow is None:
        raise Failure(
            description="Cohort membership row not found for evaluation gen_id",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Materialize cohort_id,cohort_membership to register this gen_id; use that partition key"),
            },
        )
    model_name = str(mrow.get("llm_model_id") or "").strip()
    if not model_name:
        raise Failure(
            description="Missing evaluator model for evaluation task",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure evaluation row in cohort membership includes an llm_model_id"),
            },
        )
    evaluation_template = str(mrow.get("template_id") or "").strip()
    if not evaluation_template:
        raise Failure(
            description="Missing evaluation template for evaluation task",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
            },
        )
    parent_gen_id = str(mrow.get("parent_gen_id") or "").strip()
    if not parent_gen_id:
        raise Failure(
            description="Missing parent_gen_id for evaluation task",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
            },
        )
    # Load target essay parsed content for prompt values
    gens_root = data_root / "gens"
    try:
        gen = Generation.load(gens_root, ESSAY, parent_gen_id)
    except Exception as e:
        raise Failure(
            description="Target essay document not found",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "error": MetadataValue.text(str(e)),
            },
        )
    if not isinstance(gen.parsed_text, str) or not gen.parsed_text:
        raise Failure(
            description="Missing or unreadable parsed.txt for target essay document",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
            },
        )

    # Resolve parser for evaluation
    parser_map = load_parser_map(data_root)
    parser_name = require_parser_for_template(evaluation_template, parser_map)

    # Build runner spec
    runner = StageRunner()
    spec = StageRunSpec(
        stage="evaluation",
        gen_id=str(gen_id),
        template_id=evaluation_template,
        values={"response": gen.parsed_text},
        out_dir=data_root / "gens",
        mode="llm",
        model=model_name,
        parser_name=parser_name,
        max_tokens=getattr(context.resources.experiment_config, "evaluation_max_tokens", None),
    )
    result = runner.run(spec, llm_client=context.resources.openrouter_client)

    # Emit minimal metadata for Dagster UI
    context.add_output_metadata(
        {
            "gen_id": MetadataValue.text(str(gen_id)),
            "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
            "finish_reason": MetadataValue.text(str(result.get("metadata", {}).get("finish_reason"))),
        }
    )
    context.log.info(f"Generated evaluation response for gen {gen_id}")

    return result.get("raw") or ""
