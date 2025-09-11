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
from ..utils.eval_response_parser import parse_llm_response
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
    mrow, _cohort = find_membership_row_by_gen(getattr(context.resources, "data_root", "data"), "evaluation", str(gen_id))
    model_name = str(mrow.get("llm_model_id") or mrow.get("evaluation_model") or "").strip() if mrow is not None else ""
    if not model_name:
        raise Failure(
            description="Missing evaluator model for evaluation task",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure evaluation row in cohort membership includes an llm_model_id"),
            },
        )
    llm_client = context.resources.openrouter_client
    max_tokens = getattr(context.resources.experiment_config, "evaluation_max_tokens", None)
    text, info = llm_client.generate_with_info(evaluation_prompt, model=model_name, max_tokens=max_tokens)
    context.add_output_metadata({
        "gen_id": MetadataValue.text(str(gen_id)),
        "finish_reason": MetadataValue.text(str((info or {}).get("finish_reason"))),
    })
    context.log.info(f"Generated evaluation response for gen {gen_id}")
    # Write to filesystem gens/evaluation
    import time
    from pathlib import Path as _Path
    parent_gen_id = mrow.get("parent_gen_id") if mrow is not None else None
    evaluation_template = mrow.get("template_id") if mrow is not None else None
    model_id = str(mrow.get("llm_model_id") or mrow.get("evaluation_model") or "") if mrow is not None else None
    if not (isinstance(gen_id, str) and gen_id.strip()):
        raise Failure(
            description="Missing gen_id for evaluation task",
            metadata={
                "function": MetadataValue.text("evaluation_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Partition key must be a valid evaluation gen_id from cohort membership"),
            },
        )
    docs_root = _Path(getattr(context.resources, "data_root", "data")) / "gens"
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
    run_id = getattr(getattr(context, "run", object()), "run_id", None) or getattr(context, "run_id", None)
    metadata = build_generation_metadata(
        stage=EVALUATION,
        gen_id=str(gen_id),
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        template_id=str(evaluation_template) if evaluation_template else None,
        model_id=str(model_id) if model_id else None,
        task_id=str(""),
        function="evaluation_response",
        run_id=str(run_id) if run_id else None,
        cohort_id=str(_cohort) if isinstance(_cohort, str) and _cohort else None,
        extra={
            "evaluation_template": evaluation_template,
        },
    )
    prompt_text = evaluation_prompt if isinstance(evaluation_prompt, str) else None
    doc = Generation(
        stage=EVALUATION,
        gen_id=gen_id,
        parent_gen_id=parent_gen_id,
        raw_text=normalized,
        parsed_text=parsed_out,  # do not write parsed.txt when parsing fails
        prompt_text=prompt_text,
        metadata=metadata,
    )
    target_dir = doc.write_files(docs_root)
    context.add_output_metadata(
        {
            "gen_id": MetadataValue.text(gen_id),
            "gen_dir": MetadataValue.path(str(target_dir)),
            "parent_gen_id": MetadataValue.text(str(parent_gen_id) if parent_gen_id else ""),
        }
    )

    return text
