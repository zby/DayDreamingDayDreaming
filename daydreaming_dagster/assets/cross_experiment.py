"""Cross-experiment analysis assets using gens-store as the source of truth.

Reads evaluation results from `data/gens/evaluation/<gen_id>/{parsed.txt,raw.txt,metadata.json}`
and enriches with generation metadata from parent essay/draft documents.
"""

from dagster import asset, AssetIn, MetadataValue, Config
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import json

from ..utils.evaluation_processing import calculate_evaluation_metadata
from ..utils.evaluation_parsing_config import load_parser_map, require_parser_for_template
from ..utils.eval_response_parser import parse_llm_response
from ..constants import ESSAY, EVALUATION, FILE_PARSED, FILE_RAW, FILE_METADATA

class FilteredEvaluationResultsConfig(Config):
    """Configuration for filtered evaluation results."""
    # For now, no config needed - always uses default filter
    # TODO: Add filtering configuration options later


@asset(
    group_name="cross_experiment",
    io_manager_key="cross_experiment_io_manager",
    description="Scan gens-store evaluation outputs across experiments and parse scores",
    compute_kind="pandas",
    required_resource_keys={"data_root"},
)
def filtered_evaluation_results(context, config: FilteredEvaluationResultsConfig) -> pd.DataFrame:
    """Collect evaluation results from gens-store and enrich with generation metadata.

    For each directory under `data/gens/evaluation/<gen_id>`:
      - Prefer numeric score from parsed.txt; otherwise parse raw.txt using CSV-driven strategy
      - Read evaluation metadata (template_id/model_id/parent_gen_id)
      - Enrich with essay and draft metadata (generation_template/model, combo_id)
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    eval_root = data_root / "gens" / EVALUATION
    if not eval_root.exists():
        context.add_output_metadata({
            "total_responses": MetadataValue.int(0),
            "base_path": MetadataValue.path(str(eval_root)),
        })
        return pd.DataFrame(columns=[
            "gen_id", "score", "error", "evaluation_template", "evaluation_model",
            "combo_id", "generation_template", "generation_model",
        ])

    # Load parsing strategies (evaluation_templates.csv)
    try:
        parser_map = load_parser_map(data_root)
    except Exception as e:
        parser_map = {}
        context.log.warning(f"Could not load evaluation parser map: {e}")

    rows: list[dict] = []
    for gen_dir in sorted([p for p in eval_root.iterdir() if p.is_dir()]):
        gen_id = gen_dir.name
        md = {}
        try:
            mpath = gen_dir / FILE_METADATA
            if mpath.exists():
                md = json.loads(mpath.read_text(encoding="utf-8")) or {}
        except Exception:
            md = {}
        evaluation_template = str(md.get("template_id") or md.get("evaluation_template") or "")
        evaluation_model = str(md.get("model_id") or md.get("evaluation_model") or "")
        parent_essay_id = str(md.get("parent_gen_id") or "")

        # Default enrichment
        combo_id = ""
        generation_template = ""
        generation_model = ""
        parent_draft_id = ""
        # Essay metadata
        try:
            if parent_essay_id:
                emeta_path = data_root / "gens" / ESSAY / parent_essay_id / FILE_METADATA
                if emeta_path.exists():
                    emd = json.loads(emeta_path.read_text(encoding="utf-8")) or {}
                    generation_template = str(emd.get("template_id") or emd.get("essay_template") or "")
                    generation_model = str(emd.get("model_id") or "")
                    parent_draft_id = str(emd.get("parent_gen_id") or "")
        except Exception:
            pass
        # Draft metadata
        try:
            if parent_draft_id:
                dmeta_path = data_root / "gens" / "draft" / parent_draft_id / FILE_METADATA
                if dmeta_path.exists():
                    dmd = json.loads(dmeta_path.read_text(encoding="utf-8")) or {}
                    combo_id = str(dmd.get("combo_id") or "")
                    if not generation_model:
                        generation_model = str(dmd.get("model_id") or "")
        except Exception:
            pass

        # Score parsing
        score = None
        error = None
        parsed_fp = gen_dir / FILE_PARSED
        raw_fp = gen_dir / FILE_RAW
        if parsed_fp.exists():
            try:
                # Expect numeric-only line
                txt = parsed_fp.read_text(encoding="utf-8").strip()
                score = float(txt.splitlines()[-1].strip()) if txt else None
            except Exception as e:
                error = f"Invalid parsed.txt: {e}"
        elif raw_fp.exists():
            try:
                strategy = require_parser_for_template(evaluation_template, parser_map)
                res = parse_llm_response(raw_fp.read_text(encoding="utf-8"), strategy)
                score = res.get("score")
                error = res.get("error")
            except Exception as e:
                error = f"Parse error: {e}"
        else:
            error = "Missing raw.txt and parsed.txt"

        rows.append({
            "gen_id": gen_id,
            "score": score,
            "error": error,
            "evaluation_template": evaluation_template,
            "evaluation_model": evaluation_model,
            "combo_id": combo_id,
            "generation_template": generation_template,
            "generation_model": generation_model,
        })

    df = pd.DataFrame(rows)
    # Add metadata
    metadata = calculate_evaluation_metadata(df)
    metadata.update({
        "total_responses": MetadataValue.int(len(df)),
        "base_path": MetadataValue.path(str(eval_root)),
    })
    context.add_output_metadata(metadata)
    return df


class TemplateComparisonConfig(Config):
    """Configuration for template version comparison."""
    template_versions: list = None  # If None, uses all available templates


@asset(
    group_name="cross_experiment",
    io_manager_key="cross_experiment_io_manager",
    ins={"filtered_evaluation_results": AssetIn()},
    description="Create pivot table comparing template versions from filtered results",
    compute_kind="pandas"
)
def template_version_comparison_pivot(
    context, 
    filtered_evaluation_results: pd.DataFrame,
    config: TemplateComparisonConfig
) -> pd.DataFrame:
    """Create pivot table for template version comparison from filtered results."""
    
    df = filtered_evaluation_results.copy()
    
    # If no specific template versions specified, use all available in filtered results
    template_versions = config.template_versions
    if template_versions is None:
        template_versions = df['evaluation_template'].unique().tolist()
    
    # Filter to specified template versions
    filtered_df = df[df['evaluation_template'].isin(template_versions)]
    
    # Create pivot table for comparison
    pivot_df = filtered_df.pivot_table(
        index=['combo_id', 'generation_template', 'generation_model'],
        columns='evaluation_template',
        values='score',
        aggfunc='first'
    ).reset_index()
    
    # Add metadata
    context.add_output_metadata({
        "template_versions_compared": MetadataValue.json(template_versions),
        "pivot_table_rows": MetadataValue.int(len(pivot_df)),
        "pivot_table_columns": MetadataValue.int(len(pivot_df.columns)),
        "source_filtered_results": MetadataValue.int(len(filtered_df))
    })
    
    return pivot_df


# (Auto-materializing tracking assets removed. Derive cross-experiment views from the gens store and cohort membership.)
