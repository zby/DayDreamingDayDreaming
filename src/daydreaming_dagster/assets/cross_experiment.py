"""Cross-experiment analysis assets using gens-store as the source of truth.

Reads evaluation results from `data/gens/evaluation/<gen_id>/{parsed.txt,raw.txt,metadata.json}`
and enriches with generation metadata from parent essay/draft documents.
"""

from dagster import AssetIn, MetadataValue, Config
from ._decorators import asset_with_boundary
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import json

from ..utils.evaluation_processing import calculate_evaluation_metadata
from ..config.paths import Paths
from ..utils.generation import load_generation

class FilteredEvaluationResultsConfig(Config):
    """Configuration for filtered evaluation results."""
    # For now, no config needed - always uses default filter
    # TODO: Add filtering configuration options later


@asset_with_boundary(
    stage="cross_experiment",
    group_name="cross_experiment",
    io_manager_key="cross_experiment_io_manager",
    description="Scan gens-store evaluation outputs across experiments and parse scores",
    compute_kind="pandas",
    required_resource_keys={"data_root"},
)
def filtered_evaluation_results(context, config: FilteredEvaluationResultsConfig) -> pd.DataFrame:
    """Collect evaluation results from gens-store and enrich with generation metadata.

    For each directory under `data/gens/evaluation/<gen_id>`:
      - Read numeric score strictly from parsed.txt
      - Read evaluation metadata (template_id/model_id/parent_gen_id)
      - Enrich with essay and draft metadata (generation_template/model, combo_id)
    """
    paths = Paths.from_context(context)
    gens_root = paths.gens_root
    eval_root = gens_root / "evaluation"
    # Let filesystem errors surface naturally if eval_root is missing

    rows: list[dict] = []
    for gen_dir in sorted([p for p in eval_root.iterdir() if p.is_dir()]):
        gen_id = gen_dir.name
        # Load evaluation generation via shared helper
        eval_doc = load_generation(gens_root, "evaluation", gen_id)
        md = eval_doc.get("metadata") or {}
        evaluation_template = str(md.get("template_id") or md.get("evaluation_template") or "")
        evaluation_model = str(md.get("model_id") or md.get("evaluation_model") or "")
        parent_essay_id = str(md.get("parent_gen_id") or "")

        # Default enrichment
        combo_id = ""
        generation_template = ""
        generation_model = ""
        parent_draft_id = ""
        # Essay metadata
        if parent_essay_id:
            emd = (load_generation(gens_root, "essay", parent_essay_id).get("metadata") or {})
            generation_template = str(emd.get("template_id") or emd.get("essay_template") or "")
            generation_model = str(emd.get("model_id") or "")
            parent_draft_id = str(emd.get("parent_gen_id") or "")
        # Draft metadata
        if parent_draft_id:
            dmeta_path = paths.metadata_path("draft", parent_draft_id)
            if dmeta_path.exists():
                dmd = json.loads(dmeta_path.read_text(encoding="utf-8")) or {}
                combo_id = str(dmd.get("combo_id") or "")
                if not generation_model:
                    generation_model = str(dmd.get("model_id") or "")

        # Score parsing
        score = None
        error = None
        parsed_txt = eval_doc.get("parsed_text")
        if isinstance(parsed_txt, str):
            score = float(parsed_txt.strip()) if parsed_txt.strip() else None
        else:
            error = "Missing parsed.txt"

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


@asset_with_boundary(
    stage="cross_experiment",
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
