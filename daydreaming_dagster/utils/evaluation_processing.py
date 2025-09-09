"""Utilities for processing evaluation response files and enriching with metadata (strict)."""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from dagster import MetadataValue

from .eval_response_parser import parse_llm_response
from .evaluation_parsing_config import ALLOWED_PARSERS, require_parser_for_template, load_parser_map


def parse_eval_files_with_parsers(
    evaluation_tasks: pd.DataFrame,
    base_path: Path,
    parser_map: dict[str, str],
    context=None,
) -> pd.DataFrame:
    """Parse evaluation responses using an explicit parser map (template_id -> parser).

    Strict: no fallbacks. Raises if a template is missing or invalid in parser_map.

    Returns a DataFrame with columns: evaluation_task_id, score, error, used_parser, used_response_path
    """
    if evaluation_tasks.empty:
        return pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
    
    def _parse_with_table(text: str, task_row: pd.Series) -> Dict[str, Any]:
        tpl = task_row.get('evaluation_template')
        chosen = require_parser_for_template(str(tpl) if tpl is not None else "", parser_map)
        out = parse_llm_response(text, chosen)
        return out | {"used_parser": chosen}
    
    parsed_results = []
    from .versioned_files import latest_versioned_path
    total_tasks = len(evaluation_tasks)
    
    for i, (_, task_row) in enumerate(evaluation_tasks.iterrows()):
        if context and i > 0 and i % 100 == 0:
            context.log.info(f"Processed {i}/{total_tasks} evaluation tasks")
            
        evaluation_task_id = task_row['evaluation_task_id']
        latest = latest_versioned_path(base_path, evaluation_task_id, ".txt")
        response_file = latest or (base_path / f"{evaluation_task_id}.txt")
        
        try:
            if not response_file.exists():
                parsed_results.append({
                    "evaluation_task_id": evaluation_task_id,
                    "score": None,
                    "error": "File not found",
                    "used_response_path": str(base_path / f"{evaluation_task_id}.txt"),
                })
                continue
                
            response_text = response_file.read_text()
            result = _parse_with_table(response_text, task_row)
            
            parsed_results.append({
                "evaluation_task_id": evaluation_task_id,
                "score": result.get("score"),
                "error": result.get("error"),
                "used_parser": result.get("used_parser"),
                "used_response_path": str(response_file),
            })
            
        except Exception as e:
            parsed_results.append({
                "evaluation_task_id": evaluation_task_id,
                "score": None,
                "error": f"Parse error: {str(e)}",
                "used_response_path": str(response_file),
            })
    
    return pd.DataFrame(parsed_results)


def parse_evaluation_files(
    evaluation_tasks: pd.DataFrame,
    base_path: Path,
    evaluation_templates: Optional[pd.DataFrame] = None,
    context=None,
) -> pd.DataFrame:
    """Thin wrapper that builds a parser map from evaluation_templates and delegates.

    Requires evaluation_templates with columns: template_id, parser.
    No CSV fallbacks are attempted.
    """
    if evaluation_tasks.empty:
        return pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
    if evaluation_templates is None or "template_id" not in evaluation_templates.columns or "parser" not in evaluation_templates.columns:
        raise ValueError("evaluation_templates must be provided and include 'template_id' and 'parser' columns")
    parser_map = (
        evaluation_templates[["template_id", "parser"]]
        .dropna(subset=["template_id", "parser"])  # ensure both present
        .astype({"template_id": str, "parser": str})
        .set_index("template_id")["parser"].str.strip().str.lower()
        .to_dict()
    )
    return parse_eval_files_with_parsers(evaluation_tasks, base_path, parser_map, context=context)


def parse_evaluation_file_from_filename(filename: str, base_path: Path, parser_map: dict[str, str]) -> Dict[str, Any]:
    """Parse a single evaluation file using filename-based parsing.
    
    This function is useful for cross-experiment analysis where task metadata
    may not be available, but filenames contain all necessary information.
    
    Args:
        filename: Filename without extension (e.g., 'combo_001_creative-synthesis-v2_deepseek_r1_f_style-coherence_deepseek_r1_f')
        base_path: Base directory containing the file
        
    Returns:
        Dictionary with parsed evaluation data
    """
    from .eval_response_parser import parse_llm_response
    
    # Parse identifiers from filename (similar to aggregate_scores.py logic)
    parts = filename.split('_')
    
    # Find evaluation template and model (from right side)
    # Look for known template patterns
    known_templates = {
        'creative-synthesis', 'creative-synthesis-v2', 'creative-synthesis-v3',
        'essay-inventive-synthesis', 'essay-inventive-synthesis-v3',
        'research-discovery', 'research-discovery-v2', 'research-discovery-v3',
        'systematic-analytical', 'systematic-analytical-v2',
        'problem-solving', 'problem-solving-v2',
        'application-implementation', 'application-implementation-v2',
        'gwern-original',
        'daydreaming-verification', 'daydreaming-verification-v2',
        'o3-prior-art-eval', 'gemini-prior-art-eval', 
        'style-coherence', 'style-coherence-v2', 'style-coherence-v3',
        'creativity-metrics', 'iterative-loops', 'scientific-rigor'
    }
    
    evaluation_template = None
    evaluation_model = None
    
    # Find evaluation template and model from right side
    for i in range(len(parts) - 1, -1, -1):
        part = parts[i]
        if evaluation_template is None and part in known_templates:
            evaluation_template = part
        elif evaluation_template is not None and evaluation_model is None:
            evaluation_model = part
            break
    
    # Read and parse the file
    file_path = base_path / f"{filename}.txt"
    if not file_path.exists():
        return {
            "evaluation_task_id": filename,
            "score": None,
            "error": "File not found",
            "evaluation_template": evaluation_template,
            "evaluation_model": evaluation_model
        }
    
    try:
        response_text = file_path.read_text(encoding="utf-8", errors="ignore")
        parser = require_parser_for_template(evaluation_template or "", parser_map)
        result = parse_llm_response(response_text, parser)
        
        return {
            "evaluation_task_id": filename,
            "score": result.get("score"),
            "error": result.get("error"),
            "evaluation_template": evaluation_template,
            "evaluation_model": evaluation_model
        }
        
    except Exception as e:
        return {
            "evaluation_task_id": filename,
            "score": None,
            "error": f"Parse error: {str(e)}",
            "evaluation_template": evaluation_template,
            "evaluation_model": evaluation_model
        }


def parse_evaluation_files_cross_experiment(base_path: Path, context=None) -> pd.DataFrame:
    """Parse all evaluation files in a directory for cross-experiment analysis.
    
    This function is designed for cross-experiment analysis where task metadata
    may not be available, but filenames contain all necessary information.
    
    Args:
        base_path: Directory containing evaluation response files
        context: Optional Dagster context for logging
        
    Returns:
        DataFrame with parsed evaluation results from all files
    """
    if not base_path.exists() or not base_path.is_dir():
        return pd.DataFrame(columns=['evaluation_task_id', 'score', 'error', 'evaluation_template', 'evaluation_model'])
    
    # Get all .txt files
    txt_files = list(base_path.glob("*.txt"))
    if context:
        context.log.info(f"Found {len(txt_files)} evaluation files to parse")
    
    parsed_results = []
    # Strict parser map from data root inferred from responses path
    data_root = base_path.parents[1] if len(base_path.parents) >= 2 else Path("data")
    parser_map = load_parser_map(data_root)
    for i, file_path in enumerate(txt_files):
        if context and i > 0 and i % 100 == 0:
            context.log.info(f"Processed {i}/{len(txt_files)} evaluation files")
        
        # Parse file using filename-based approach
        filename = file_path.stem
        result = parse_evaluation_file_from_filename(filename, base_path, parser_map)
        parsed_results.append(result)
    
    return pd.DataFrame(parsed_results)


def enrich_evaluation_data(parsed_df: pd.DataFrame, evaluation_tasks: pd.DataFrame, 
                          essay_generation_tasks: pd.DataFrame, required_columns: List[str] = None) -> pd.DataFrame:
    """Enrich parsed evaluation data with task metadata via DataFrame joins.
    
    Args:
        parsed_df: DataFrame with parsed evaluation results
        evaluation_tasks: Evaluation task metadata
        essay_generation_tasks: Essay generation task metadata
        required_columns: List of columns to ensure exist (with 'unknown' defaults)
        
    Returns:
        Enriched DataFrame with all evaluation metadata
    """
    if parsed_df.empty:
        return parsed_df
        
    # Join with evaluation metadata
    enriched_df = parsed_df.merge(
        evaluation_tasks[['evaluation_task_id', 'essay_task_id', 'evaluation_template', 'evaluation_model']],
        on='evaluation_task_id',
        how='left'
    )
    
    # Join with essay generation metadata
    if not essay_generation_tasks.empty:
        enriched_df = enriched_df.merge(
            essay_generation_tasks[['essay_task_id', 'combo_id', 'essay_template', 'generation_model']],
            on='essay_task_id',
            how='left'
        )
    else:
        enriched_df['combo_id'] = 'unknown'
        enriched_df['essay_template'] = 'unknown'
        enriched_df['generation_model'] = 'unknown'
    
    # Ensure required columns exist
    if required_columns:
        for col in required_columns:
            if col not in enriched_df.columns:
                enriched_df[col] = 'unknown'
    
    return enriched_df


def calculate_evaluation_metadata(df: pd.DataFrame, score_column: str = 'score', error_column: str = 'error') -> Dict[str, Any]:
    """Calculate essential metadata for evaluation DataFrame assets.
    
    Args:
        df: DataFrame containing evaluation results
        score_column: Column name containing evaluation scores
        error_column: Column name containing error information
        
    Returns:
        Dictionary of evaluation metadata values
    """
    total_responses = len(df)
    successful_parses = len(df[df[error_column].isna()]) if error_column in df.columns else total_responses
    
    metadata = {
        "total_responses": MetadataValue.int(total_responses),
        "successful_parses": MetadataValue.int(successful_parses),
        "success_rate": MetadataValue.float(round((successful_parses / total_responses * 100), 2) if total_responses > 0 else 0.0),
    }
    
    # Add score statistics if available
    if score_column in df.columns and successful_parses > 0:
        valid_scores = df[df[error_column].isna()][score_column]
        if len(valid_scores) > 0:
            metadata.update({
                "avg_score": MetadataValue.float(round(float(valid_scores.mean()), 2)),
                "min_score": MetadataValue.float(round(float(valid_scores.min()), 2)),
                "max_score": MetadataValue.float(round(float(valid_scores.max()), 2)),
            })
    
    return metadata


def add_evaluation_file_paths(df: pd.DataFrame, base_path: str, filename_template: str) -> pd.DataFrame:
    """Add evaluation file path columns based on metadata in the DataFrame.
    
    Args:
        df: DataFrame to add paths to
        base_path: Base directory path for evaluation files
        filename_template: Template string for evaluation filename construction
        
    Returns:
        DataFrame with added file_path column
    """
    df_copy = df.copy()
    df_copy['file_path'] = df_copy.apply(
        lambda row: f"{base_path}/{filename_template.format(**row)}",
        axis=1
    )
    return df_copy
