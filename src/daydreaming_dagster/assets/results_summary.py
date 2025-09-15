from dagster import MetadataValue
from ._decorators import asset_with_boundary
from ._decorators import asset_with_boundary
import pandas as pd
import numpy as np
from pathlib import Path
from ..config.paths import Paths
from .raw_data import EVALUATION_TEMPLATES_KEY
from ..utils.raw_readers import read_templates
from ..utils.evaluation_processing import filter_valid_scores
@asset_with_boundary(
    stage="results_summary",
    group_name="results_processing",
    io_manager_key="summary_results_io_manager",
    required_resource_keys={"data_root"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def generation_scores_pivot(context, aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot individual evaluation scores per generation.

    Rows: combo_id, generation_template, generation_model
    Columns: Each unique (evaluation_template, evaluation_llm_model) combination
    Values: Individual score for that specific evaluator combination (no averaging)
    """
    # Load evaluation templates CSV and extract active templates
    eval_df = read_templates(Paths.from_context(context).data_root, "evaluation", filter_active=True)
    if eval_df is None or eval_df.empty:
        context.log.warning("No evaluation templates CSV found or empty; returning empty pivot")
        return pd.DataFrame()
    active_templates = eval_df[eval_df.get('active', True) == True]['template_id'].tolist()
    
    if not active_templates:
        context.log.warning("No active evaluation templates found; returning empty pivot")
        return pd.DataFrame()
    
    # Filter to valid scored rows
    if aggregated_scores is None or aggregated_scores.empty:
        context.log.warning("No aggregated_scores provided; returning empty pivot")
        return pd.DataFrame()

    valid_scores = filter_valid_scores(aggregated_scores)

    if valid_scores.empty:
        context.log.warning("No valid scores found; returning empty pivot")
        return pd.DataFrame()

    # Require evaluator id column and compose combined key (strict)
    if 'evaluation_llm_model' not in valid_scores.columns:
        raise ValueError("Missing required column 'evaluation_llm_model' in aggregated_scores")
    # Unified convention: model_template
    valid_scores['eval_model_template'] = (
        valid_scores['evaluation_llm_model'] + '_' + valid_scores['evaluation_template']
    )

    # Build pivots: mean (baseline, keeps original column names), plus min/max/count for stability
    index_cols = ['combo_id', 'draft_template', 'generation_template', 'generation_model']
    pivot_mean = valid_scores.pivot_table(
        index=index_cols,
        columns='eval_model_template',
        values='score',
        aggfunc='mean'
    ).round(2)
    pivot_min = valid_scores.pivot_table(
        index=index_cols,
        columns='eval_model_template',
        values='score',
        aggfunc='min'
    ).round(2)
    pivot_max = valid_scores.pivot_table(
        index=index_cols,
        columns='eval_model_template',
        values='score',
        aggfunc='max'
    ).round(2)
    pivot_cnt = valid_scores.pivot_table(
        index=index_cols,
        columns='eval_model_template',
        values='score',
        aggfunc='count'
    ).astype(int)

    # Flatten and suffix stability columns, keep mean columns unchanged
    def _suffix_cols(df, suffix):
        df = df.copy()
        df.columns = [f"{c}{suffix}" for c in df.columns]
        return df

    pivot_df = pivot_mean.reset_index()
    # Track mean columns for sum_scores later
    mean_eval_cols = [c for c in pivot_df.columns if c not in index_cols]

    # Join min/max/count
    pv_min = _suffix_cols(pivot_min, "_min").reset_index()
    pv_max = _suffix_cols(pivot_max, "_max").reset_index()
    pv_cnt = _suffix_cols(pivot_cnt, "_n").reset_index()

    # Merge on index columns in order
    for extra in (pv_min, pv_max, pv_cnt):
        pivot_df = pivot_df.merge(extra, on=index_cols, how='left')

    # Flatten and reset index
    pivot_df = pivot_df.reset_index()

    # Get mean evaluation columns (exclude index and suffixed stability columns)
    eval_columns = [col for col in mean_eval_cols if col in pivot_df.columns]
    
    # Add aggregate column with all scores summed across evaluators
    # NaNs are ignored in the sum; round for readability
    if eval_columns:
        pivot_df['sum_scores'] = pivot_df[eval_columns].sum(axis=1, skipna=True).round(2)
    else:
        pivot_df['sum_scores'] = 0.0

    # Require draft_template in aggregated_scores (added with two-phase architecture)
    if 'draft_template' not in valid_scores.columns:
        raise ValueError("Missing required column 'draft_template' in aggregated_scores for two-phase path generation")
    # draft_template is now already included in the pivot index, so no need to merge it back

    # Attach generation_response_path from aggregated_scores (strict requirement)
    if 'generation_response_path' not in aggregated_scores.columns:
        raise ValueError("Missing 'generation_response_path' in aggregated_scores")
    path_map = aggregated_scores[
        ['combo_id', 'draft_template', 'generation_template', 'generation_model', 'generation_response_path']
    ].drop_duplicates()
    pivot_df = pivot_df.merge(
        path_map,
        on=['combo_id', 'draft_template', 'generation_template', 'generation_model'],
        how='left'
    )
    
    # Order columns: index columns first, then evaluation columns
    # Place stability columns after the corresponding mean columns
    stability_cols = [c for c in pivot_df.columns if c.endswith('_min') or c.endswith('_max') or c.endswith('_n')]
    ordered_cols = index_cols + eval_columns + stability_cols + ['sum_scores', 'generation_response_path']
    pivot_df = pivot_df[ordered_cols]

    # Metadata
    context.add_output_metadata({
        "rows": MetadataValue.int(len(pivot_df)),
        "unique_generations": MetadataValue.int(pivot_df[['combo_id', 'draft_template', 'generation_template', 'generation_model']].drop_duplicates().shape[0]),
        "evaluation_combinations": MetadataValue.int(len(eval_columns)),
        "total_active_templates": MetadataValue.int(len(active_templates)),
        "evaluation_columns": MetadataValue.text(", ".join(eval_columns))
    })

    return pivot_df

@asset_with_boundary(
    stage="results_summary",
    group_name="results_summary", 
    io_manager_key="summary_results_io_manager"
)
def final_results(context, aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Create comprehensive pivot table summaries with statistics.
    Includes average scores, perfect scores count, and standard deviation.
    """
    # Filter out rows with errors (no valid scores)
    valid_scores = filter_valid_scores(aggregated_scores)
    score_col = 'score'
    analysis_df = valid_scores
    
    def create_pivot_summary(df, group_cols, name_prefix):
        """Create pivot summary with statistics"""
        if df.empty:
            return pd.DataFrame()
            
        grouped = df.groupby(group_cols)[score_col].agg([
            ('count', 'count'),
            ('average', 'mean'),
            ('std_dev', 'std'),
            ('min_score', 'min'),
            ('max_score', 'max'),
            ('perfect_scores', lambda x: (x == 10.0).sum()),
            ('high_scores_8plus', lambda x: (x >= 8.0).sum()),
            ('low_scores_3minus', lambda x: (x <= 3.0).sum())
        ]).round(2)
        
        # Add perfect score percentage
        grouped['perfect_score_pct'] = (grouped['perfect_scores'] / grouped['count'] * 100).round(1)
        grouped['high_score_pct'] = (grouped['high_scores_8plus'] / grouped['count'] * 100).round(1)
        
        # Reset index to make group columns regular columns
        result = grouped.reset_index()
        
        # Add analysis category
        result['analysis_type'] = name_prefix
        
        return result
    
    # Create different pivot analyses
    summaries = []
    
    # 1. By Generation Template
    template_summary = create_pivot_summary(
        analysis_df, ['generation_template'], 'by_generation_template'
    )
    summaries.append(template_summary)
    
    # 2. By Generation Model Provider
    gen_model_summary = create_pivot_summary(
        analysis_df, ['generation_model'], 'by_generation_model'
    )
    summaries.append(gen_model_summary)
    
    # 3. By Evaluation Model Provider
    eval_model_summary = create_pivot_summary(
        analysis_df, ['evaluation_llm_model'], 'by_evaluation_model'
    )
    summaries.append(eval_model_summary)
    
    # 4. By Combo ID
    combo_summary = create_pivot_summary(
        analysis_df, ['combo_id'], 'by_combo_id'
    )
    summaries.append(combo_summary)
    
    # 5. By Template + Generation Model combination
    template_model_summary = create_pivot_summary(
        analysis_df, ['generation_template', 'generation_model'], 'by_template_and_generation_model'
    )
    summaries.append(template_model_summary)
    
    # 6. By Generation Model + Evaluation Model combination
    gen_eval_model_summary = create_pivot_summary(
        analysis_df, ['generation_model', 'evaluation_llm_model'], 'by_generation_vs_evaluation_model'
    )
    summaries.append(gen_eval_model_summary)
    
    # 7. Overall statistics
    if not valid_scores.empty:
        overall_stats = pd.DataFrame([{
            'analysis_type': 'overall_statistics',
            'count': len(valid_scores),
            'average': valid_scores['score'].mean(),
            'std_dev': valid_scores['score'].std(),
            'min_score': valid_scores['score'].min(),
            'max_score': valid_scores['score'].max(),
            'perfect_scores': (valid_scores['score'] == 10.0).sum(),
            'high_scores_8plus': (valid_scores['score'] >= 8.0).sum(),
            'low_scores_3minus': (valid_scores['score'] <= 3.0).sum(),
            'perfect_score_pct': ((valid_scores['score'] == 10.0).sum() / len(valid_scores) * 100),
            'high_score_pct': ((valid_scores['score'] >= 8.0).sum() / len(valid_scores) * 100)
        }]).round(2)
        summaries.append(overall_stats)
    
    # Combine all summaries
    if summaries:
        final_summary = pd.concat(summaries, ignore_index=True)
        
        # Reorder columns for better readability
        column_order = [
            'analysis_type',
            'generation_template', 
            'generation_model',
            'evaluation_llm_model',
            'combo_id',
            'count',
            'average',
            'std_dev',
            'min_score',
            'max_score',
            'perfect_scores',
            'perfect_score_pct',
            'high_scores_8plus',
            'high_score_pct',
            'low_scores_3minus'
        ]
        
        # Only keep columns that exist
        existing_columns = [col for col in column_order if col in final_summary.columns]
        final_summary = final_summary[existing_columns]
        
        context.log.info(f"Created comprehensive analysis with {len(final_summary)} summary rows from {len(valid_scores)} valid scores")
        
        # Add output metadata
        analysis_type_counts = final_summary['analysis_type'].value_counts().to_dict() if 'analysis_type' in final_summary.columns else {}
        
        context.add_output_metadata({
            "summary_rows": MetadataValue.int(len(final_summary)),
            "source_evaluations": MetadataValue.int(len(valid_scores)),
            "analysis_categories": MetadataValue.int(len(analysis_type_counts)),
            "by_template": MetadataValue.int(analysis_type_counts.get('by_generation_template', 0)),
            "by_model": MetadataValue.int(analysis_type_counts.get('by_generation_model', 0)),
            "by_combo": MetadataValue.int(analysis_type_counts.get('by_combo_id', 0)),
            "overall_stats": MetadataValue.int(analysis_type_counts.get('overall_statistics', 0)),
            "columns_included": MetadataValue.text(", ".join(existing_columns))
        })
        
        return final_summary
    else:
        context.log.warning("No valid scores found for analysis")
        
        # Add output metadata for empty result
        context.add_output_metadata({
            "summary_rows": MetadataValue.int(0),
            "source_evaluations": MetadataValue.int(0),
            "analysis_result": MetadataValue.text("No valid scores found for analysis")
        })
        
        return pd.DataFrame()


@asset_with_boundary(
    stage="results_summary",
    group_name="results_summary", 
    io_manager_key="summary_results_io_manager"
)
def perfect_score_paths(context, aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a file with paths to all responses that received perfect scores (10.0).
    Includes both generation and evaluation response paths for analysis.
    """
    # Filter for perfect scores only (use centralized filter first)
    valid = filter_valid_scores(aggregated_scores)
    perfect_scores = valid[valid['score'] == 10.0].copy()
    
    if perfect_scores.empty:
        context.log.warning("No perfect scores found")
        return pd.DataFrame(columns=[
            'combo_id', 'generation_template', 'generation_model', 
            'evaluation_llm_model', 'score',
            'generation_response_path', 'evaluation_response_path'
        ])
    
    # Require expected columns from the aggregator output
    required_cols = {
        'combo_id', 'generation_template', 'generation_model', 'evaluation_llm_model',
        'score', 'generation_response_path', 'evaluation_response_path'
    }
    missing = [c for c in required_cols if c not in perfect_scores.columns]
    if missing:
        raise ValueError(f"aggregated_scores missing required columns: {missing}")

    result_df = perfect_scores[[
        'combo_id', 'generation_template', 'generation_model',
        'evaluation_llm_model', 'score',
        'generation_response_path', 'evaluation_response_path'
    ]].copy()
    # Optional notes for human readers
    result_df['notes'] = (
        "Perfect score from " + result_df['generation_model'] + " generation + " + result_df['evaluation_llm_model'] + " evaluation"
    )
    
    context.log.info(f"Found {len(result_df)} perfect score responses")
    context.log.info(f"Perfect scores by evaluator: {perfect_scores['evaluation_llm_model'].value_counts().to_dict()}")
    context.log.info(f"Perfect scores by template: {perfect_scores['generation_template'].value_counts().to_dict()}")
    
    # Add output metadata
    evaluator_counts = perfect_scores['evaluation_llm_model'].value_counts().to_dict() if not perfect_scores.empty else {}
    template_counts = perfect_scores['generation_template'].value_counts().to_dict() if not perfect_scores.empty else {}
    
    context.add_output_metadata({
        "perfect_scores": MetadataValue.int(len(result_df)),
        "unique_combinations": MetadataValue.int(result_df['combo_id'].nunique() if 'combo_id' in result_df.columns else 0),
        "unique_templates": MetadataValue.int(result_df['generation_template'].nunique() if 'generation_template' in result_df.columns else 0),
        "unique_evaluators": MetadataValue.int(result_df['evaluation_llm_model'].nunique() if 'evaluation_llm_model' in result_df.columns else 0),
        "deepseek_perfect": MetadataValue.int(evaluator_counts.get('deepseek', 0)),
        "qwen_perfect": MetadataValue.int(evaluator_counts.get('qwen', 0)),
        "google_perfect": MetadataValue.int(evaluator_counts.get('google', 0)),
        "top_template": MetadataValue.text(max(template_counts.keys(), key=template_counts.get) if template_counts else "None")
    })
    
    return result_df


@asset_with_boundary(
    stage="results_summary", 
    group_name="results_summary", 
    io_manager_key="summary_results_io_manager"
)
def evaluation_model_template_pivot(context, aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Create pivot table with (evaluation_llm_model, evaluation_template) combinations as columns.
    
    Rows: Each generation response (combo_id, generation_template, generation_model)
    Columns: Each (evaluation_llm_model, evaluation_template) combination
    Values: Score for that generation evaluated by that (model, template) combination
    
    This table enables easy comparison of how different evaluation approaches
    score the same generation responses.
    """
    if aggregated_scores is None or aggregated_scores.empty:
        context.log.warning("No aggregated_scores provided; returning empty pivot")
        return pd.DataFrame()
    
    # Filter to valid scored rows
    valid_scores = filter_valid_scores(aggregated_scores)
    
    if valid_scores.empty:
        context.log.warning("No valid scores found; returning empty pivot")
        return pd.DataFrame()
    
    # Require evaluator id and create combined key
    if 'evaluation_llm_model' not in valid_scores.columns:
        raise ValueError("Missing required column 'evaluation_llm_model' in aggregated_scores")
    valid_scores['eval_model_template'] = (
        valid_scores['evaluation_llm_model'] + '_' + valid_scores['evaluation_template']
    )
    
    # Create pivot table
    pivot_df = valid_scores.pivot_table(
        index=['combo_id', 'generation_template', 'generation_model'],
        columns='eval_model_template',
        values='score',
        aggfunc='mean'  # In case there are duplicates, take mean
    ).reset_index()
    
    # Fill NaN values with None for cleaner display
    pivot_df = pivot_df.fillna(np.nan)
    
    # Get column statistics
    eval_columns = [col for col in pivot_df.columns if col not in ['combo_id', 'generation_template', 'generation_model']]
    total_generations = len(pivot_df)
    coverage_stats = {}
    
    for col in eval_columns:
        non_null_count = int(pivot_df[col].count())  # Convert to Python int
        coverage_pct = (non_null_count / total_generations * 100) if total_generations > 0 else 0
        mean_score = pivot_df[col].mean()
        coverage_stats[col] = {
            'evaluations': non_null_count,
            'coverage_pct': round(float(coverage_pct), 1),  # Convert to Python float
            'mean_score': round(float(mean_score), 2) if non_null_count > 0 and not pd.isna(mean_score) else None
        }
    
    context.log.info(f"Created pivot table with {total_generations} generation responses")
    context.log.info(f"Evaluation combinations: {len(eval_columns)}")
    
    # Add metadata
    context.add_output_metadata({
        "total_generation_responses": MetadataValue.int(total_generations),
        "evaluation_combinations": MetadataValue.int(len(eval_columns)),
        "unique_combos": MetadataValue.int(pivot_df['combo_id'].nunique()),
        "unique_generation_templates": MetadataValue.int(pivot_df['generation_template'].nunique()), 
        "unique_generation_models": MetadataValue.int(pivot_df['generation_model'].nunique()),
        "evaluation_coverage": MetadataValue.json(coverage_stats)
    })
    
    return pivot_df
