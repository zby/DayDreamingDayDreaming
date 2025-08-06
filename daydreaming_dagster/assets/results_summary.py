from dagster import asset, MetadataValue
import pandas as pd
import numpy as np


@asset(
    group_name="results_processing", 
    io_manager_key="summary_results_io_manager"
)
def final_results(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Create comprehensive pivot table summaries with statistics.
    Includes average scores, perfect scores count, and standard deviation.
    """
    # Filter out rows with errors (no valid scores)
    valid_scores = parsed_scores[parsed_scores['error'].isna() & parsed_scores['score'].notna()].copy()
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
        analysis_df, ['evaluation_model'], 'by_evaluation_model'
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
        analysis_df, ['generation_model', 'evaluation_model'], 'by_generation_vs_evaluation_model'
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
            'evaluation_model',
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


@asset(
    group_name="results_processing",
    io_manager_key="summary_results_io_manager"
)
def perfect_score_paths(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a file with paths to all responses that received perfect scores (10.0).
    Includes both generation and evaluation response paths for analysis.
    """
    # Filter for perfect scores only
    perfect_scores = parsed_scores[
        (parsed_scores['score'] == 10.0) & 
        (parsed_scores['error'].isna())
    ].copy()
    
    if perfect_scores.empty:
        context.log.warning("No perfect scores found")
        return pd.DataFrame(columns=[
            'combo_id', 'generation_template', 'generation_model', 
            'evaluation_model', 'score',
            'generation_response_path', 'evaluation_response_path'
        ])
    
    # Reconstruct file paths for each perfect score
    paths_data = []
    
    for _, row in perfect_scores.iterrows():
        # Reconstruct generation response path - flat file structure
        # Format: data/3_generation/generation_responses/combo_X_template_model.txt
        generation_path = f"data/3_generation/generation_responses/{row['combo_id']}_{row['generation_template']}_{row['generation_model']}.txt"
        
        # Reconstruct evaluation response path - flat file structure
        # Format: data/4_evaluation/evaluation_responses/combo_X_template_model_eval_template_eval_model.txt
        evaluation_path = f"data/4_evaluation/evaluation_responses/{row['combo_id']}_{row['generation_template']}_{row['generation_model']}_{row.get('evaluation_template', 'daydreaming-verification-v2')}_{row['evaluation_model']}.txt"
        
        paths_data.append({
            'combo_id': row['combo_id'],
            'generation_template': row['generation_template'],
            'generation_model': row['generation_model'],
            'evaluation_model': row['evaluation_model'],
            'score': row['score'],
            'generation_response_path': generation_path,
            'evaluation_response_path': evaluation_path,
            'notes': f"Perfect score from {row['generation_model']} generation + {row['evaluation_model']} evaluation"
        })
    
    result_df = pd.DataFrame(paths_data)
    
    context.log.info(f"Found {len(result_df)} perfect score responses")
    context.log.info(f"Perfect scores by evaluator: {perfect_scores['evaluation_model'].value_counts().to_dict()}")
    context.log.info(f"Perfect scores by template: {perfect_scores['generation_template'].value_counts().to_dict()}")
    
    # Add output metadata
    evaluator_counts = perfect_scores['evaluation_model'].value_counts().to_dict() if not perfect_scores.empty else {}
    template_counts = perfect_scores['generation_template'].value_counts().to_dict() if not perfect_scores.empty else {}
    
    context.add_output_metadata({
        "perfect_scores": MetadataValue.int(len(result_df)),
        "unique_combinations": MetadataValue.int(result_df['combo_id'].nunique() if 'combo_id' in result_df.columns else 0),
        "unique_templates": MetadataValue.int(result_df['generation_template'].nunique() if 'generation_template' in result_df.columns else 0),
        "unique_evaluators": MetadataValue.int(result_df['evaluation_model'].nunique() if 'evaluation_model' in result_df.columns else 0),
        "deepseek_perfect": MetadataValue.int(evaluator_counts.get('deepseek', 0)),
        "qwen_perfect": MetadataValue.int(evaluator_counts.get('qwen', 0)),
        "google_perfect": MetadataValue.int(evaluator_counts.get('google', 0)),
        "top_template": MetadataValue.text(max(template_counts.keys(), key=template_counts.get) if template_counts else "None")
    })
    
    return result_df