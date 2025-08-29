from dagster import asset, MetadataValue
import pandas as pd
import numpy as np


@asset(
    group_name="results_summary",
    io_manager_key="summary_results_io_manager"
)
def evaluator_agreement_analysis(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate evaluator agreement metrics for the same generation responses.
    Groups evaluations by essay_task_id to analyze variance across:
    1. Multiple evaluation models (deepseek_r1_f vs qwq_32b_f)  
    2. Multiple evaluation templates (when available)
    
    This provides a comprehensive view of evaluation stability across both dimensions.
    """
    # Filter out rows with errors (no valid scores)
    valid_scores = parsed_scores[
        parsed_scores['error'].isna() & 
        parsed_scores['score'].notna()
    ].copy()
    
    if valid_scores.empty:
        context.log.warning("No valid scores found for evaluator agreement analysis")
        return pd.DataFrame()
    
    # Group by the combination that identifies the same generated response
    # This groups evaluations that scored the same generated content (same combo + generation method)
    generation_group_cols = ['combo_id', 'generation_template', 'generation_model']
    agreement_stats = valid_scores.groupby(generation_group_cols)['score'].agg([
        ('evaluator_count', 'count'),
        ('mean_score', 'mean'),
        ('std_dev', 'std'),
        ('min_score', 'min'),
        ('max_score', 'max'),
        ('median_score', 'median')
    ]).reset_index()
    
    # Only keep cases where we have multiple evaluators (agreement possible)
    multi_evaluator = agreement_stats[agreement_stats['evaluator_count'] >= 2].copy()
    
    if multi_evaluator.empty:
        context.log.warning("No generation responses found with multiple evaluators")
        return pd.DataFrame()
    
    # Calculate agreement metrics
    multi_evaluator['score_range'] = multi_evaluator['max_score'] - multi_evaluator['min_score']
    multi_evaluator['coefficient_of_variation'] = multi_evaluator['std_dev'] / multi_evaluator['mean_score']
    
    # Classify agreement levels
    def classify_agreement(row):
        """Classify evaluator agreement based on score range and CV"""
        if pd.isna(row['std_dev']) or row['evaluator_count'] < 2:
            return 'insufficient_data'
        elif row['score_range'] <= 1.0:  # Within 1 point
            return 'high_agreement'
        elif row['score_range'] <= 2.0:  # Within 2 points
            return 'moderate_agreement'
        elif row['score_range'] <= 3.0:  # Within 3 points
            return 'low_agreement'
        else:
            return 'poor_agreement'
    
    multi_evaluator['agreement_classification'] = multi_evaluator.apply(classify_agreement, axis=1)
    
    # Add relative variance metrics (normalized by score scale)
    multi_evaluator['relative_std_dev'] = multi_evaluator['std_dev'] / 10.0
    multi_evaluator['relative_range'] = multi_evaluator['score_range'] / 10.0
    
    # The groupby operation already includes combo_id, generation_template, generation_model columns
    # No additional metadata extraction needed
    
    # Create summary statistics
    agreement_summary = multi_evaluator['agreement_classification'].value_counts()
    # Convert numpy types to Python types for clean logging
    agreement_dict = {k: int(v) for k, v in agreement_summary.items()}
    context.log.info(f"Evaluator agreement summary: {agreement_dict}")
    
    # Log overall agreement statistics
    if len(multi_evaluator) > 0:
        overall_range = multi_evaluator['score_range'].median()
        overall_std = multi_evaluator['std_dev'].median()
        high_disagreement = len(multi_evaluator[multi_evaluator['score_range'] > 3.0])
        
        context.log.info(f"Overall median score range: {overall_range:.2f}")
        context.log.info(f"Overall median standard deviation: {overall_std:.2f}")
        
        if high_disagreement > 0:
            context.log.warning(f"Found {high_disagreement} generation responses with high evaluator disagreement (range > 3.0)")
    
    context.log.info(f"Analyzed evaluator agreement for {len(multi_evaluator)} generation responses with multiple evaluators")
    
    # Add output metadata
    agreement_counts = multi_evaluator['agreement_classification'].value_counts().to_dict() if not multi_evaluator.empty else {}
    
    context.add_output_metadata({
        "responses_analyzed": MetadataValue.int(len(multi_evaluator)),
        "total_valid_scores": MetadataValue.int(len(valid_scores)),
        "multi_evaluator_cases": MetadataValue.int(len(multi_evaluator)),
        "high_agreement": MetadataValue.int(agreement_counts.get('high_agreement', 0)),
        "moderate_agreement": MetadataValue.int(agreement_counts.get('moderate_agreement', 0)),
        "low_agreement": MetadataValue.int(agreement_counts.get('low_agreement', 0)),
        "poor_agreement": MetadataValue.int(agreement_counts.get('poor_agreement', 0)),
        "median_score_range": MetadataValue.float(round(float(multi_evaluator['score_range'].median()), 2) if not multi_evaluator.empty and not pd.isna(multi_evaluator['score_range'].median()) else 0.0)
    })
    
    return multi_evaluator


@asset(
    group_name="results_summary",
    io_manager_key="summary_results_io_manager"
)
def comprehensive_variance_analysis(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Comprehensive variance analysis across all evaluation dimensions:
    1. Template variance: Same model, different evaluation templates
    2. Model variance: Same template, different evaluation models  
    3. Overall variance: All evaluations of the same generation response
    
    This creates a detailed breakdown of where evaluation instability comes from.
    """
    # Filter out rows with errors
    valid_scores = parsed_scores[
        parsed_scores['error'].isna() & 
        parsed_scores['score'].notna()
    ].copy()
    
    if valid_scores.empty:
        context.log.warning("No valid scores found for comprehensive variance analysis")
        return pd.DataFrame()
    
    # Define the columns that identify the same generated response
    generation_group_cols = ['combo_id', 'generation_template', 'generation_model']
    
    # Map from existing column names (always available in parsed_scores)
    valid_scores['eval_template'] = valid_scores['evaluation_template']
    valid_scores['eval_model'] = valid_scores['evaluation_model']
    
    # Now we can analyze variance across multiple dimensions
    variance_analyses = []
    
    # 1. Overall variance: Group by generation method (same generated content)
    overall_variance = valid_scores.groupby(generation_group_cols).agg({
        'score': ['count', 'mean', 'std', 'min', 'max'],
        'eval_template': lambda x: x.nunique(),  # How many different templates
        'eval_model': lambda x: x.nunique()     # How many different models
    }).round(3)
    
    overall_variance.columns = ['total_evaluations', 'mean_score', 'std_dev', 'min_score', 'max_score', 'num_templates', 'num_models']
    overall_variance = overall_variance.reset_index()
    overall_variance['score_range'] = overall_variance['max_score'] - overall_variance['min_score']
    overall_variance['coefficient_of_variation'] = overall_variance['std_dev'] / overall_variance['mean_score']
    overall_variance['analysis_type'] = 'overall_variance'
    
    # 2. Template variance: Same generation method + same eval model, different eval templates
    template_groups = valid_scores.groupby(generation_group_cols + ['eval_model'])
    template_variance_list = []
    
    for group_keys, group in template_groups:
        if len(group) >= 2:  # Need multiple evaluations to calculate variance
            # Unpack the group keys based on the number of grouping columns
            combo_id, gen_template, gen_model, eval_model = group_keys
            template_stats = {
                'combo_id': combo_id,
                'generation_template': gen_template,
                'generation_model': gen_model,
                'eval_model': eval_model,
                'template_evaluations': len(group),
                'mean_score': group['score'].mean(),
                'std_dev': group['score'].std(),
                'min_score': group['score'].min(),
                'max_score': group['score'].max(),
                'num_templates': group['eval_template'].nunique(),
                'templates_used': ', '.join(sorted(group['eval_template'].unique())),
                'analysis_type': 'template_variance'
            }
            template_stats['score_range'] = template_stats['max_score'] - template_stats['min_score']
            template_stats['coefficient_of_variation'] = template_stats['std_dev'] / template_stats['mean_score'] if template_stats['mean_score'] != 0 else 0
            template_variance_list.append(template_stats)
    
    template_variance = pd.DataFrame(template_variance_list) if template_variance_list else pd.DataFrame()
    
    # 3. Model variance: Same generation method + same eval template, different eval models  
    model_groups = valid_scores.groupby(generation_group_cols + ['eval_template'])
    model_variance_list = []
    
    for group_keys, group in model_groups:
        if len(group) >= 2:  # Need multiple evaluations to calculate variance
            # Unpack the group keys
            combo_id, gen_template, gen_model, eval_template = group_keys
            model_stats = {
                'combo_id': combo_id,
                'generation_template': gen_template,
                'generation_model': gen_model,
                'eval_template': eval_template,
                'model_evaluations': len(group),
                'mean_score': group['score'].mean(),
                'std_dev': group['score'].std(),
                'min_score': group['score'].min(),
                'max_score': group['score'].max(),
                'num_models': group['eval_model'].nunique(),
                'models_used': ', '.join(sorted(group['eval_model'].unique())),
                'analysis_type': 'model_variance'
            }
            model_stats['score_range'] = model_stats['max_score'] - model_stats['min_score']
            model_stats['coefficient_of_variation'] = model_stats['std_dev'] / model_stats['mean_score'] if model_stats['mean_score'] != 0 else 0
            model_variance_list.append(model_stats)
    
    model_variance = pd.DataFrame(model_variance_list) if model_variance_list else pd.DataFrame()
    
    # Combine all analyses
    result_dfs = []
    
    if not overall_variance.empty:
        result_dfs.append(overall_variance)
        context.log.info(f"Overall variance: {len(overall_variance)} generation responses with multiple evaluations")
    
    if not template_variance.empty:
        result_dfs.append(template_variance)
        context.log.info(f"Template variance: {len(template_variance)} cases where same model used different templates")
    
    if not model_variance.empty:
        result_dfs.append(model_variance)  
        context.log.info(f"Model variance: {len(model_variance)} cases where same template used different models")
    
    if result_dfs:
        # Combine all analyses into one comprehensive DataFrame
        combined_analysis = pd.concat(result_dfs, ignore_index=True, sort=False)
        
        # Add stability classifications
        def classify_variance_stability(row):
            if pd.isna(row['coefficient_of_variation']) or row.get('total_evaluations', row.get('template_evaluations', row.get('model_evaluations', 1))) < 2:
                return 'insufficient_data'
            elif row['score_range'] <= 1.0:
                return 'high_agreement'
            elif row['score_range'] <= 2.0:
                return 'moderate_agreement'
            elif row['score_range'] <= 3.0:
                return 'low_agreement'
            else:
                return 'poor_agreement'
        
        combined_analysis['stability_classification'] = combined_analysis.apply(classify_variance_stability, axis=1)
        
        # Log summary statistics by analysis type
        for analysis_type in combined_analysis['analysis_type'].unique():
            subset = combined_analysis[combined_analysis['analysis_type'] == analysis_type]
            stability_summary = subset['stability_classification'].value_counts()
            median_range = subset['score_range'].median()
            # Convert numpy types to Python types for clean logging
            stability_dict = {k: int(v) for k, v in stability_summary.items()}
            context.log.info(f"{analysis_type} - Median range: {median_range:.2f}, Stability: {stability_dict}")
        
        context.log.info(f"Comprehensive variance analysis complete: {len(combined_analysis)} variance measurements")
        
        # Add output metadata
        analysis_type_counts = combined_analysis['analysis_type'].value_counts().to_dict()
        stability_counts = combined_analysis['stability_classification'].value_counts().to_dict()
        
        context.add_output_metadata({
            "variance_measurements": MetadataValue.int(len(combined_analysis)),
            "overall_variance": MetadataValue.int(analysis_type_counts.get('overall_variance', 0)),
            "template_variance": MetadataValue.int(analysis_type_counts.get('template_variance', 0)),
            "model_variance": MetadataValue.int(analysis_type_counts.get('model_variance', 0)),
            "high_agreement": MetadataValue.int(stability_counts.get('high_agreement', 0)),
            "moderate_agreement": MetadataValue.int(stability_counts.get('moderate_agreement', 0)),
            "low_agreement": MetadataValue.int(stability_counts.get('low_agreement', 0)),
            "poor_agreement": MetadataValue.int(stability_counts.get('poor_agreement', 0)),
            "median_score_range": MetadataValue.float(round(float(combined_analysis['score_range'].median()), 2) if not combined_analysis.empty and not pd.isna(combined_analysis['score_range'].median()) else 0.0)
        })
        
        return combined_analysis
    else:
        context.log.warning("No variance patterns found - all evaluations appear to be single instances")
        
        # Add output metadata for empty result
        context.add_output_metadata({
            "variance_measurements": MetadataValue.int(0),
            "source_evaluations": MetadataValue.int(len(valid_scores)),
            "analysis_result": MetadataValue.text("No multi-evaluator patterns found")
        })
        
        return pd.DataFrame()
