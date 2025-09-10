from dagster import asset, MetadataValue, Failure
import pandas as pd
import numpy as np
from pathlib import Path
from .raw_data import EVALUATION_TEMPLATES_KEY
from ..utils.raw_readers import read_evaluation_templates
from ..constants import ESSAY, FILE_PARSED
from functools import lru_cache


@lru_cache(maxsize=1)
def _load_essay_tasks(base: str) -> pd.DataFrame:
    """Load essay_generation_tasks.csv once (best-effort).

    Returns empty DataFrame on failure.
    """
    try:
        df = pd.read_csv(Path(base) / "2_tasks" / "essay_generation_tasks.csv")
        # Normalize types
        for col in ("combo_id", "draft_template", "essay_template", "generation_model", "generation_model_name", "gen_id"):
            if col in df.columns:
                df[col] = df[col].astype(str)
        return df
    except Exception:
        return pd.DataFrame()


def get_generation_response_path(combo_id: str, draft_template: str, essay_template: str, model_name: str, *, data_root: str | Path = "data") -> str:
    """Construct the canonical gens-store essay path for a given row.

    Tries to resolve gen_id from data/2_tasks/essay_generation_tasks.csv using
    (combo_id, draft_template, essay_template, model). If found, returns
    data/gens/essay/<gen_id>/parsed.txt. If not found, falls back to the legacy
    single-phase path under data/3_generation/essay_responses/ for historical
    compatibility in older reports.
    """
    # Attempt gens-store resolution via tasks CSV
    df = _load_essay_tasks(str(data_root))
    if not df.empty:
        # Some historical CSVs used 'link_template' for drafts; prefer 'draft_template' when present
        draft_col = "draft_template" if "draft_template" in df.columns else ("link_template" if "link_template" in df.columns else None)
        if draft_col and {"combo_id", draft_col, "essay_template", "gen_id"}.issubset(df.columns):
            # Match by model_id only to avoid provider dependency
            mask = (
                (df["combo_id"].astype(str) == str(combo_id)) &
                (df[draft_col].astype(str) == str(draft_template)) &
                (df["essay_template"].astype(str) == str(essay_template))
            )
            if "generation_model" in df.columns:
                mask = mask & (df["generation_model"].astype(str) == str(model_name))
            candidates = df[mask]
            if not candidates.empty:
                gen_id = str(candidates.iloc[0]["gen_id"])  # first match is fine for reporting
                return str(Path(data_root) / "gens" / ESSAY / gen_id / FILE_PARSED)

    # Legacy fallback for historical reports
    return f"data/3_generation/essay_responses/{combo_id}_{draft_template}_{model_name}_{essay_template}.txt"


@asset(
    group_name="results_processing",
    io_manager_key="summary_results_io_manager",
    required_resource_keys={"data_root"},
    deps={EVALUATION_TEMPLATES_KEY},
)
def generation_scores_pivot(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot individual evaluation scores per generation.

    Rows: combo_id, generation_template, generation_model
    Columns: Each unique (evaluation_template, evaluation_model) combination
    Values: Individual score for that specific evaluator combination (no averaging)
    """
    # Load evaluation templates CSV and extract active templates
    eval_df = read_evaluation_templates(Path(context.resources.data_root))
    if eval_df is None or eval_df.empty:
        context.log.warning("No evaluation templates CSV found or empty; returning empty pivot")
        return pd.DataFrame()
    active_templates = eval_df[eval_df.get('active', True) == True]['template_id'].tolist()
    
    if not active_templates:
        context.log.warning("No active evaluation templates found; returning empty pivot")
        return pd.DataFrame()
    
    # Filter to valid scored rows
    if parsed_scores is None or parsed_scores.empty:
        context.log.warning("No parsed_scores provided; returning empty pivot")
        return pd.DataFrame()

    valid_scores = parsed_scores[
        parsed_scores['error'].isna() & parsed_scores['score'].notna()
    ].copy()

    if valid_scores.empty:
        context.log.warning("No valid scores found; returning empty pivot")
        return pd.DataFrame()

    # Create combined column name for (evaluation_template, evaluation_model)
    valid_scores['eval_template_model'] = (
        valid_scores['evaluation_template'] + '_' + valid_scores['evaluation_model']
    )

    # Build pivot: individual score for each generation across each (template, model) combination
    # Include draft_template and stage in index to distinguish modes
    pivot_df = valid_scores.pivot_table(
        index=['combo_id', 'stage', 'draft_template', 'generation_template', 'generation_model'],
        columns='eval_template_model',
        values='score',
        aggfunc='first'  # Take first score if duplicates exist (shouldn't happen with proper data)
    ).round(2)

    # Flatten and reset index
    pivot_df = pivot_df.reset_index()

    # Get all evaluation columns (excluding the index columns)
    eval_columns = [col for col in pivot_df.columns if col not in ['combo_id', 'stage', 'draft_template', 'generation_template', 'generation_model']]
    
    # Add aggregate column with all scores summed across evaluators
    # NaNs are ignored in the sum; round for readability
    if eval_columns:
        pivot_df['sum_scores'] = pivot_df[eval_columns].sum(axis=1, skipna=True).round(2)
    else:
        pivot_df['sum_scores'] = 0.0

    # Require draft_template in parsed_scores (added with two-phase architecture)
    if 'draft_template' not in valid_scores.columns:
        # Fail fast with guidance to rematerialize upstream asset
        raise Failure(
            description="Missing required column 'draft_template' in parsed_scores for two-phase path generation",
            metadata={
                "resolution": MetadataValue.text(
                    "Rematerialize 'parsed_scores' to include new columns (draft_template, essay_task_id). "
                    "For example: `uv run dagster asset materialize --select parsed_scores -f daydreaming_dagster/definitions.py`"
                ),
                "present_columns": MetadataValue.text(", ".join(list(valid_scores.columns))),
                "expected_column": MetadataValue.text("draft_template"),
            }
        )
    # draft_template is now already included in the pivot index, so no need to merge it back

    # Attach generation_response_path from parsed_scores when available
    if 'generation_response_path' in parsed_scores.columns:
        path_map = parsed_scores[
            ['combo_id', 'stage', 'draft_template', 'generation_template', 'generation_model', 'generation_response_path']
        ].drop_duplicates()
        pivot_df = pivot_df.merge(
            path_map,
            on=['combo_id', 'stage', 'draft_template', 'generation_template', 'generation_model'],
            how='left'
        )
    else:
        pivot_df['generation_response_path'] = pivot_df.apply(
            lambda row: get_generation_response_path(
                row['combo_id'], row.get('draft_template'), row['generation_template'], row['generation_model']
            ),
            axis=1,
        )
    
    # Order columns: index columns first, then evaluation columns
    ordered_cols = ['combo_id', 'stage', 'draft_template', 'generation_template', 'generation_model'] + eval_columns + ['sum_scores', 'generation_response_path']
    pivot_df = pivot_df[ordered_cols]

    # Metadata
    context.add_output_metadata({
        "rows": MetadataValue.int(len(pivot_df)),
        "unique_generations": MetadataValue.int(pivot_df[['combo_id', 'stage', 'draft_template', 'generation_template', 'generation_model']].drop_duplicates().shape[0]),
        "evaluation_combinations": MetadataValue.int(len(eval_columns)),
        "total_active_templates": MetadataValue.int(len(active_templates)),
        "evaluation_columns": MetadataValue.text(", ".join(eval_columns))
    })

    return pivot_df

@asset(
    group_name="results_summary", 
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
    group_name="results_summary",
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
        # Generation path: prefer parsed 'generation_response_path' or explicit essay_task_id
        generation_path = None
        if 'generation_response_path' in row and isinstance(row['generation_response_path'], str) and row['generation_response_path']:
            generation_path = row['generation_response_path']
        elif 'essay_task_id' in row and isinstance(row['essay_task_id'], str) and row['essay_task_id']:
            generation_path = f"data/3_generation/essay_responses/{row['essay_task_id']}.txt"
        else:
            # Build from components; require draft_template in doc-id-first architecture
            draft_tpl = row.get('draft_template') if 'draft_template' in row else None
            if not isinstance(draft_tpl, str) or not draft_tpl:
                raise Failure(
                    description="Cannot reconstruct essay path: missing draft_template",
                    metadata={
                        "combo_id": MetadataValue.text(str(row.get('combo_id'))),
                        "generation_template": MetadataValue.text(str(row.get('generation_template'))),
                        "generation_model": MetadataValue.text(str(row.get('generation_model'))),
                        "resolution": MetadataValue.text("Ensure parsed_scores includes 'draft_template' and essay_task_id, or provide generation_response_path."),
                    },
                )
            generation_path = get_generation_response_path(
                str(row['combo_id']), str(draft_tpl), str(row['generation_template']), str(row['generation_model'])
            )

        # Evaluation path: use parsed column when present
        # FALLBACK(DATA): Use legacy single-phase evaluation path when parsed_scores lacks an explicit path.
        evaluation_path = row['evaluation_response_path'] if 'evaluation_response_path' in row else f"data/4_evaluation/evaluation_responses/{row['combo_id']}_{row['generation_template']}_{row['generation_model']}_{row.get('evaluation_template', 'daydreaming-verification-v2')}_{row['evaluation_model']}.txt"
        
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


@asset(
    group_name="results_summary", 
    io_manager_key="summary_results_io_manager"
)
def evaluation_model_template_pivot(context, parsed_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Create pivot table with (evaluation_model, evaluation_template) combinations as columns.
    
    Rows: Each generation response (combo_id, generation_template, generation_model)
    Columns: Each (evaluation_model, evaluation_template) combination  
    Values: Score for that generation evaluated by that (model, template) combination
    
    This table enables easy comparison of how different evaluation approaches
    score the same generation responses.
    """
    if parsed_scores is None or parsed_scores.empty:
        context.log.warning("No parsed_scores provided; returning empty pivot")
        return pd.DataFrame()
    
    # Filter to valid scored rows
    valid_scores = parsed_scores[
        parsed_scores['error'].isna() & parsed_scores['score'].notna()
    ].copy()
    
    if valid_scores.empty:
        context.log.warning("No valid scores found; returning empty pivot")
        return pd.DataFrame()
    
    # Create combined column name for (evaluation_model, evaluation_template)
    valid_scores['eval_model_template'] = (
        valid_scores['evaluation_model'] + '_' + valid_scores['evaluation_template']
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
