from dagster import AssetKey, MetadataValue
from ._decorators import asset_with_boundary
import pandas as pd

from ..data_layer.paths import Paths, COHORT_REPORT_ASSET_TARGETS
from .raw_data import EVALUATION_TEMPLATES_KEY
from ..cohorts import build_spec_catalogs, load_cohort_allowlists
from ..results_summary.transformations import (
    compute_generation_scores_pivot,
    compute_final_results,
    compute_evaluation_model_template_pivot,
    filter_perfect_score_rows,
)
from ..utils.errors import DDError, Err
from .partitions import cohort_reports_partitions


GENERATION_INDEX_COLUMNS = [
    "combo_id",
    "draft_template",
    "generation_template",
    "generation_model",
]


def _require_cohort_partition(context, asset_name: str) -> str:
    partition_key = getattr(context, "partition_key", None)
    if not partition_key:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "cohort_partition_required",
                "asset": asset_name,
            },
        )
    return str(partition_key)


@asset_with_boundary(
    stage="results_summary",
    group_name="results_processing",
    io_manager_key="summary_results_io_manager",
    required_resource_keys={"data_root", "cohort_spec"},
    deps={EVALUATION_TEMPLATES_KEY, AssetKey("cohort_id")},
    partitions_def=cohort_reports_partitions,
)
def generation_scores_pivot(context, cohort_aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    """Pivot individual evaluation scores per generation."""

    cohort_id = _require_cohort_partition(context, "generation_scores_pivot")
    paths = Paths.from_context(context)

    allowlists = load_cohort_allowlists(
        data_root=paths.data_root,
        cohort_id=cohort_id,
        compile_definition=context.resources.cohort_spec.compile_definition,
        catalogs=build_spec_catalogs(paths.data_root),
        require_evaluation_axes=False,
    )

    if not allowlists.has_evaluation_axes():
        context.log.warning(
            "No evaluation templates/models defined in cohort spec; returning empty pivot",
        )
        return pd.DataFrame()

    eval_templates = allowlists.evaluation_templates_list()
    eval_models = allowlists.evaluation_models_list()

    if cohort_aggregated_scores is None or cohort_aggregated_scores.empty:
        context.log.warning("No cohort aggregated scores provided; returning empty pivot")
        return pd.DataFrame()

    if "generation_response_path" in cohort_aggregated_scores.columns:
        path_lookup = cohort_aggregated_scores[
            GENERATION_INDEX_COLUMNS + ["generation_response_path"]
        ].drop_duplicates()
    else:
        path_lookup = None

    pivot_result = compute_generation_scores_pivot(
        cohort_aggregated_scores,
        allowed_eval_templates=eval_templates,
        allowed_eval_models=eval_models,
        generation_path_lookup=path_lookup,
    )

    if pivot_result.valid_row_count == 0:
        context.log.warning("No valid scores found; returning empty pivot")
        return pivot_result.frame

    if pivot_result.allowlisted_row_count == 0:
        context.log.warning(
            "No cohort scores match evaluation spec; returning empty pivot",
        )
        return pivot_result.frame

    pivot_df = pivot_result.frame

    _, filename = COHORT_REPORT_ASSET_TARGETS["generation_scores_pivot"]
    context.add_output_metadata({
        "rows": MetadataValue.int(len(pivot_df)),
        "unique_generations": MetadataValue.int(
            pivot_df[GENERATION_INDEX_COLUMNS].drop_duplicates().shape[0]
        ),
        "evaluation_combinations": MetadataValue.int(len(pivot_result.evaluation_columns)),
        "total_spec_templates": MetadataValue.int(len(eval_templates)),
        "evaluation_columns": MetadataValue.text(
            ", ".join(pivot_result.evaluation_columns)
        ),
        "cohort_id": MetadataValue.text(cohort_id),
        "output": MetadataValue.path(
            str(paths.cohort_summary_csv(cohort_id, filename))
        ),
    })

    return pivot_df


@asset_with_boundary(
    stage="results_summary",
    group_name="results_summary",
    io_manager_key="summary_results_io_manager",
    deps={AssetKey("cohort_id")},
    partitions_def=cohort_reports_partitions,
)
def final_results(context, cohort_aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    """Create comprehensive pivot table summaries with statistics."""

    cohort_id = _require_cohort_partition(context, "final_results")
    paths = Paths.from_context(context)

    results = compute_final_results(cohort_aggregated_scores)
    final_summary = results.frame

    if final_summary.empty:
        context.log.warning("No valid scores found for analysis")

        _, filename = COHORT_REPORT_ASSET_TARGETS["final_results"]
        context.add_output_metadata({
            "summary_rows": MetadataValue.int(0),
            "source_evaluations": MetadataValue.int(0),
            "analysis_result": MetadataValue.text(
                "No valid scores found for analysis"
            ),
            "cohort_id": MetadataValue.text(cohort_id),
            "output": MetadataValue.path(
                str(paths.cohort_summary_csv(cohort_id, filename))
            ),
        })
        return final_summary

    context.log.info(
        "Created comprehensive analysis with %s summary rows from %s valid scores",
        len(final_summary),
        results.valid_score_count,
    )

    _, filename = COHORT_REPORT_ASSET_TARGETS["final_results"]
    analysis_type_counts = (
        final_summary["analysis_type"].value_counts().to_dict()
        if "analysis_type" in final_summary.columns
        else {}
    )
    existing_columns = ", ".join(final_summary.columns.tolist())

    context.add_output_metadata({
        "summary_rows": MetadataValue.int(len(final_summary)),
        "source_evaluations": MetadataValue.int(results.valid_score_count),
        "analysis_categories": MetadataValue.int(len(analysis_type_counts)),
        "by_template": MetadataValue.int(
            analysis_type_counts.get("by_generation_template", 0)
        ),
        "by_model": MetadataValue.int(
            analysis_type_counts.get("by_generation_model", 0)
        ),
        "by_combo": MetadataValue.int(analysis_type_counts.get("by_combo_id", 0)),
        "overall_stats": MetadataValue.int(
            analysis_type_counts.get("overall_statistics", 0)
        ),
        "columns_included": MetadataValue.text(existing_columns),
        "cohort_id": MetadataValue.text(cohort_id),
        "output": MetadataValue.path(
            str(paths.cohort_summary_csv(cohort_id, filename))
        ),
    })

    return final_summary


@asset_with_boundary(
    stage="results_summary",
    group_name="results_summary",
    io_manager_key="summary_results_io_manager",
    deps={AssetKey("cohort_id")},
    partitions_def=cohort_reports_partitions,
)
def perfect_score_paths(context, cohort_aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    """Generate a file with paths to responses that received perfect scores."""

    cohort_id = _require_cohort_partition(context, "perfect_score_paths")
    paths = Paths.from_context(context)

    result_df = filter_perfect_score_rows(cohort_aggregated_scores)

    if result_df.empty:
        context.log.warning("No perfect scores found")
        _, filename = COHORT_REPORT_ASSET_TARGETS["perfect_score_paths"]
        context.add_output_metadata({
            "perfect_scores": MetadataValue.int(0),
            "cohort_id": MetadataValue.text(cohort_id),
            "output": MetadataValue.path(
                str(paths.cohort_summary_csv(cohort_id, filename))
            ),
        })
        return result_df

    evaluator_counts = result_df["evaluation_llm_model"].value_counts().to_dict()
    template_counts = result_df["generation_template"].value_counts().to_dict()

    context.log.info("Found %s perfect score responses", len(result_df))
    context.log.info(
        "Perfect scores by evaluator: %s", evaluator_counts
    )
    context.log.info("Perfect scores by template: %s", template_counts)

    _, filename = COHORT_REPORT_ASSET_TARGETS["perfect_score_paths"]
    context.add_output_metadata({
        "perfect_scores": MetadataValue.int(len(result_df)),
        "unique_combinations": MetadataValue.int(
            result_df["combo_id"].nunique()
        ),
        "unique_templates": MetadataValue.int(
            result_df["generation_template"].nunique()
        ),
        "unique_evaluators": MetadataValue.int(
            result_df["evaluation_llm_model"].nunique()
        ),
        "deepseek_perfect": MetadataValue.int(
            evaluator_counts.get("deepseek", 0)
        ),
        "qwen_perfect": MetadataValue.int(
            evaluator_counts.get("qwen", 0)
        ),
        "google_perfect": MetadataValue.int(
            evaluator_counts.get("google", 0)
        ),
        "top_template": MetadataValue.text(
            max(template_counts, key=template_counts.get)
            if template_counts
            else "None"
        ),
        "cohort_id": MetadataValue.text(cohort_id),
        "output": MetadataValue.path(
            str(paths.cohort_summary_csv(cohort_id, filename))
        ),
    })

    return result_df


@asset_with_boundary(
    stage="results_summary",
    group_name="results_summary",
    io_manager_key="summary_results_io_manager",
    deps={AssetKey("cohort_id")},
    partitions_def=cohort_reports_partitions,
)
def evaluation_model_template_pivot(
    context, cohort_aggregated_scores: pd.DataFrame
) -> pd.DataFrame:
    """Create pivot table with (evaluation_llm_model, evaluation_template) columns."""

    cohort_id = _require_cohort_partition(context, "evaluation_model_template_pivot")
    paths = Paths.from_context(context)

    if cohort_aggregated_scores is None or cohort_aggregated_scores.empty:
        context.log.warning("No cohort aggregated scores provided; returning empty pivot")
        _, filename = COHORT_REPORT_ASSET_TARGETS[
            "evaluation_model_template_pivot"
        ]
        context.add_output_metadata({
            "total_generation_responses": MetadataValue.int(0),
            "evaluation_combinations": MetadataValue.int(0),
            "unique_combos": MetadataValue.int(0),
            "unique_generation_templates": MetadataValue.int(0),
            "unique_generation_models": MetadataValue.int(0),
            "cohort_id": MetadataValue.text(cohort_id),
            "output": MetadataValue.path(
                str(paths.cohort_summary_csv(cohort_id, filename))
            ),
        })
        return pd.DataFrame()

    pivot_result = compute_evaluation_model_template_pivot(cohort_aggregated_scores)

    if pivot_result.valid_row_count == 0:
        context.log.warning("No valid scores found; returning empty pivot")
        _, filename = COHORT_REPORT_ASSET_TARGETS[
            "evaluation_model_template_pivot"
        ]
        context.add_output_metadata({
            "total_generation_responses": MetadataValue.int(0),
            "evaluation_combinations": MetadataValue.int(0),
            "unique_combos": MetadataValue.int(0),
            "unique_generation_templates": MetadataValue.int(0),
            "unique_generation_models": MetadataValue.int(0),
            "cohort_id": MetadataValue.text(cohort_id),
            "output": MetadataValue.path(
                str(paths.cohort_summary_csv(cohort_id, filename))
            ),
        })
        return pivot_result.frame

    pivot_df = pivot_result.frame
    eval_columns = [
        column
        for column in pivot_df.columns
        if column not in GENERATION_INDEX_COLUMNS
    ]

    context.log.info(
        "Created pivot table with %s generation responses", len(pivot_df)
    )
    context.log.info(
        "Evaluation combinations: %s", len(eval_columns)
    )

    _, filename = COHORT_REPORT_ASSET_TARGETS[
        "evaluation_model_template_pivot"
    ]
    context.add_output_metadata({
        "total_generation_responses": MetadataValue.int(len(pivot_df)),
        "evaluation_combinations": MetadataValue.int(len(eval_columns)),
        "unique_combos": MetadataValue.int(pivot_df["combo_id"].nunique()),
        "unique_generation_templates": MetadataValue.int(
            pivot_df["generation_template"].nunique()
        ),
        "unique_generation_models": MetadataValue.int(
            pivot_df["generation_model"].nunique()
        ),
        "evaluation_coverage": MetadataValue.json(pivot_result.coverage_stats),
        "cohort_id": MetadataValue.text(cohort_id),
        "output": MetadataValue.path(
            str(paths.cohort_summary_csv(cohort_id, filename))
        ),
    })

    return pivot_df
