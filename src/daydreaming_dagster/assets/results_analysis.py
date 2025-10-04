from dagster import AssetKey, MetadataValue
from ._decorators import asset_with_boundary
import pandas as pd

from ..data_layer.paths import Paths, COHORT_REPORT_ASSET_TARGETS
from ..utils.errors import DDError, Err
from .partitions import cohort_reports_partitions


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
    group_name="results_summary",
    io_manager_key="summary_results_io_manager",
    deps={AssetKey("cohort_id")},
    partitions_def=cohort_reports_partitions,
)
def comprehensive_variance_analysis(context, cohort_aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Comprehensive variance analysis across all evaluation dimensions:
    1. Template variance: Same model, different evaluation templates
    2. Model variance: Same template, different evaluation models
    3. Overall variance: All evaluations of the same generation response

    This creates a detailed breakdown of where evaluation instability comes from.
    """

    cohort_id = _require_cohort_partition(context, "comprehensive_variance_analysis")
    paths = Paths.from_context(context)

    from ..utils.evaluation_processing import filter_valid_scores

    valid_scores = filter_valid_scores(cohort_aggregated_scores)
    if valid_scores.empty:
        context.log.warning("No valid scores found for comprehensive variance analysis")
        _, filename = COHORT_REPORT_ASSET_TARGETS["comprehensive_variance_analysis"]
        context.add_output_metadata(
            {
                "sections": MetadataValue.int(0),
                "cohort_id": MetadataValue.text(cohort_id),
                "output": MetadataValue.path(str(paths.cohort_analysis_csv(cohort_id, filename))),
            }
        )
        return pd.DataFrame()

    generation_group_cols = ["combo_id", "generation_template", "generation_model"]

    valid_scores["eval_template"] = valid_scores["evaluation_template"]
    if "evaluation_llm_model" not in valid_scores.columns:
        context.log.error("cohort_aggregated_scores missing 'evaluation_llm_model' column")
        _, filename = COHORT_REPORT_ASSET_TARGETS["comprehensive_variance_analysis"]
        context.add_output_metadata(
            {
                "sections": MetadataValue.int(0),
                "cohort_id": MetadataValue.text(cohort_id),
                "output": MetadataValue.path(str(paths.cohort_analysis_csv(cohort_id, filename))),
            }
        )
        return pd.DataFrame()
    valid_scores["eval_model"] = valid_scores["evaluation_llm_model"]

    variance_analyses: list[pd.DataFrame] = []

    overall_variance = (
        valid_scores.groupby(generation_group_cols)
        .agg(
            {
                "score": ["count", "mean", "std", "min", "max"],
                "eval_template": lambda x: x.nunique(),
                "eval_model": lambda x: x.nunique(),
            }
        )
        .round(3)
    )
    overall_variance.columns = [
        "total_evaluations",
        "mean_score",
        "std_dev",
        "min_score",
        "max_score",
        "num_templates",
        "num_models",
    ]
    overall_variance = overall_variance.reset_index()
    overall_variance["score_range"] = overall_variance["max_score"] - overall_variance["min_score"]
    overall_variance["coefficient_of_variation"] = overall_variance["std_dev"] / overall_variance[
        "mean_score"
    ]
    overall_variance["analysis_type"] = "overall_variance"

    template_groups = valid_scores.groupby(generation_group_cols + ["eval_model"])
    template_variance_rows: list[dict[str, object]] = []
    for (combo_id, gen_template, gen_model, eval_model), group in template_groups:
        if len(group) < 2:
            continue
        stats = {
            "combo_id": combo_id,
            "generation_template": gen_template,
            "generation_model": gen_model,
            "eval_model": eval_model,
            "template_evaluations": len(group),
            "mean_score": group["score"].mean(),
            "std_dev": group["score"].std(),
            "min_score": group["score"].min(),
            "max_score": group["score"].max(),
            "num_templates": group["eval_template"].nunique(),
            "templates_used": ", ".join(sorted(group["eval_template"].unique())),
            "analysis_type": "template_variance",
        }
        stats["score_range"] = stats["max_score"] - stats["min_score"]
        stats["coefficient_of_variation"] = (
            stats["std_dev"] / stats["mean_score"] if stats["mean_score"] else 0
        )
        template_variance_rows.append(stats)

    template_variance = pd.DataFrame(template_variance_rows) if template_variance_rows else pd.DataFrame()

    model_groups = valid_scores.groupby(generation_group_cols + ["eval_template"])
    model_variance_rows: list[dict[str, object]] = []
    for (combo_id, gen_template, gen_model, eval_template), group in model_groups:
        if len(group) < 2:
            continue
        stats = {
            "combo_id": combo_id,
            "generation_template": gen_template,
            "generation_model": gen_model,
            "eval_template": eval_template,
            "model_evaluations": len(group),
            "mean_score": group["score"].mean(),
            "std_dev": group["score"].std(),
            "min_score": group["score"].min(),
            "max_score": group["score"].max(),
            "num_models": group["eval_model"].nunique(),
            "models_used": ", ".join(sorted(group["eval_model"].unique())),
            "analysis_type": "model_variance",
        }
        stats["score_range"] = stats["max_score"] - stats["min_score"]
        stats["coefficient_of_variation"] = (
            stats["std_dev"] / stats["mean_score"] if stats["mean_score"] else 0
        )
        model_variance_rows.append(stats)

    model_variance = pd.DataFrame(model_variance_rows) if model_variance_rows else pd.DataFrame()

    if not overall_variance.empty:
        variance_analyses.append(overall_variance)
        context.log.info(
            "Overall variance: %s generation responses with multiple evaluations",
            len(overall_variance),
        )
    if not template_variance.empty:
        variance_analyses.append(template_variance)
        context.log.info(
            "Template variance: %s cases where the same model used different templates",
            len(template_variance),
        )
    if not model_variance.empty:
        variance_analyses.append(model_variance)
        context.log.info(
            "Model variance: %s cases where the same template used different models",
            len(model_variance),
        )

    if not variance_analyses:
        context.log.warning("No variance patterns found - all evaluations appear to be single instances")
        _, filename = COHORT_REPORT_ASSET_TARGETS["comprehensive_variance_analysis"]
        context.add_output_metadata(
            {
                "variance_measurements": MetadataValue.int(0),
                "source_evaluations": MetadataValue.int(len(valid_scores)),
                "analysis_result": MetadataValue.text("No multi-evaluator patterns found"),
                "cohort_id": MetadataValue.text(cohort_id),
                "output": MetadataValue.path(str(paths.cohort_analysis_csv(cohort_id, filename))),
            }
        )
        return pd.DataFrame()

    combined_analysis = pd.concat(variance_analyses, ignore_index=True, sort=False)

    def classify_stability(row: pd.Series) -> str:
        evaluations = row.get(
            "total_evaluations",
            row.get("template_evaluations", row.get("model_evaluations", 1)),
        )
        if pd.isna(row["coefficient_of_variation"]) or evaluations < 2:
            return "insufficient_data"
        if row["score_range"] <= 1.0:
            return "high_agreement"
        if row["score_range"] <= 2.0:
            return "moderate_agreement"
        if row["score_range"] <= 3.0:
            return "low_agreement"
        return "poor_agreement"

    combined_analysis["stability_classification"] = combined_analysis.apply(
        classify_stability, axis=1
    )

    for analysis_type in combined_analysis["analysis_type"].unique():
        subset = combined_analysis[combined_analysis["analysis_type"] == analysis_type]
        stability_summary = subset["stability_classification"].value_counts()
        median_range = subset["score_range"].median()
        context.log.info(
            "%s - Median range: %.2f, Stability: %s",
            analysis_type,
            median_range,
            {k: int(v) for k, v in stability_summary.items()},
        )

    context.log.info(
        "Comprehensive variance analysis complete: %s variance measurements",
        len(combined_analysis),
    )

    analysis_type_counts = combined_analysis["analysis_type"].value_counts().to_dict()
    stability_counts = combined_analysis["stability_classification"].value_counts().to_dict()

    _, filename = COHORT_REPORT_ASSET_TARGETS["comprehensive_variance_analysis"]
    context.add_output_metadata(
        {
            "variance_measurements": MetadataValue.int(len(combined_analysis)),
            "overall_variance": MetadataValue.int(analysis_type_counts.get("overall_variance", 0)),
            "template_variance": MetadataValue.int(analysis_type_counts.get("template_variance", 0)),
            "model_variance": MetadataValue.int(analysis_type_counts.get("model_variance", 0)),
            "high_agreement": MetadataValue.int(stability_counts.get("high_agreement", 0)),
            "moderate_agreement": MetadataValue.int(stability_counts.get("moderate_agreement", 0)),
            "low_agreement": MetadataValue.int(stability_counts.get("low_agreement", 0)),
            "poor_agreement": MetadataValue.int(stability_counts.get("poor_agreement", 0)),
            "median_score_range": MetadataValue.float(
                round(
                    float(combined_analysis["score_range"].median()),
                    2,
                )
                if not combined_analysis.empty
                and not pd.isna(combined_analysis["score_range"].median())
                else 0.0
            ),
            "cohort_id": MetadataValue.text(cohort_id),
            "output": MetadataValue.path(str(paths.cohort_analysis_csv(cohort_id, filename))),
        }
    )

    return combined_analysis
