from dagster import AssetKey, MetadataValue
from ._decorators import asset_with_boundary
from pathlib import Path
from ..data_layer.paths import Paths
from ..utils.errors import DDError, Err
import pandas as pd
from ..utils.evaluation_processing import calculate_evaluation_metadata
from .partitions import cohort_reports_partitions


def cohort_aggregated_scores_impl(
    data_root: Path,
    *,
    scores_aggregator,
    membership_service,
    cohort_id: str | None = None,
) -> pd.DataFrame:
    """Pure implementation for the cohort-scoped aggregated scores.

    - Reads the evaluation gen_id list from membership_service
    - Delegates aggregation to scores_aggregator.parse_all_scores
    - Returns the resulting DataFrame (no Dagster context required)
    """
    keep_list = membership_service.stage_gen_ids(
        data_root, "evaluation", cohort_id=cohort_id
    )
    return scores_aggregator.parse_all_scores(data_root, keep_list)


@asset_with_boundary(
    stage="results_processing",
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"data_root", "scores_aggregator", "membership_service"},
    description=(
        "Aggregate evaluation scores from gens store for the CURRENT cohort only. "
        "Requires parsed.txt; rows without it are included with error=missing parsed.txt."
    ),
    compute_kind="pandas",
    partitions_def=cohort_reports_partitions,
    deps={AssetKey("cohort_id")},
)
def cohort_aggregated_scores(context) -> pd.DataFrame:
    """Aggregate evaluation scores for the current cohort.

    Source rows come from scripts.aggregate_scores.parse_all, which:
    - Reads data/gens/evaluation/<gen_id>/{parsed.txt,metadata.json}
    - Requires parsed.txt; when missing, emits a row with error="missing parsed.txt"
    - Emits enriched columns required downstream (combo_id, draft_template, generation_template,
      generation_model, generation_response_path, evaluation_llm_model, origin_cohort_id).
    This asset simply filters that cross-experiment aggregation down to the current cohort.
    """
    cohort_id = context.partition_key
    if not cohort_id:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "cohort_partition_required", "asset": "cohort_aggregated_scores"},
        )

    paths = Paths.from_context(context)
    data_root = paths.data_root
    out_csv = paths.cohort_parsing_csv(cohort_id, "aggregated_scores.csv")
    df = cohort_aggregated_scores_impl(
        data_root,
        scores_aggregator=context.resources.scores_aggregator,
        membership_service=context.resources.membership_service,
        cohort_id=cohort_id,
    )

    md = calculate_evaluation_metadata(df)
    md.update({
        "output": MetadataValue.path(str(out_csv)),
        "enriched": MetadataValue.bool(False),
        "cohort_id": MetadataValue.text(str(cohort_id)),
    })
    context.add_output_metadata(md)
    return df
