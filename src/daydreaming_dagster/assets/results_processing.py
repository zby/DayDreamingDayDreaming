from dagster import MetadataValue
from ._decorators import asset_with_boundary
from pathlib import Path
from ..config.paths import Paths
import pandas as pd
from ..utils.evaluation_processing import calculate_evaluation_metadata


def aggregated_scores_impl(data_root: Path, *, scores_aggregator, membership_service) -> pd.DataFrame:
    """Pure implementation for aggregated_scores.

    - Reads the evaluation gen_id list from membership_service
    - Delegates aggregation to scores_aggregator.parse_all_scores
    - Returns the resulting DataFrame (no Dagster context required)
    """
    try:
        keep_list = membership_service.stage_gen_ids(data_root, "evaluation")
    except FileNotFoundError:
        keep_list = []
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
)
def aggregated_scores(context) -> pd.DataFrame:
    """Aggregate evaluation scores for the current cohort.

    Source rows come from scripts.aggregate_scores.parse_all, which:
    - Reads data/gens/evaluation/<gen_id>/{parsed.txt,metadata.json}
    - Requires parsed.txt; when missing, emits a row with error="missing parsed.txt"
    - Emits enriched columns required downstream (combo_id, draft_template, generation_template,
      generation_model, generation_response_path, evaluation_llm_model, cohort_id).
    This asset simply filters that cross-experiment aggregation down to the current cohort.
    """
    data_root = Paths.from_context(context).data_root
    out_csv = data_root / "5_parsing" / "aggregated_scores.csv"
    df = aggregated_scores_impl(
        data_root,
        scores_aggregator=context.resources.scores_aggregator,
        membership_service=context.resources.membership_service,
    )

    md = calculate_evaluation_metadata(df)
    md.update({
        "output": MetadataValue.path(str(out_csv)),
        "enriched": MetadataValue.bool(False),
    })
    context.add_output_metadata(md)
    return df
