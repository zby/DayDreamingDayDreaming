from dagster import AssetKey

from daydreaming_dagster.assets.partitions import (
    cohort_reports_partitions,
    cohort_spec_partitions,
)
from daydreaming_dagster.assets.group_cohorts import (
    cohort_id,
    cohort_membership,
    content_combinations,
    register_cohort_partitions,
    selected_combo_mappings,
)
from daydreaming_dagster.assets.results_processing import cohort_aggregated_scores
from daydreaming_dagster.assets.results_summary import (
    generation_scores_pivot,
    final_results,
    perfect_score_paths,
    evaluation_model_template_pivot,
)
from daydreaming_dagster.assets.results_analysis import (
    comprehensive_variance_analysis,
)


COHORT_REPORT_ASSETS = [
    cohort_aggregated_scores,
    generation_scores_pivot,
    final_results,
    perfect_score_paths,
    evaluation_model_template_pivot,
    comprehensive_variance_analysis,
]


def test_cohort_report_assets_use_cohort_partitions():
    """All cohort report assets must depend on cohort_id and share the cohort report partitions."""
    cohort_id_key = AssetKey("cohort_id")
    for asset in COHORT_REPORT_ASSETS:
        assert asset.partitions_def is cohort_reports_partitions, asset.keys
        assert cohort_id_key in asset.dependency_keys, asset.keys


def test_cohort_core_assets_use_cohort_spec_partitions():
    assets = [
        cohort_id,
        cohort_membership,
        selected_combo_mappings,
        content_combinations,
        register_cohort_partitions,
    ]

    for asset in assets:
        assert asset.partitions_def is cohort_spec_partitions
