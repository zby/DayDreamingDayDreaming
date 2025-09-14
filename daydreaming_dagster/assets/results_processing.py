from dagster import asset, MetadataValue
from pathlib import Path
import pandas as pd
from scripts.aggregate_scores import parse_all as parse_all_scores
from ..utils.membership_lookup import stage_gen_ids


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"data_root"},
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
      generation_model, stage, generation_response_path, evaluation_llm_model).
    This asset simply filters that cross-experiment aggregation down to the current cohort.
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    out_csv = data_root / "5_parsing" / "aggregated_scores.csv"
    df = parse_all_scores(data_root, out_csv)

    # Filter to evaluation gen_ids found in cohort membership (if any)
    try:
        keep_list = stage_gen_ids(data_root, "evaluation")
        keep = set(keep_list)
        if isinstance(df, pd.DataFrame) and not df.empty and "gen_id" in df.columns and keep:
            df = df[df["gen_id"].astype(str).isin(keep)].reset_index(drop=True)
    except FileNotFoundError:
        # No cohort membership present; keep full set
        pass

    context.add_output_metadata(
        {
            "rows": MetadataValue.int(int(df.shape[0]) if hasattr(df, "shape") else 0),
            "output": MetadataValue.path(str(out_csv)),
            "enriched": MetadataValue.bool(False),  # passthrough of aggregator enrichments
        }
    )
    return df
