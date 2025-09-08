from dagster import asset, MetadataValue
from pathlib import Path
import pandas as pd
import numpy as np
from scripts.parse_all_scores import parse_all as parse_all_scores


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"data_root"},
    description="Parse evaluation scores from docs store (data/docs/evaluation/<doc_id>/parsed.txt)",
    compute_kind="pandas",
)
def parsed_scores(context) -> pd.DataFrame:
    """Parse evaluation scores from the docs store and write a consolidated DataFrame.

    Reads data/docs/evaluation/<doc_id>/{parsed.txt,metadata.json} and uses
    evaluation_templates.csv to select the parser when parsed.txt is missing.
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    out_csv = data_root / "5_parsing" / "parsed_scores.csv"
    df = parse_all_scores(data_root, out_csv)
    context.add_output_metadata({
        "rows": MetadataValue.int(int(df.shape[0]) if hasattr(df, "shape") else 0),
        "output": MetadataValue.path(str(out_csv)),
    })
    return df
