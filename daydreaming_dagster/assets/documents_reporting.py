from __future__ import annotations

from dagster import asset, MetadataValue
import pandas as pd
from pathlib import Path


@asset(
    group_name="reporting",
    io_manager_key="error_log_io_manager",
    required_resource_keys={"data_root"},
    compute_kind="python",
)
def documents_latest_report(context) -> pd.DataFrame:
    """Export a small CSV snapshot of the latest OK rows per stage.

    - If documents index resource is available and enabled, query SQLite; otherwise emit an empty CSV with headers.
    - Output path: data/7_reporting/documents_latest_report.csv via CSVIOManager.
    """
    try:
        idx_res = getattr(context.resources, "documents_index", None)
        if idx_res and getattr(idx_res, "index_enabled", False):
            idx = idx_res.get_index()
            con = idx.connect()
            # Select latest rows by stage/logical key using rowid tie-breaker
            q = (
                "SELECT d1.* FROM documents d1 JOIN ("
                "  SELECT stage, logical_key_id, MAX(created_at) AS mx FROM documents WHERE status='ok' GROUP BY stage, logical_key_id"
                ") t ON d1.stage=t.stage AND d1.logical_key_id=t.logical_key_id AND d1.created_at=t.mx"
            )
            rows = list(con.execute(q))
            df = pd.DataFrame(rows) if rows else pd.DataFrame(
                columns=[
                    "doc_id","logical_key_id","stage","task_id","parent_doc_id","template_id","model_id","run_id","prompt_path","parser","status","usage_prompt_tokens","usage_completion_tokens","usage_max_tokens","created_at","doc_dir","raw_chars","parsed_chars","content_hash","meta_small","lineage_prev_doc_id"
                ]
            )
            context.add_output_metadata({
                "rows": MetadataValue.int(len(df)),
                "source": MetadataValue.text("documents.sqlite"),
            })
            return df
    except Exception as e:
        context.log.warning(f"documents_latest_report: falling back to empty output due to: {e}")
    # Fallback: empty CSV with headers
    df = pd.DataFrame(
        columns=[
            "doc_id","logical_key_id","stage","task_id","parent_doc_id","template_id","model_id","run_id","prompt_path","parser","status","usage_prompt_tokens","usage_completion_tokens","usage_max_tokens","created_at","doc_dir","raw_chars","parsed_chars","content_hash","meta_small","lineage_prev_doc_id"
        ]
    )
    context.add_output_metadata({
        "rows": MetadataValue.int(0),
        "source": MetadataValue.text("empty"),
    })
    return df

