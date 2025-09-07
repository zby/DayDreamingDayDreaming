from __future__ import annotations

from dagster import asset, MetadataValue
import pandas as pd
from pathlib import Path
import os
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex


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
        idx = idx_res.get_index() if idx_res else None
        if idx is not None:
            con = idx.connect()
            # Select latest rows by stage/logical key using rowid tie-breaker
            q = (
                "SELECT d1.* FROM documents d1 JOIN ("
                "  SELECT stage, logical_key_id, MAX(created_at) AS mx FROM documents WHERE status='ok' GROUP BY stage, logical_key_id"
                ") t ON d1.stage=t.stage AND d1.logical_key_id=t.logical_key_id AND d1.created_at=t.mx"
            )
            rows = list(con.execute(q))
            if not rows:
                # Fallback: latest rows overall (helpful for smoke and empty groups)
                rows = list(con.execute("SELECT * FROM documents ORDER BY created_at DESC, rowid DESC LIMIT 50"))
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

@asset(
    group_name="reporting",
    io_manager_key="error_log_io_manager",
    required_resource_keys={"data_root"},
    compute_kind="python",
)
def documents_consistency_report(context) -> pd.DataFrame:
    """Scan the documents index and report simple consistency issues per row.

    Columns:
    - doc_id, stage, task_id, doc_dir
    - missing_raw, missing_parsed, missing_prompt
    - dir_exists
    """
    idx: SQLiteDocumentsIndex | None = None
    # Open via resource only (DB-only mode)
    try:
        idx_res = getattr(context.resources, "documents_index", None)
        idx = idx_res.get_index() if idx_res else None
    except Exception:
        idx = None

    if idx is None:
        context.log.info("documents_consistency_report: index disabled or unavailable; emitting empty report")
        return pd.DataFrame(columns=[
            "doc_id","stage","task_id","doc_dir","missing_raw","missing_parsed","missing_prompt","dir_exists"
        ])

    con = idx.connect()
    # Examine recent rows (limit for performance); prefer latest per (stage, logical), then recent
    rows = list(con.execute(
        "SELECT d1.* FROM documents d1 JOIN ("
        "  SELECT stage, logical_key_id, MAX(created_at) AS mx FROM documents GROUP BY stage, logical_key_id"
        ") t ON d1.stage=t.stage AND d1.logical_key_id=t.logical_key_id AND d1.created_at=t.mx"
    ))
    if not rows:
        rows = list(con.execute("SELECT * FROM documents ORDER BY created_at DESC, rowid DESC LIMIT 1000"))

    records: list[dict] = []
    for r in rows:
        base = idx.resolve_doc_dir(r)
        raw = base / "raw.txt"
        parsed = base / "parsed.txt"
        prompt = base / "prompt.txt"
        records.append({
            "doc_id": r.get("doc_id"),
            "stage": r.get("stage"),
            "task_id": r.get("task_id"),
            "doc_dir": str(base),
            "missing_raw": not raw.exists(),
            "missing_parsed": not parsed.exists(),
            "missing_prompt": not prompt.exists(),
            "dir_exists": base.exists(),
        })
    df = pd.DataFrame(records)
    context.add_output_metadata({
        "rows": MetadataValue.int(len(df)),
        "issues": MetadataValue.int(int(df[["missing_raw","missing_parsed","missing_prompt","dir_exists"]].any(axis=1).sum())) if not df.empty else MetadataValue.int(0),
    })
    return df
