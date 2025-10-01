from __future__ import annotations

from dagster import MetadataValue
from ._decorators import asset_with_boundary
import pandas as pd
import json
from ..types import STAGES
from ..data_layer.paths import Paths


@asset_with_boundary(
    stage="reporting",
    group_name="reporting",
    io_manager_key="error_log_io_manager",
    required_resource_keys={"data_root"},
    compute_kind="python",
)
def documents_latest_report(context) -> pd.DataFrame:
    """Export a small CSV snapshot of recent generations by scanning filesystem metadata.

    Output path: data/7_reporting/documents_latest_report.csv via CSVIOManager.
    """
    paths = Paths.from_context(context)
    docs_root = paths.gens_root
    records: list[dict] = []
    for stage in STAGES:
        base = docs_root / stage
        if not base.exists():
            continue
        for doc_dir in base.iterdir():
            if not doc_dir.is_dir():
                continue
            meta_path = paths.metadata_path(stage, doc_dir.name)
            task_id = None
            created_at = None
            if meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(meta, dict):
                    task_id = meta.get("task_id")
                    created_at = meta.get("created_at")
            records.append({
                "gen_id": doc_dir.name,
                "stage": stage,
                "task_id": task_id,
                "created_at": created_at,
                "gen_dir": str(doc_dir),
            })
    df = pd.DataFrame(records)
    context.add_output_metadata({
        "rows": MetadataValue.int(len(df)),
        "source": MetadataValue.text("filesystem"),
    })
    return df

@asset_with_boundary(
    stage="reporting",
    group_name="reporting",
    io_manager_key="error_log_io_manager",
    required_resource_keys={"data_root"},
    compute_kind="python",
)
def documents_consistency_report(context) -> pd.DataFrame:
    """Scan the gens store and report simple consistency issues per row.

    Columns:
    - gen_id, stage, task_id, gen_dir
    - missing_raw, missing_parsed, missing_prompt
    - dir_exists
    """
    paths = Paths.from_context(context)
    docs_root = paths.gens_root
    records: list[dict] = []
    for stage in STAGES:
        base = docs_root / stage
        if not base.exists():
            continue
        for doc_dir in base.iterdir():
            if not doc_dir.is_dir():
                continue
            raw = paths.raw_path(stage, doc_dir.name)
            parsed = paths.parsed_path(stage, doc_dir.name)
            prompt = paths.prompt_path(stage, doc_dir.name)
            task_id = None
            meta_path = paths.metadata_path(stage, doc_dir.name)
            if meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(meta, dict):
                    task_id = meta.get("task_id")
            records.append({
                "gen_id": doc_dir.name,
                "stage": stage,
                "task_id": task_id,
                "gen_dir": str(doc_dir),
                "missing_raw": not raw.exists(),
                "missing_parsed": not parsed.exists(),
                "missing_prompt": not prompt.exists(),
                "dir_exists": doc_dir.exists(),
            })
    df = pd.DataFrame(records)
    context.add_output_metadata({
        "rows": MetadataValue.int(len(df)),
        "issues": MetadataValue.int(int(df[["missing_raw","missing_parsed","missing_prompt","dir_exists"]].any(axis=1).sum())) if not df.empty else MetadataValue.int(0),
    })
    return df
