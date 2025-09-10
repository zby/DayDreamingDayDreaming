from __future__ import annotations

from dagster import asset_check, AssetCheckResult, MetadataValue
from pathlib import Path
import os

from daydreaming_dagster.assets.group_generation_draft import draft_response as draft_response_asset
from daydreaming_dagster.assets.group_generation_essays import essay_response as essay_response_asset
from daydreaming_dagster.assets.group_evaluation import evaluation_response as evaluation_response_asset
import pandas as pd
from ..constants import DRAFT, ESSAY, EVALUATION, FILE_PARSED


"""Asset checks for gens-store outputs.

These checks assert that parsed.txt exists under data/gens/<stage>/<gen_id>.
"""


def _get_pk(context):
    return getattr(context, "partition_key", None) or getattr(context, "asset_partition_key", None)


def _resolve_doc_id(tasks_csv: Path, key_col: str, key: str) -> str | None:
    try:
        if not tasks_csv.exists():
            return None
        df = pd.read_csv(tasks_csv)
        if key_col not in df.columns or "doc_id" not in df.columns:
            return None
        row = df[df[key_col].astype(str) == str(key)]
        if row.empty:
            return None
        val = row.iloc[0].get("doc_id")
        return str(val) if isinstance(val, (str, int)) and str(val) else None
    except Exception:
        return None


@asset_check(asset=draft_response_asset, required_resource_keys={"data_root"})
def draft_files_exist_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    data_root = Path(getattr(context.resources, "data_root", "data"))
    base = data_root / "gens" / DRAFT / str(pk)
    ok = (base / FILE_PARSED).exists()
    return AssetCheckResult(passed=bool(ok), metadata={"gen_dir": MetadataValue.path(str(base))})


@asset_check(asset=essay_response_asset, required_resource_keys={"data_root"})
def essay_files_exist_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    data_root = Path(getattr(context.resources, "data_root", "data"))
    base = data_root / "gens" / ESSAY / str(pk)
    ok = (base / FILE_PARSED).exists()
    return AssetCheckResult(passed=bool(ok), metadata={"gen_dir": MetadataValue.path(str(base))})


@asset_check(asset=evaluation_response_asset, required_resource_keys={"data_root"})
def evaluation_files_exist_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    data_root = Path(getattr(context.resources, "data_root", "data"))
    base = data_root / "gens" / EVALUATION / str(pk)
    ok = (base / FILE_PARSED).exists()
    return AssetCheckResult(passed=bool(ok), metadata={"gen_dir": MetadataValue.path(str(base))})


# DB row-present checks removed under filesystem-only design
