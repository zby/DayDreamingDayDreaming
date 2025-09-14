from __future__ import annotations

from dagster import asset_check, AssetCheckResult, MetadataValue
from pathlib import Path
import os

from daydreaming_dagster.assets.group_generation_draft import draft_response as draft_response_asset
from daydreaming_dagster.assets.group_generation_essays import essay_response as essay_response_asset
from daydreaming_dagster.assets.group_evaluation import evaluation_response as evaluation_response_asset
import pandas as pd
from ..constants import FILE_PARSED
from ..types import Stage


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



def _files_exist_check_impl(context, stage: Stage) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    data_root = Path(getattr(context.resources, "data_root", "data"))
    base = data_root / "gens" / stage / str(pk)
    ok = (base / FILE_PARSED).exists()
    return AssetCheckResult(passed=bool(ok), metadata={"gen_dir": MetadataValue.path(str(base))})


def _make_files_exist_check(stage: Stage):
    def _check(context) -> AssetCheckResult:
        return _files_exist_check_impl(context, stage)

    return _check


# Register checks for each stage using the generic implementation and the global STAGES list
_ASSET_BY_STAGE = {
    "draft": draft_response_asset,
    "essay": essay_response_asset,
    "evaluation": evaluation_response_asset,
}

for _stage, _asset in _ASSET_BY_STAGE.items():
    _fn = asset_check(asset=_asset, required_resource_keys={"data_root"})(_make_files_exist_check(_stage))
    globals()[f"{_stage}_files_exist_check"] = _fn

del _fn


# DB row-present checks removed under filesystem-only design
