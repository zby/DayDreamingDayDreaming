from __future__ import annotations

from dagster import asset_check, AssetCheckResult, MetadataValue
from pathlib import Path
import os

from daydreaming_dagster.assets.group_generation_draft import draft_response as draft_response_asset
from daydreaming_dagster.assets.group_generation_essays import essay_response as essay_response_asset
from daydreaming_dagster.assets.group_evaluation import evaluation_response as evaluation_response_asset
from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex


def _latest_versioned(path: Path, stem: str) -> Path | None:
    if not path.exists():
        return None
    best = None
    best_ver = -1
    prefix = f"{stem}_v"
    for name in path.iterdir():
        if not name.name.startswith(prefix) or not name.suffix == ".txt":
            continue
        try:
            v = int(name.stem.split("_v")[-1])
        except Exception:
            continue
        if v > best_ver:
            best_ver = v
            best = name
    if best is not None and best.exists():
        return best
    legacy = path / f"{stem}.txt"
    return legacy if legacy.exists() else None


def _get_pk(context):
    return getattr(context, "partition_key", None) or getattr(context, "asset_partition_key", None)


@asset_check(asset=draft_response_asset, required_resource_keys={"documents_index"})
def draft_files_exist_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    # DB-only check
    idx_res = getattr(context.resources, "documents_index", None)
    idx = idx_res.get_index() if idx_res else None
    if not idx:
        return AssetCheckResult(passed=False, metadata={"error": MetadataValue.text("documents_index unavailable")})
    row = idx.get_latest_by_task("draft", pk)
    if not row:
        return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})
    base = idx.resolve_doc_dir(row)
    raw = base / "raw.txt"
    if raw.exists():
        return AssetCheckResult(passed=True, metadata={"doc_dir": MetadataValue.path(str(base))})
    return AssetCheckResult(passed=False, metadata={"doc_dir": MetadataValue.path(str(base))})


@asset_check(asset=essay_response_asset, required_resource_keys={"documents_index"})
def essay_files_exist_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    idx_res = getattr(context.resources, "documents_index", None)
    idx = idx_res.get_index() if idx_res else None
    if not idx:
        return AssetCheckResult(passed=False, metadata={"error": MetadataValue.text("documents_index unavailable")})
    row = idx.get_latest_by_task("essay", pk)
    if not row:
        return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})
    base = idx.resolve_doc_dir(row)
    raw = base / "raw.txt"
    if raw.exists():
        return AssetCheckResult(passed=True, metadata={"doc_dir": MetadataValue.path(str(base))})
    return AssetCheckResult(passed=False, metadata={"doc_dir": MetadataValue.path(str(base))})


@asset_check(asset=evaluation_response_asset, required_resource_keys={"documents_index"})
def evaluation_files_exist_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    idx_res = getattr(context.resources, "documents_index", None)
    idx = idx_res.get_index() if idx_res else None
    if not idx:
        return AssetCheckResult(passed=False, metadata={"error": MetadataValue.text("documents_index unavailable")})
    row = idx.get_latest_by_task("evaluation", pk)
    if not row:
        return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})
    base = idx.resolve_doc_dir(row)
    raw = base / "raw.txt"
    if raw.exists():
        return AssetCheckResult(passed=True, metadata={"doc_dir": MetadataValue.path(str(base))})
    return AssetCheckResult(passed=False, metadata={"doc_dir": MetadataValue.path(str(base))})


def _open_index_from_context(context) -> SQLiteDocumentsIndex | None:
    try:
        idx_res = getattr(context.resources, "documents_index", None)
        return idx_res.get_index() if idx_res else None
    except Exception:
        return None


@asset_check(asset=draft_response_asset, required_resource_keys={"documents_index"})
def draft_db_row_present_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    idx = _open_index_from_context(context)
    if not idx:
        return AssetCheckResult(passed=False, metadata={"error": MetadataValue.text("documents_index unavailable")})
    row = idx.get_latest_by_task("draft", pk)
    if not row:
        return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})
    base = idx.resolve_doc_dir(row)
    ok = base.exists()
    return AssetCheckResult(passed=ok, metadata={"doc_dir": MetadataValue.path(str(base))})


@asset_check(asset=essay_response_asset, required_resource_keys={"documents_index"})
def essay_db_row_present_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    idx = _open_index_from_context(context)
    if not idx:
        return AssetCheckResult(passed=False, metadata={"error": MetadataValue.text("documents_index unavailable")})
    row = idx.get_latest_by_task("essay", pk)
    if not row:
        return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})
    base = idx.resolve_doc_dir(row)
    ok = base.exists()
    return AssetCheckResult(passed=ok, metadata={"doc_dir": MetadataValue.path(str(base))})


@asset_check(asset=evaluation_response_asset, required_resource_keys={"documents_index"})
def evaluation_db_row_present_check(context) -> AssetCheckResult:
    pk = _get_pk(context)
    if not pk:
        return AssetCheckResult(passed=True, metadata={"skipped": MetadataValue.text("no partition context")})
    idx = _open_index_from_context(context)
    if not idx:
        return AssetCheckResult(passed=False, metadata={"error": MetadataValue.text("documents_index unavailable")})
    row = idx.get_latest_by_task("evaluation", pk)
    if not row:
        return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})
    base = idx.resolve_doc_dir(row)
    ok = base.exists()
    return AssetCheckResult(passed=ok, metadata={"doc_dir": MetadataValue.path(str(base))})
