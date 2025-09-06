from __future__ import annotations

from dagster import asset_check, AssetCheckResult, MetadataValue
from pathlib import Path

from daydreaming_dagster.assets.group_generation_draft import draft_response as draft_response_asset
from daydreaming_dagster.assets.group_generation_essays import essay_response as essay_response_asset
from daydreaming_dagster.assets.group_evaluation import evaluation_response as evaluation_response_asset


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


@asset_check(asset=draft_response_asset, required_resource_keys={"data_root"})
def draft_files_exist_check(context) -> AssetCheckResult:
    pk = context.partition_key
    data_root = Path(context.resources.data_root)
    fs_dir = data_root / "3_generation" / "draft_responses"
    fs_file = _latest_versioned(fs_dir, pk)
    used = None
    if fs_file:
        used = f"fs:{fs_file.name}"
        return AssetCheckResult(passed=True, metadata={"used": MetadataValue.text(used), "path": MetadataValue.path(str(fs_file))})

    # Try documents index if available and enabled
    try:
        idx_res = getattr(context.resources, "documents_index", None)
        if idx_res and getattr(idx_res, "index_enabled", False):
            idx = idx_res.get_index()
            row = idx.get_latest_by_task("draft", pk)
            if row:
                base = idx.resolve_doc_dir(row)
                raw = base / "raw.txt"
                if raw.exists():
                    used = f"db:{base.name}"
                    return AssetCheckResult(passed=True, metadata={"used": MetadataValue.text(used), "doc_dir": MetadataValue.path(str(base))})
    except Exception as e:
        context.log.warning(f"draft_files_exist_check: DB probe failed: {e}")
    return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})


@asset_check(asset=essay_response_asset, required_resource_keys={"data_root"})
def essay_files_exist_check(context) -> AssetCheckResult:
    pk = context.partition_key
    data_root = Path(context.resources.data_root)
    fs_dir = data_root / "3_generation" / "essay_responses"
    fs_file = _latest_versioned(fs_dir, pk)
    if fs_file:
        return AssetCheckResult(passed=True, metadata={"path": MetadataValue.path(str(fs_file))})
    try:
        idx_res = getattr(context.resources, "documents_index", None)
        if idx_res and getattr(idx_res, "index_enabled", False):
            idx = idx_res.get_index()
            row = idx.get_latest_by_task("essay", pk)
            if row:
                base = idx.resolve_doc_dir(row)
                raw = base / "raw.txt"
                if raw.exists():
                    return AssetCheckResult(passed=True, metadata={"doc_dir": MetadataValue.path(str(base))})
    except Exception as e:
        context.log.warning(f"essay_files_exist_check: DB probe failed: {e}")
    return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})


@asset_check(asset=evaluation_response_asset, required_resource_keys={"data_root"})
def evaluation_files_exist_check(context) -> AssetCheckResult:
    pk = context.partition_key
    data_root = Path(context.resources.data_root)
    fs_dir = data_root / "4_evaluation" / "evaluation_responses"
    fs_file = _latest_versioned(fs_dir, pk)
    if fs_file:
        return AssetCheckResult(passed=True, metadata={"path": MetadataValue.path(str(fs_file))})
    try:
        idx_res = getattr(context.resources, "documents_index", None)
        if idx_res and getattr(idx_res, "index_enabled", False):
            idx = idx_res.get_index()
            row = idx.get_latest_by_task("evaluation", pk)
            if row:
                base = idx.resolve_doc_dir(row)
                raw = base / "raw.txt"
                if raw.exists():
                    return AssetCheckResult(passed=True, metadata={"doc_dir": MetadataValue.path(str(base))})
    except Exception as e:
        context.log.warning(f"evaluation_files_exist_check: DB probe failed: {e}")
    return AssetCheckResult(passed=False, metadata={"partition_key": MetadataValue.text(pk)})
