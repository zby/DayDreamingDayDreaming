from __future__ import annotations

from dagster import observable_source_asset, AssetExecutionContext, ObserveResult, DataVersion
from pathlib import Path
from hashlib import sha256
from typing import Iterable


def _fingerprint_paths(paths: Iterable[Path]) -> str:
    """Compute a stable fingerprint over files and directories.

    - For directories, include all regular files under them (recursive), sorted by relative path.
    - Uses path relative name + mtime nanoseconds as a fast, reliable change detector.
      (If you want bit-for-bit robustness, switch to reading file contents.)
    """
    files: list[Path] = []
    for p in paths:
        p = Path(p)
        if p.is_file():
            files.append(p)
        elif p.is_dir():
            files.extend([f for f in p.rglob("*") if f.is_file()])
        else:
            # Non-existent path should still influence the version deterministically
            # by contributing a marker
            files.append(p)

    # Sort by relative path from repo root for stable ordering
    root = Path(".").resolve()
    files_sorted = sorted(files, key=lambda f: str(f.resolve().relative_to(root)) if f.exists() else str(f))

    h = sha256()
    for f in files_sorted:
        rel = str((f.resolve()).relative_to(root)) if f.exists() else f"MISSING::{str(f)}"
        h.update(rel.encode("utf-8"))
        try:
            if f.exists():
                h.update(str(f.stat().st_mtime_ns).encode("utf-8"))
            else:
                h.update(b"missing")
        except Exception:
            # Be resilient to odd FS errors; still produce a fingerprint
            h.update(b"stat_error")
    return h.hexdigest()


def _raw_concepts_source(context: AssetExecutionContext) -> ObserveResult:
    base = Path("data/1_raw/concepts")
    fingerprint = _fingerprint_paths([
        base / "concepts_metadata.csv",
        base / "descriptions-sentence",
        base / "descriptions-paragraph",
        base / "descriptions-article",
    ])
    return ObserveResult(
        metadata={"path": str(base), "fingerprint": fingerprint},
        data_version=DataVersion(fingerprint),
    )


def _llm_models_source(context: AssetExecutionContext) -> ObserveResult:
    p = Path("data/1_raw/llm_models.csv")
    fingerprint = _fingerprint_paths([p])
    return ObserveResult(
        metadata={"path": str(p), "fingerprint": fingerprint},
        data_version=DataVersion(fingerprint),
    )


def _link_templates_source(context: AssetExecutionContext) -> ObserveResult:
    csv_path = Path("data/1_raw/link_templates.csv")
    dir_path = Path("data/1_raw/generation_templates/links")
    fingerprint = _fingerprint_paths([csv_path, dir_path])
    return ObserveResult(
        metadata={
            "csv": str(csv_path),
            "dir": str(dir_path),
            "fingerprint": fingerprint,
        },
        data_version=DataVersion(fingerprint),
    )


def _essay_templates_source(context: AssetExecutionContext) -> ObserveResult:
    csv_path = Path("data/1_raw/essay_templates.csv")
    dir_path = Path("data/1_raw/generation_templates/essay")
    fingerprint = _fingerprint_paths([csv_path, dir_path])
    return ObserveResult(
        metadata={
            "csv": str(csv_path),
            "dir": str(dir_path),
            "fingerprint": fingerprint,
        },
        data_version=DataVersion(fingerprint),
    )


def _evaluation_templates_source(context: AssetExecutionContext) -> ObserveResult:
    csv_path = Path("data/1_raw/evaluation_templates.csv")
    dir_path = Path("data/1_raw/evaluation_templates")
    fingerprint = _fingerprint_paths([csv_path, dir_path])
    return ObserveResult(
        metadata={
            "csv": str(csv_path),
            "dir": str(dir_path),
            "fingerprint": fingerprint,
        },
        data_version=DataVersion(fingerprint),
    )


# Expose Dagster observable source assets with expected names
raw_concepts_source = observable_source_asset(name="raw_concepts_source")(_raw_concepts_source)
llm_models_source = observable_source_asset(name="llm_models_source")(_llm_models_source)
link_templates_source = observable_source_asset(name="link_templates_source")(_link_templates_source)
essay_templates_source = observable_source_asset(name="essay_templates_source")(_essay_templates_source)
evaluation_templates_source = observable_source_asset(name="evaluation_templates_source")(_evaluation_templates_source)
