from __future__ import annotations

import json
import os
import subprocess
from hashlib import blake2s

PIPELINE_VERSION = "2025-09-09"


def short_git_sha() -> str | None:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True)
            .strip()
        )
    except Exception:
        return None


def compute_cohort_id(
    kind: str,
    manifest: dict,
    *,
    mode: str = "deterministic",
    explicit: str | None = None,
) -> str:
    if explicit:
        return explicit
    if mode == "timestamped":
        import datetime

        ts = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%SZ")
        sha = short_git_sha() or "nogit"
        return f"{kind}-{ts}-{sha}"

    stable = {
        "combos": manifest.get("combos"),
        "llms": manifest.get("llms"),
        "templates": manifest.get("templates"),
        "prompt_versions": manifest.get("prompt_versions"),
        "pipeline_version": PIPELINE_VERSION,
    }
    s = json.dumps(stable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    h = blake2s(s, digest_size=6).hexdigest()
    return f"{kind}-{h}"


def get_env_cohort_id() -> str | None:
    return os.environ.get("DD_COHORT") or None


def write_manifest(base_path: str | os.PathLike[str], cohort_id: str, manifest: dict) -> None:
    from pathlib import Path

    root = Path(base_path) / "cohorts" / cohort_id
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

