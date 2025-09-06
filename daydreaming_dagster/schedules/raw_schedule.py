from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List

from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    define_asset_job,
)

from ..utils.file_fingerprint import FileEntry, combined_fingerprint, scan_files
from ..utils.raw_state import FingerprintState, read_last_state, write_last_state


RAW_CSVS = [
    Path("data/1_raw/concepts_metadata.csv"),
    Path("data/1_raw/llm_models.csv"),
    Path("data/1_raw/draft_templates.csv"),
    Path("data/1_raw/essay_templates.csv"),
    Path("data/1_raw/evaluation_templates.csv"),
]


# After removing intermediate raw assets, recompute task definitions on CSV change
raw_reload_job = define_asset_job(
    "raw_reload_job",
    selection=AssetSelection.groups("task_definitions"),
)


def _cron() -> str:
    return os.getenv("RAW_SCAN_CRON", "*/5 * * * *")


def _dagster_home() -> Path:
    return Path(os.getenv("DAGSTER_HOME", "dagster_home"))


def _build_state(base: Path, files: List[Path]) -> FingerprintState:
    entries = scan_files(base, files)
    by_file = {e.rel_path: e for e in entries}
    return FingerprintState(combined=combined_fingerprint(entries), by_file=by_file)


def _changed_files(prev: FingerprintState | None, curr: FingerprintState) -> List[str]:
    if prev is None:
        return sorted(curr.by_file.keys())
    changed: List[str] = []
    for rel, entry in curr.by_file.items():
        if rel not in prev.by_file:
            changed.append(rel)
            continue
        p = prev.by_file[rel]
        if p.size != entry.size or p.mtime_ns != entry.mtime_ns:
            changed.append(rel)
    return changed


def raw_schedule_execution_fn(context):
    base = Path("data/1_raw")
    curr = _build_state(base, RAW_CSVS)
    last = read_last_state(_dagster_home())
    if last and last.combined == curr.combined:
        return SkipReason("no raw CSV changes")

    changed = _changed_files(last, curr)
    short = curr.combined
    tags = {
        "trigger": "raw_schedule",
        "raw_changed_files": ",".join(changed[:5]),
        "raw_fingerprint": short,
    }
    # Persist immediately; if the run fails, we won't retrigger for the same change.
    write_last_state(_dagster_home(), curr)
    return RunRequest(run_key=short, tags=tags)


raw_schedule = ScheduleDefinition(
    name="raw_schedule",
    job=raw_reload_job,
    cron_schedule=_cron(),
    execution_fn=raw_schedule_execution_fn,
)
