from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Optional

from .file_fingerprint import FileEntry


@dataclass
class FingerprintState:
    combined: str
    by_file: Dict[str, FileEntry]


def _state_dir(dagster_home: Path) -> Path:
    d = dagster_home / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def read_last_state(dagster_home: Path) -> Optional[FingerprintState]:
    p = _state_dir(dagster_home) / "raw_fingerprint.json"
    if not p.exists():
        return None
    data = json.loads(p.read_text())
    by_file = {k: FileEntry(**v) for k, v in data["by_file"].items()}
    return FingerprintState(combined=data["combined"], by_file=by_file)


def write_last_state(dagster_home: Path, state: FingerprintState) -> None:
    p = _state_dir(dagster_home) / "raw_fingerprint.json"
    payload = {
        "combined": state.combined,
        "by_file": {k: asdict(v) for k, v in state.by_file.items()},
    }
    p.write_text(json.dumps(payload, indent=2))

