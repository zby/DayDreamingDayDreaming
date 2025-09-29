from __future__ import annotations

from pathlib import Path
from typing import List, Optional
import pandas as pd


def _iter_membership_paths(data_root: Path) -> list[Path]:
    root = Path(data_root) / "cohorts"
    if not root.exists():
        return []
    out: list[Path] = []
    for child in sorted(root.iterdir()):
        if child.is_dir():
            m = child / "membership.csv"
            if m.exists():
                out.append(m)
    return out


def stage_gen_ids(
    data_root: str | Path, stage: str, cohort_id: Optional[str] = None
) -> List[str]:
    """Return all gen_ids for a given stage across cohort membership CSVs.

    If cohort_id is provided, limit to that cohort only. Returns an empty list
    if no membership CSVs exist or no matching rows are found.
    """
    base = Path(data_root)
    stage_norm = (stage or "").strip().lower()
    out: list[str] = []
    roots: list[Path] = []
    croot = base / "cohorts"
    if not croot.exists():
        return out
    if cohort_id:
        roots = [croot / str(cohort_id)] if (croot / str(cohort_id)).exists() else []
    else:
        roots = [p for p in sorted(croot.iterdir()) if p.is_dir()]
    for r in roots:
        m = r / "membership.csv"
        if not m.exists():
            continue
        df = pd.read_csv(m, usecols=["stage", "gen_id"])
        if df.empty:
            continue
        ids = (
            df[df["stage"].astype(str).str.lower() == stage_norm]["gen_id"].astype(str).dropna().tolist()
        )
        if ids:
            out.extend(ids)
    # Deduplicate preserving order
    seen = set()
    uniq: list[str] = []
    for gid in out:
        if gid not in seen:
            seen.add(gid)
            uniq.append(gid)
    return uniq
