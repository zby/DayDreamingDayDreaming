from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, List
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


def find_membership_row_by_gen(
    data_root: str | Path, stage: str, gen_id: str
) -> Tuple[Optional[pd.Series], Optional[str]]:
    """Locate the membership row for (stage, gen_id) across all cohorts.

    Returns (row, cohort_id) or (None, None) if not found.
    """
    if not isinstance(gen_id, str) or not gen_id:
        return None, None
    stage_norm = (stage or "").strip().lower()
    base = Path(data_root)
    for mpath in _iter_membership_paths(base):
        try:
            # Fast scan: only stage/gen_id
            df = pd.read_csv(mpath, usecols=["stage", "gen_id"])
            mask = (df["gen_id"].astype(str) == str(gen_id)) & (df["stage"].astype(str).str.lower() == stage_norm)
            if mask.any():
                # Read full row
                full = pd.read_csv(mpath)
                row = full[mask].iloc[0]
                cohort_id = mpath.parent.name
                return row, cohort_id
        except Exception:
            continue
    return None, None


def find_parent_membership_row(
    data_root: str | Path, parent_stage: str, parent_gen_id: str
) -> Tuple[Optional[pd.Series], Optional[str]]:
    """Locate the parent membership row by parent_gen_id across all cohorts.

    Returns (row, cohort_id) or (None, None) if not found.
    """
    return find_membership_row_by_gen(data_root, parent_stage, parent_gen_id)


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
        try:
            df = pd.read_csv(m)
            if {"stage", "gen_id"}.issubset(df.columns):
                ids = (
                    df[df["stage"].astype(str).str.lower() == stage_norm]["gen_id"].astype(str).dropna().tolist()
                )
                if ids:
                    out.extend(ids)
        except Exception:
            continue
    # Deduplicate preserving order
    seen = set()
    uniq: list[str] = []
    for gid in out:
        if gid not in seen:
            seen.add(gid)
            uniq.append(gid)
    return uniq
