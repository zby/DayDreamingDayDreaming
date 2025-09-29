from __future__ import annotations

from pathlib import Path
import pandas as pd


def write_membership_csv(base: Path, rows: list[dict], cohort: str = "T-INTEG") -> Path:
    """Write a slim membership.csv (stage/gen_id) under base/cohorts/<cohort>.

    Additional keys in ``rows`` are ignored; callers should persist detailed metadata
    via gens-store files instead. Returns the path to the written CSV.
    """

    slim_rows: list[dict[str, str]] = []
    for entry in rows:
        stage = str(entry.get("stage") or "").strip()
        gen_id = str(entry.get("gen_id") or "").strip()
        if not stage or not gen_id:
            raise ValueError("membership rows require both 'stage' and 'gen_id'")
        slim_rows.append({"stage": stage, "gen_id": gen_id})

    df = pd.DataFrame(slim_rows, columns=["stage", "gen_id"])
    df = df.drop_duplicates(subset=["stage", "gen_id"], keep="first")

    cohort_dir = Path(base) / "cohorts" / cohort
    cohort_dir.mkdir(parents=True, exist_ok=True)
    path = cohort_dir / "membership.csv"
    path.write_text(df.to_csv(index=False), encoding="utf-8")
    return path
