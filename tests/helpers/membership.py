from __future__ import annotations

from pathlib import Path
import pandas as pd


def write_membership_csv(base: Path, rows: list[dict], cohort: str = "T-INTEG") -> Path:
    """Write a minimal membership.csv under base/cohorts/<cohort>/.

    Normalizes rows to canonical columns used by tests and assets.
    Returns the path to the written CSV.
    """
    cols = [
        "stage",
        "gen_id",
        "cohort_id",
        "parent_gen_id",
        "combo_id",
        "template_id",
        "llm_model_id",
    ]
    out_rows: list[dict] = []
    for r in rows:
        d = {k: r.get(k, "") for k in cols}
        if not d["cohort_id"]:
            d["cohort_id"] = cohort
        out_rows.append(d)
    df = pd.DataFrame(out_rows, columns=cols)
    cdir = Path(base) / "cohorts" / cohort
    cdir.mkdir(parents=True, exist_ok=True)
    path = cdir / "membership.csv"
    path.write_text(df.to_csv(index=False), encoding="utf-8")
    return path

