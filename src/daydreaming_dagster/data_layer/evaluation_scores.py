from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.evaluation_scores import (
    NORMALIZED_EVALUATION_SCORE_COLUMNS,
)

__all__ = [
    "write_normalized_evaluation_scores",
    "read_normalized_evaluation_scores",
    "normalized_scores_path",
]


def normalized_scores_path(paths: Paths, cohort_id: str) -> Path:
    return paths.evaluation_scores_normalized_csv(cohort_id)


def write_normalized_evaluation_scores(
    paths: Paths,
    cohort_id: str,
    dataframe: pd.DataFrame,
) -> Path:
    target = normalized_scores_path(paths, cohort_id)
    target.parent.mkdir(parents=True, exist_ok=True)

    normalized = dataframe.copy()
    for column in NORMALIZED_EVALUATION_SCORE_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized = normalized[NORMALIZED_EVALUATION_SCORE_COLUMNS]
    normalized.to_csv(target, index=False)
    return target


def read_normalized_evaluation_scores(paths: Paths, cohort_id: str) -> pd.DataFrame:
    target = normalized_scores_path(paths, cohort_id)
    if not target.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={"cohort_id": cohort_id, "artifact": "evaluation_scores", "path": str(target)},
        )
    df = pd.read_csv(target)
    missing = [col for col in NORMALIZED_EVALUATION_SCORE_COLUMNS if col not in df.columns]
    if missing:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "cohort_id": cohort_id,
                "reason": "normalized_scores_missing_columns",
                "missing": missing,
                "path": str(target),
            },
        )
    return df[NORMALIZED_EVALUATION_SCORE_COLUMNS]
