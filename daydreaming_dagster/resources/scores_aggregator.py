from __future__ import annotations

from pathlib import Path
import pandas as pd

from ..utils.evaluation_scores import aggregate_evaluation_scores_for_ids


class ScoresAggregatorResource:
    """Thin wrapper for score aggregation with explicit dependency injection.

    Default implementation delegates to utils.evaluation_scores.aggregate_evaluation_scores_for_ids,
    but tests can supply a fake resource with the same interface.
    """

    def parse_all_scores(self, data_root: Path, gen_ids: list[str] | None) -> pd.DataFrame:
        return aggregate_evaluation_scores_for_ids(Path(data_root), list(gen_ids or []))


__all__ = ["ScoresAggregatorResource"]

