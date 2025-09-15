from __future__ import annotations

from pathlib import Path
from dagster import Failure, MetadataValue
from ..utils.membership_lookup import (
    stage_gen_ids as _stage_gen_ids,
    find_membership_row_by_gen as _find_row,
)


class MembershipServiceResource:
    """Cohort membership lookups (injectable for testing).

    Provides stage_gen_ids(data_root, stage) that assets can call without
    importing utils directly. Tests can replace this with a stub.
    """

    def stage_gen_ids(self, data_root: Path, stage: str) -> list[str]:
        return list(_stage_gen_ids(Path(data_root), stage))

    # New helpers for row access
    def get_row(self, data_root: Path, stage: str, gen_id: str):
        return _find_row(Path(data_root), stage, gen_id)

    def require_row(self, data_root: Path, stage: str, gen_id: str, *, require_columns: list[str] | tuple[str, ...] = ( )):
        row, cohort_id = self.get_row(Path(data_root), stage, gen_id)
        if row is None:
            raise Failure(
                description="Membership row not found for generation",
                metadata={
                    "function": MetadataValue.text("membership_service.require_row"),
                    "stage": MetadataValue.text(str(stage)),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "resolution": MetadataValue.text(
                        "Ensure data/cohorts/*/membership.csv contains this stage/gen_id"
                    ),
                },
            )
        missing: list[str] = []
        for c in list(require_columns or ()):  # normalize Iterable -> list
            if c not in row.index:
                missing.append(str(c))
            else:
                val = row.get(c)
                # Treat None, empty string, and NaN as missing
                import pandas as _pd
                if val is None or (isinstance(val, str) and not val.strip()) or (_pd.isna(val)):
                    missing.append(str(c))
        if missing:
            raise Failure(
                description="Membership row missing required columns (missing_columns)",
                metadata={
                    "function": MetadataValue.text("membership_service.require_row"),
                    "stage": MetadataValue.text(str(stage)),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "missing_columns": MetadataValue.json(missing),
                    "resolution": MetadataValue.text(
                        "Populate required fields in cohort membership (e.g., llm_model_id, template_id)"
                    ),
                },
            )
        return row, cohort_id


__all__ = ["MembershipServiceResource"]
