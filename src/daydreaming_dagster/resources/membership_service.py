from __future__ import annotations

from pathlib import Path
from ..utils.membership_lookup import stage_gen_ids as _stage_gen_ids


class MembershipServiceResource:
    """Cohort membership lookups (injectable for testing).

    Provides stage/scoped lookup helpers so assets can avoid importing the
    lower-level utils directly. Tests can replace this with a stub.
    """

    def stage_gen_ids(
        self,
        data_root: Path,
        stage: str,
        cohort_id: str | None = None,
    ) -> list[str]:
        """Return gen_ids for the given stage.

        When ``cohort_id`` is provided, results are limited to that cohort's
        membership.csv; otherwise all cohorts are scanned.
        """
        return list(_stage_gen_ids(Path(data_root), stage, cohort_id))

__all__ = ["MembershipServiceResource"]
