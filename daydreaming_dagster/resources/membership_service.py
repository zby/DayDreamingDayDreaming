from __future__ import annotations

from pathlib import Path
from ..utils.membership_lookup import stage_gen_ids as _stage_gen_ids


class MembershipServiceResource:
    """Cohort membership lookups (injectable for testing).

    Provides stage_gen_ids(data_root, stage) that assets can call without
    importing utils directly. Tests can replace this with a stub.
    """

    def stage_gen_ids(self, data_root: Path, stage: str) -> list[str]:
        return list(_stage_gen_ids(Path(data_root), stage))


__all__ = ["MembershipServiceResource"]

