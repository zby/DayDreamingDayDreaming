from __future__ import annotations

from pathlib import Path

import json
import pandas as pd
import pytest

from dagster import build_asset_context

from daydreaming_dagster.assets.group_cohorts import cohort_membership
from daydreaming_dagster.utils import ids as ids_utils
from daydreaming_dagster.utils.ids import (
    draft_signature,
    essay_signature,
    evaluation_signature,
    compute_deterministic_gen_id,
)


@pytest.fixture(autouse=True)
def _reset_flag(monkeypatch):
    monkeypatch.setattr(ids_utils, "DETERMINISTIC_GEN_IDS_ENABLED", False, raising=False)


# ... rest of tests ...
