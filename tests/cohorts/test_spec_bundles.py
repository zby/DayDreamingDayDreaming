from __future__ import annotations

from pathlib import Path

import pytest

from daydreaming_dagster.cohorts import build_spec_catalogs, load_cohort_definition


DATA_ROOT = Path(__file__).resolve().parents[2] / "data"
COHORTS_ROOT = DATA_ROOT / "cohorts"


def _spec_directories() -> list[Path]:
    if not COHORTS_ROOT.exists():
        return []
    results: list[Path] = []
    for candidate in COHORTS_ROOT.iterdir():
        spec_dir = candidate / "spec"
        config = spec_dir / "config.yaml"
        if spec_dir.is_dir() and config.is_file():
            results.append(spec_dir)
    return sorted(results)


SPEC_DIRS = _spec_directories()

if not SPEC_DIRS:
    pytest.skip("No cohort specs found under data/cohorts", allow_module_level=True)


@pytest.mark.parametrize("spec_dir", SPEC_DIRS, ids=lambda path: path.parent.name)
def test_spec_bundle_compiles(spec_dir: Path) -> None:
    catalogs = build_spec_catalogs(DATA_ROOT)  # reuse runtime catalog hydration
    plan = load_cohort_definition(spec_dir, catalogs=catalogs)

    assert plan.drafts or plan.essays or plan.evaluations
