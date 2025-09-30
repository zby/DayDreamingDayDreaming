# Baseline Metrics — src Simplification Review

## Commands
- `XDG_CACHE_HOME=$(pwd)/.cache uv run radon cc src/daydreaming_dagster -s --total-average`
- `HOME=$(pwd)/.sloccache XDG_CACHE_HOME=$(pwd)/.cache uv run sloccount src/daydreaming_dagster`
- `XDG_CACHE_HOME=$(pwd)/.cache .venv/bin/pytest src/daydreaming_dagster -q`

## Summary
- **Radon CC**: average complexity A (≈4.11). Worst hotspot is `src/daydreaming_dagster/assets/group_cohorts.py:cohort_membership` at F (63).
- **SLOCCount**: total Python SLOC 7,174. Largest packages by SLOC — `assets` (3,194), `unified` (1,443), `utils` (1,274).
- **Pytest**: 88 tests passed (≈2.9s). 1 pydantic deprecation warning noted.

Raw outputs stored alongside this file:
- `plans/notes/metrics_baseline_radon.txt`
- `plans/notes/metrics_baseline_sloccount.txt`
- `plans/notes/metrics_baseline_pytest.txt`
