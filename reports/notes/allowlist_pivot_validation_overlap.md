# Allowlist & Pivot Validation Overlap

## Snapshot
- Earlier duplication saw both results assets and the CLI script rebuild catalogs, guard spec paths, and repeat empty-allowlist checks. With `load_cohort_allowlists` in place (`src/daydreaming_dagster/cohorts/spec_planner.py:237`), callers now share the guard rails and evaluation-axis validation. Assets can optionally opt-out of hard failures while scripts still fail fast.
- `CohortDefinitionAllowlists` now exposes `has_evaluation_axes()` plus list helpers (`src/daydreaming_dagster/cohorts/spec_planner.py:204`), removing ad-hoc tuple→list conversions and empty checks scattered across callers.
- The results-summary asset consumes the new helper and uses the list accessors for a single normalization point (`src/daydreaming_dagster/assets/results_summary.py:54`). The pivot-building script routes through the same helper (with optional pre-loaded definitions) for consistent error reporting (`scripts/build_pivot_tables.py:115`).
- Pivot aggregation logic is consolidated via `_pivot_tables` and `_combine_pivot_tables` (`src/daydreaming_dagster/results_summary/transformations.py:70`), eliminating duplicate mean/min/max/count code paths and aligning coverage metrics across generation and evaluation pivots.

## Follow-ups
- Membership planning still applies bespoke task-column validation (`assets/group_cohorts.py:742`) alongside the script’s safeguards. A future sweep could centralize task-column contracts once the cohort engine refactor begins.
- The new helper keeps `require_evaluation_axes` optional. Once downstream callers are comfortable with hard failures, the flag can be removed to simplify semantics further.
