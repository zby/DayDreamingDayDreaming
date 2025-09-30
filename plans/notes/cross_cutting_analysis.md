# Cross-Cutting Analysis — Simplification Review

## Error Handling & Codes
- Structured errors already flow through `DDError` but enforcement is uneven. Assets like `src/daydreaming_dagster/assets/group_cohorts.py:274` emit repetitive `DDError` blocks; centralising validation helpers would reduce duplication while keeping `Err` codes intact.
- Utilities such as `src/daydreaming_dagster/utils/generation.py:33` still raise generic exceptions via filesystem errors; route all IO paths through `DDError` to maintain boundary expectations.
- Schedules/checks sometimes fall back to silent `None` returns (e.g., `_resolve_doc_id` in `checks/documents_checks.py:25`); when simplifying, re-emit structured errors or explicitly document benign fallbacks.

## Complexity Hotspots
- `cohort_membership` (`src/daydreaming_dagster/assets/group_cohorts.py:528`) remains the dominant cyclomatic hotspot (Radon F63). Break into mode-specific builders and share deterministic-ID helpers to trim branches.
- Pivot assets (`src/daydreaming_dagster/assets/results_summary.py:19`) contain repeated Pandas boilerplate; extracting a utility for multi-stat pivots would shrink repeated logic across results analysis.
- Unified stage helpers (`src/daydreaming_dagster/unified/stage_inputs.py:30`, `stage_parsed.py:17`) duplicate validations; folding them into shared utilities will reduce branch count without altering behaviour.

## Testing Gaps
- Data-layer unit coverage ends at `tests/data_layer/test_gens_data_layer.py`; add tests for `Paths.from_context` failure cases and deterministic signature helpers before refactors.
- Orchestration shell lacks a lightweight smoke test confirming asset/check/schedule registration after automated wiring changes.
- Combo ID determinism currently only covered via `assets/tests/test_utilities_unit.py`; once models switch to `generate_combo_id`, add a direct model-level regression.

## Logging & Boundaries
- Error logging primarily funnels through `with_asset_error_boundary` (`assets/_error_boundary.py:11`), but schedules (`schedules/raw_schedule.py:64`) and resources (`resources/llm_client.py:142`) log directly. Consolidate user-facing logs at boundaries to avoid duplicate messages.
- IO managers and data layer write metadata directly; consider a single logging hook (e.g., when seeding metadata or registering partitions) to make audit trails easier while keeping hot paths quiet.

## Shared Infrastructure Opportunities
- Promote `Paths`/`GensDataLayer` as Dagster resources so assets stop instantiating them per invocation (`definitions.py:126`, unified stage modules). This aligns path handling, simplifies testing, and enables future data-root overrides.
- Centralise deterministic ID utilities and signature lookups (`utils/ids.py`, `utils/cohort_scope.py`, `assets/group_cohorts.py`) to prevent drift between generation and validation logic.
