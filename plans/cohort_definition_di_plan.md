# CohortDefinition Dependency Injection Plan

## Goal
- Treat spec parsing as a boundary concern: production code, assets, and tests should work with `ExperimentSpec` / `CohortDefinition` objects rather than reading spec files directly.
- Reduce duplicated temp-file handling in tests and enable easier stubbing/mocking of cohort specs.

## Current Pain Points
- `load_cohort_definition` hard-codes path-based loading, so Dagster assets and scripts read from disk every time.
- Test suites (`spec_dsl/tests`, `tests/spec_dsl`, `cohorts/tests`) repeatedly write JSON/YAML files just to call `load_spec`.
- No shared factory for test specs or easy hook for resources to inject parsed specs.

## Workstreams

1. **Loader Enhancements**
   - Add a public `parse_spec_mapping(data: Mapping[str, Any], *, source: Path | None = None) -> ExperimentSpec` in `spec_dsl.loader`.
   - Refactor `load_spec` to delegate to the parser after reading from disk; keep behavior/validation identical.
   - Export the parser via `spec_dsl.__all__`.
   - Document the new API in `docs/spec_dsl.md`.

2. **Test Utilities & Simplifications**
   - Create a minimal `ExperimentSpecFactory` (or helper functions) under `src/daydreaming_dagster/spec_dsl/tests/fixtures.py` for common axis/rule payloads.
   - Update unit tests to call `parse_spec_mapping` directly instead of writing temporary JSON unless a test explicitly covers file IO:
     * `src/daydreaming_dagster/spec_dsl/tests/test_loader.py`
     * `tests/spec_dsl/test_examples.py` (convert fixtures to load once, or hydrate via parser with shared fixture data)
     * `src/daydreaming_dagster/cohorts/tests/test_spec_planner.py`
   - Keep at least one loader round-trip test that ensures file parsing still works.

3. **Dagster Integration**
   - Introduce a new `CohortSpecResource` (likely in `src/daydreaming_dagster/resources`) that loads and caches an `ExperimentSpec` (via `load_spec` or injected mapping).
   - Assets (e.g. `group_cohorts.py`, `results_summary.py`) request the resource and call `compile_cohort_definition` with their existing catalog wiring.
   - Wire the resource into `definitions.build_definitions()` shared resources; expose configuration for spec path and optional seed.
   - Add tests (unit or Dagster asset tests) verifying the resource provides a singleton `ExperimentSpec` per execution and that assets no longer hit the filesystem directly.

4. **API Surface Adjustments**
   - Enhance `load_cohort_definition` to accept either a `Path`/`str` or a pre-parsed `ExperimentSpec`; update callers accordingly and keep legacy alias behavior with deprecation notes.
   - Update scripts (`scripts/build_pivot_tables.py`) to accept an optional `ExperimentSpec` (or `CohortDefinition`) so logic can run without touching disk in unit tests.
   - Ensure CLI (`spec_dsl/cli.py`) remains the top-level boundary that still reads from disk.

5. **Docs & Migration Notes**
   - Add guidance in `README.md` / `docs/spec_dsl.md` about using `parse_spec_mapping` and the new resource for dependency injection.
   - Record any intentional behavioral changes in `REFactor_NOTES.md` if legacy compatibility diverges.

## Validation & Testing
- Extend/adjust unit tests to cover `parse_spec_mapping` and the new resource behavior.
- Run targeted suites:
  * `pytest src/daydreaming_dagster/spec_dsl/tests`
  * `pytest tests/spec_dsl`
  * `pytest tests/cohorts`
  * Relevant Dagster asset tests if present.

## Open Questions
- (Resolved) Resource will provide the parsed spec; assets continue to build catalogs themselves.
- (Resolved) No known external consumers beyond in-repo aliases.
