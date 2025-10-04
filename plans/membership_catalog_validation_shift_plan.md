# Plan: Validate Catalogs Against membership.csv Output

## Context
- `compile_design` currently calls `_validate_catalogs` before rule application, so tuple-bound axes are checked against manual axis lists instead of the concrete rows.
- `cohort_membership` asset builds `membership_df` via `generate_membership`, then writes `membership.csv` through `persist_membership_csv`.
- Catalog lookups are effectively mandatory: helpers default to `build_spec_catalogs(data_root)` whenever `catalogs` is omitted, so the optional parameter masks a hard dependency.
- Goal: move catalog validation to operate on the final membership rows (draft/essay/evaluation) rather than the raw spec axes, and make catalog inputs explicitly required.

## Approach
1. **Make catalogs explicit**
   - Update `load_cohort_context` (and related helpers) to require a `catalogs` mapping argument instead of auto-populating from `data_root`.
   - Ensure asset and script callers pass `build_spec_catalogs(data_root)` directly; adjust tests/fixtures accordingly.

2. **Collect concrete axis values from membership rows**
   - Add a helper (likely in `cohorts/membership_engine.py` or a new module) that accepts the final membership DataFrame (or iterable of `MembershipRow`) and extracts per-axis value sets (combo IDs, templates per stage, llms, replicate counts).
   - Ensure replicate axes are included if global catalogs track them; otherwise document/skip.

3. **Validate membership rows against catalogs (always on)**
   - Reuse or adapt `CohortCatalog` / `validate_cohort_definition` to consume the extracted sets.
   - Introduce `validate_membership_against_catalog` invoked inside `cohort_membership` before `persist_membership_csv`; raise `DDError` if catalogs are missing or incomplete, matching existing catalog expectations.

4. **Retire early spec validation**
   - Remove `_validate_catalogs` call from `compile_design` (and tighten callers/tests accordingly).
   - Update CLI/tests that relied on early failures to expect validation during membership generation.

5. **Test coverage**
   - Unit tests for the new helper: verify that invalid membership values raise `SpecDslError`/`DDError` with axis + missing context, and valid rows pass.
   - Update existing spec DSL tests to reflect removal of early catalog validation.
   - Integration test exercising `cohort_membership` asset with mismatched catalogs to ensure the new validation path triggers.

6. **Documentation & cleanup**
   - Note the new validation timing in `docs/spec_dsl.md` and state that catalogs must be provided when compiling specs or building memberships.
   - Audit existing plans (e.g., `cohort_membership_validation_refactor_plan.md`) for overlap and mark consolidation steps if needed.

## Decisions
- Catalog validation remains mandatory; builds must supply an explicit catalog mapping, typically via `build_spec_catalogs(data_root)`.

