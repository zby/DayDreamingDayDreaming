# Systematic Error Handling Refactor Plan

## Objective
Introduce the shared `utils.errors` layer (Err Enum + DDError) and standardize exception handling so deep layers raise structured codes and boundaries format/log user-facing text.

## Current Assessment
- Error surface patterns: ad-hoc `ValueError` / `RuntimeError` / `KeyError` across assets, utils, resources; message strings used in tests/doc expectations.
- Logging boundaries inconsistent: many assets catch exceptions only to wrap strings and re-raise.
- Tests often assert message substrings instead of error types.

## Proposed Strata
1. **Deep layers (utils/models/resources):** raise `DDError(code, ctx, cause)` exclusively; no direct logging or message composition.
2. **Existing asset error boundary** (`daydreaming_dagster/assets/_error_boundary.py`) becomes the standard choke-point: it should catch `DDError`, enrich context (e.g., asset key, cohort), log once, and re-raise as Dagster `Failure` while preserving `Err` code.
3. **Other boundaries** (resources, CLI scripts) that need user-facing messages should follow the same pattern or delegate to the asset boundary helper.
4. **Tests:** assert `err.code` plus minimal context; remove message assertions.
5. **Docs:** update error-handling guidance (already partially captured in AGENTS).

## Err Enum Draft (initial set)
- `DATA_MISSING` – required CSV/file missing or empty
- `INVALID_CONFIG` – user-supplied configuration invalid
- `MISSING_TEMPLATE` – template id not found or inactive
- `INVALID_MEMBERSHIP` – membership.csv or parent integrity issues
- `IO_ERROR` – filesystem read/write issues
- `PARSER_FAILURE` – parsing raw outputs failed
- `UNKNOWN` – fallback for uncategorized exceptions (temporary)

## Phased Refactor Plan
### Phase 1 – Foundation
- Add `src/daydreaming_dagster/utils/errors.py` with Err Enum & DDError.
- Add helper utilities (`wrap_and_raise`, optional `legacy_message` map for backcompat).
- Update AGENTS/refactor contract (already done) to reference new module.

### Phase 2 – Utils & Data Layer ✓/✗
1. Convert deterministic-id helpers: `src/daydreaming_dagster/utils/ids.py`, `utils/tests/test_ids.py`.
2. Generation helpers: `utils/generation.py`, `utils/tests/test_generation.py`.
3. Evaluation helpers: `utils/evaluation_processing.py`, `utils/tests/test_evaluation_processing.py`; `utils/evaluation_scores.py`, `utils/tests/test_evaluation_scores.py`.
4. Cohort utilities: `utils/cohort_scope.py`, `utils/tests/test_cohort_scope.py`.
5. Run `.venv/bin/pytest src/daydreaming_dagster/utils/tests` and commit (e.g., `feat: add DDError to utils`).

### Phase 3 – Assets & Boundary Wiring ✓/✗
1. Wire `cohort_membership` and helpers: `assets/group_cohorts.py`, `_error_boundary.py`, tests in `assets/tests/test_cohort_membership.py`.
2. Results assets: `assets/results_summary.py`, `assets/results_processing.py`, `assets/tests/test_results_summary.py` (if present).
3. Other high-traffic assets (draft/essay/evaluation groups): ensure they raise/propagate `DDError`.
4. Validate: `.venv/bin/pytest src/daydreaming_dagster/assets/tests` (targeted) + rerun key integration tests if needed; commit.

### Phase 4 – Resources & IO Managers ✓/✗
1. `resources/io_managers.py`, `resources/tests` (if any).
2. LLM resources: `resources/llm_client.py` and related tests.
3. Update boundary handling for CLI wrappers if they translate errors.
4. Validate: `.venv/bin/pytest src/daydreaming_dagster/resources/tests` (if present) + spot asset tests; commit.

### Phase 5 – Remaining Modules ✓/✗
1. Sweep repo for remaining `raise` statements: `rg "raise" src/daydreaming_dagster` and convert to `DDError` where appropriate.
2. Double-check scripts (`scripts/`), CLI entry points, and docs references.
3. Validate: full unit + integration test run (`.venv/bin/pytest src/daydreaming_dagster tests`); commit.

### Phase 6 – Cleanup & Docs ✓/✗
1. Remove temporary `legacy_message` helpers and unused imports.
2. Add `docs/errors.md` if useful; ensure AGENTS references updated process.
3. Final validation run + documentation check; commit with summary.

## Validation Strategy
- Unit test pass (`.venv/bin/pytest src/daydreaming_dagster/...`).
- Integration test pass (`.venv/bin/pytest tests`).
- Spot-check Dagster asset runs to ensure user-facing messages/logs are reasonable.
- `rg "assert .* in str("` to ensure message-based assertions cleaned up.

## Risk Mitigation
- Back up existing error messages in tests before replacement; run incremental diffs per module.
- Use temporary `legacy_message` map with `BACKCOMPAT:` tags if external CLI tooling still expects specific text.
- Keep Err Enum small; only add codes when required by multiple call sites.

## Open Questions
- Do any external scripts (outside repo) depend on current message text? If yes, coordinate change.
- Should we expose Err codes via CLI exit codes (e.g., map to numeric exit statuses)? Potential future enhancement.

## Next Steps
1. Implement Phase 1 foundation in a dedicated branch (add errors.py, update AGENTS reference already done).
2. Execute Phase 2 & 3 sequentially, merging after each with updated tests.
3. Track progress in this plan; update as new modules are addressed.
