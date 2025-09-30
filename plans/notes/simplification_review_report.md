# Simplification Review — daydreaming_dagster/src

## Snapshot
- Baseline metrics captured (`plans/notes/metrics_baseline.md`): Radon CC average A (4.11), SLOC 7,174, pytest (88 pass).
- Batch findings documented for orchestration (A), assets/resources (B), data layer (C), models/types (D), and utilities/constants (E).
- Cross-cutting analysis & confusing constructs catalog highlight shared pain points (structured errors, logging boundaries, deterministic IDs).

## Top Contract-Safe Priorities
1. **Refactor cohort membership orchestration** (`src/daydreaming_dagster/assets/group_cohorts.py:528`)
   - Split curated/cartesian logic into testable helpers; consolidate deterministic ID usage; add focused unit fixtures.
2. **Enforce deterministic combo IDs at model layer** (`src/daydreaming_dagster/models/content_combination.py:38`)
   - Replace salted `hash` with `generate_combo_id`; document metadata schema; add model regression tests.
3. **Centralize shared stage utilities** (`src/daydreaming_dagster/unified/stage_inputs.py:16`, `stage_parsed.py:13`)
   - Extract common helpers into `stage_core`; ensure unified tests reuse shared functions.
4. **Promote `Paths`/`GensDataLayer` to resources** (`src/daydreaming_dagster/definitions.py:126`)
   - Remove hard-coded `Path("data")` usage; add orchestration smoke test verifying definitions wiring.
5. **Consolidate pivot/IO utilities** (`src/daydreaming_dagster/assets/results_summary.py:19`, `utils/generation.py:18`)
   - Introduce reusable pivot builder and align generation writers with `GensDataLayer`; extend unit coverage accordingly.
6. **Adopt Stage literal across utilities** (`src/daydreaming_dagster/types.py:4`)
   - Update ID helpers, cohort scope, IO managers to accept `Stage`; add validation tests.

## Stretch / Contract-Breaking Ideas
- Deprecate `src/daydreaming_dagster/config/paths.py` re-export after confirming external consumers.
- Introduce typed generation metadata schema (replace ad-hoc JSON structures) for richer validation.
- Promote stage literal to Enum and persist enum values in metadata (requires downstream coordination).

## Testing & Coverage Follow-Ups
- Add targeted unit tests for `group_cohorts` helpers, deterministic combo IDs, orchestration smoke, and `Paths.from_context` failure cases.
- When refactoring pivot/generation utilities, extend existing suites (`assets/tests/test_results_summary_unit.py`, `utils/tests/test_generation.py`).

## Supporting Notes
- Batch notes: `plans/notes/batch_a_orchestration.md` through `batch_e_utilities.md`
- Cross-cutting themes: `plans/notes/cross_cutting_analysis.md`
- Confusing constructs: `plans/notes/confusing_constructs.md`
- Synthesis details: `plans/notes/synthesis_recommendations.md`

## Next Steps
1. Sequence contract-safe priorities (cohort asset refactor, deterministic IDs) and line up required unit tests.
2. Evaluate resource/Paths introduction with minimal wiring changes to Definitions.
3. Plan stretch items once contract-preserving refactors stabilize and consumer impact is clear.
