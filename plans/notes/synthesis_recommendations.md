# Synthesis — Simplification Recommendations

## Contract-Safe Priorities
1. **Break down cohort membership asset** (`src/daydreaming_dagster/assets/group_cohorts.py:528`)
   - Simplify curated vs. cartesian flows by splitting into helper modules and adopting shared deterministic-ID utilities.
   - Tests: expand `assets/tests/test_cohort_membership.py` with targeted unit fixtures.

2. **Adopt deterministic combo IDs in models** (`src/daydreaming_dagster/models/content_combination.py:38`)
   - Replace Python hash usage with `generate_combo_id` and document metadata contract.
   - Tests: new model-level regression ensuring ID stability, update existing asset utility tests.

3. **Centralize stage helper utilities** (`src/daydreaming_dagster/unified/stage_inputs.py:16`, `stage_parsed.py:13`)
   - Extract `_count_non_empty_lines`, template resolution, and validation helpers into `stage_core`.
   - Tests: ensure unified stage unit suites use the shared helpers.

4. **Route asset/resource paths through `Paths`/`GensDataLayer` resources** (`src/daydreaming_dagster/definitions.py:126`, `resources/gens_prompt_io_manager.py:31`)
   - Provide Dagster resources for `paths`/`gens_data_layer` to remove ad-hoc `Path('data')` usage.
   - Tests: add orchestration smoke test verifying asset/check/schedule registration with new resources.

5. **Consolidate CSV/testing utilities** (`src/daydreaming_dagster/assets/results_summary.py:19`, `utils/generation.py:18`)
   - Extract reusable pivot builder and align file writers with `GensDataLayer` to reduce duplication.
   - Tests: unit tests for new pivot utility; ensure generation writer tests cover refactored path.

6. **Tighten stage typing** (`src/daydreaming_dagster/types.py:4`)
   - Update utilities and ID helpers to consume the `Stage` Literal and remove manual `.lower()` guards.
   - Tests: add assertion of stage validation in utils/id tests.

## Contract-Breaking / Stretch Ideas
1. **Retire deprecated shims** (`src/daydreaming_dagster/config/paths.py:1`)
   - Remove re-export once downstream callers migrate; requires communication to consumers.

2. **Schema consolidation for generation metadata** (`src/daydreaming_dagster/utils/generation.py:43`)
   - Introduce typed metadata objects and persist consistent JSON schema, potentially migrating existing files.

3. **Enum-based stages** (`src/daydreaming_dagster/types.py:4`)
   - Promote Stage literal to Enum and persist enum names, requiring downstream adjustments.

## Supporting Notes
- Batches A–E: see `plans/notes/batch_*.md`
- Metrics baseline: `plans/notes/metrics_baseline.md`
- Cross-cutting themes: `plans/notes/cross_cutting_analysis.md`
- Confusing constructs: `plans/notes/confusing_constructs.md`
