# Cohort Membership Refactor – Implementation Guide

## Current Snapshot (Jan 2026)

- `cohort_membership` (src/daydreaming_dagster/assets/group_cohorts.py:480+) is ~450 lines long. The body still defines several nested helpers (`_add_draft_row`, `_build_curated_entries`, `_build_from_drafts`, `_seed_generation_metadata`, etc.) that close over shared state such as `rows`, `essay_seed_combo`, `rep_cfg`, and `existing_eval_cache`.
- Evaluation reuse is already in place. The loop at `group_cohorts.py:900` pulls existing evaluation gen_ids via `_existing_evaluations_by_template_model`, reuses the first `rep_cfg['evaluation']` entries, and only allocates new replicate indices when capacity is not met.
- Membership seeding still writes a wide DataFrame (stage, template, combo, model, replicate) before trimming to a slim CSV (`group_cohorts.py:857`). Metadata seeding uses `template_modes` and the still-wide DataFrame.
- Replicate allocation relies on `_ReplicateAllocator` and deterministic signatures; curated modes have been removed, so the function now branches only on `selection_type in {None,'draft','essay'}`.
- Tests live in `src/daydreaming_dagster/assets/tests/test_cohort_membership.py`. They currently exercise curated essays, curated drafts, Cartesian expansion, and evaluation fill-up semantics against the monolithic function.

## Refactor Goals

1. Shrink `cohort_membership` into composable pieces without changing behaviour, file formats, or deterministic IDs.
2. Preserve evaluation reuse semantics (existing evaluations must remain first-class citizens and keep their replicate numbers).
3. Keep metadata seeding intact until the manifest work (tracked separately) lands; refactor must not drop the current wide-row data that `_seed_generation_metadata` still expects.
4. Improve testability by giving the extracted pieces thin, dependency-injected surfaces so we can add unit tests alongside the existing integration-style tests.

## Constraints & Non‑Goals

- No manifest/slim-membership changes in this refactor. That work stays in `plans/cohort_membership_minimal_schema.md`.
- Deterministic IDs, replication config, and `_existing_evaluations_by_template_model` reuse must remain exactly as today.
- We do **not** introduce the full builder/expander class hierarchy yet. Keep scope incremental so we can land changes in small PRs.
- Curated selection files are already mode-free; do not re-introduce modes.

## Incremental Plan

### Phase 0 – Metrics & Safety Net
- Capture current coverage for the asset tests: `.venv/bin/pytest src/daydreaming_dagster/assets/tests/test_cohort_membership.py`.
- Use `radon cc src/daydreaming_dagster/assets/group_cohorts.py -s --total-average` to baseline complexity.
- Optional: snapshot the Dagster metadata output (rows/drafts/essays/evaluations counts) for a representative cohort run.

### Phase 1 – Extract Declarative Row Structure
1. Introduce a lightweight `MembershipRow` `@dataclass` inside `group_cohorts.py` with `to_dict()` helper.
2. Update `_add_draft_row` / `_add_essay_row` to construct `MembershipRow` and append `.to_dict()` so downstream code remains unchanged.
3. Add focused unit tests for `MembershipRow` (new file `src/daydreaming_dagster/assets/tests/test_membership_row_unit.py`) verifying conversions and default handling.

### Phase 2 – Pull Nested Builders to Top Level
1. Move `_build_curated_entries` and `_build_from_drafts` out of `cohort_membership` into module-level functions. Give them explicit parameters for the state they touch (`data_root`, `rep_cfg`, `allocator`, `essay_seed_combo`, etc.).
2. Update call sites in `cohort_membership` to pass those parameters explicitly. This reduces reliance on closure variables and clarifies dependencies.
3. Add unit tests that exercise the new functions with fixtures/mocks (use tmp paths and in-memory DataFrames where possible). Keep existing integration tests in place.

### Phase 3 – Isolate Cartesian Expansion Helpers
1. Extract a helper `expand_cartesian_drafts(...)` that encapsulates the loop at `group_cohorts.py:648` (combos × draft templates × generation models).
2. Extract a helper `expand_cartesian_essays(...)` that covers the essay loop at `group_cohorts.py:667` and reuses the shared `_ReplicateAllocator`.
3. Ensure these helpers accept the row-adder callbacks created in Phase 1 so they remain agnostic about mutation details.
4. Unit test both helpers with minimal DataFrames to confirm they produce expected deterministic IDs and replicates.

### Phase 4 – Evaluation Expansion Facelift
1. Extract the evaluation loop into a `build_evaluations_for_essay(...)` helper that takes `essay_gen_id`, existing counts, the axes, and the allocator. The helper should return a list of `MembershipRow` instances (new + reused) and a flag indicating whether new evaluations were minted.
2. Wrap that helper in a thin orchestrator that accumulates `fill_up_fully_covered` and `evaluation_fill_added`, preserving the metadata reported today.
3. Expand unit tests to validate: reused evaluations keep their original replicate, new ones allocate sequentially, and the helper respects `rep_cfg['evaluation']`.

### Phase 5 – CohortMembership Orchestrator Cleanup
1. After Phases 1–4, the body of `cohort_membership` should largely be orchestration logic. Review for shared setup that can be hoisted (e.g., reading templates/models, building `selection_cfg`).
2. Consider wrapping the orchestration state in a simple `@dataclass CohortMembershipContext` (cohort_id, data_root, rep_cfg, allocator, etc.) if it meaningfully reduces argument lists; otherwise keep plain variables.
3. Re-run complexity metrics and ensure the main function drops below the agreed target (aim for Radon < 20).

### Phase 6 – Testing & Documentation
1. Ensure all new helpers have colocated unit tests; extend integration tests only where behaviour changes.
2. Update `plans/cohort_membership_minimal_schema.md` to reflect any new helper names if they become future integration points.
3. Run `.venv/bin/pytest src/daydreaming_dagster/assets/tests` and relevant integration suites.
4. Capture radon/pytest reports in `plans/notes/metrics_after_cohort_membership_refactor.md`.

## Risks & Mitigations
- **Risk:** Accidentally break evaluation reuse by reordering helper calls.
  - *Mitigation:* Unit-test the new evaluation helper and cross-check with integration test `test_cohort_membership_curated_reuse_essays_adds_new_evaluations`.
- **Risk:** Losing metadata needed by `_seed_generation_metadata` if we change the row payload.
  - *Mitigation:* Keep the row dict keys identical; add assertions in unit tests verifying required keys (`stage`, `gen_id`, `combo_id`, `template_id`, `llm_model_id`, `replicate`).
- **Risk:** PR scope creep into manifest/slim membership work.
  - *Mitigation:* Explicitly defer manifest topics back to `cohort_membership_minimal_schema.md` and keep this plan focused on structural decomposition only.

## Exit Criteria
- `cohort_membership` orchestrates extracted helpers and contains no nested function definitions.
- New helper functions/classes are unit-tested, and existing asset tests continue to pass without fixture churn.
- Radon complexity for `cohort_membership` drops to ≤ 20, and total lines in the function fall under ~200.
- Evaluation reuse counters (`fill_up_fully_covered`, `fill_up_added_evaluations`) maintain their current semantics.

