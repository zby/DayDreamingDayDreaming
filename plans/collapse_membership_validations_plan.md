# Plan: Collapse Cohort Membership/Path Validations onto Shared Helpers

## Current Redundancy
- `cohort_membership` still builds its own catalog, runs `compile_definition`, and derives template mode maps via `_read_templates_safe`. The new `load_cohort_allowlists` only covers evaluation axes; other membership-specific validation (template presence, replication config) remains bespoke.
- `cohort_id` mirrors the same steps to assemble manifests (combos, template ids, llm IDs) and still handles spec-directory fallbacks on its own.
- Helper routines like `_template_mode_map`, `_read_templates_safe`, and `validate_cohort_membership` live in `group_cohorts.py`, duplicating checks that could live alongside the shared cohort helpers.

## Approach
1. **Design a Cohort Context Helper**
   - Add a new helper (e.g. `load_cohort_context`) in `cohorts/spec_planner.py` that wraps `load_cohort_allowlists`, template CSV hydration, and replication-config fetch. Expose a small dataclass with:
     - `definition` (compiled spec plan)
     - `allowlists`
     - `template_modes`
     - `replication_config`
   - The helper should accept either a precompiled definition or compile internally, mirroring the flexibility already in `load_cohort_allowlists`.

2. **Refactor `cohort_membership`**
   - Replace manual catalog + compile logic with `load_cohort_context`.
   - Consume the returned `template_modes` and `replication_config` directly; delete redundant `_require_replication_config` invocations if the helper already enforced it.
   - Keep only the membership-specific logic (builder invocation, metadata seeding, partition registration).

3. **Refactor `cohort_id`**
   - Use the same helper to retrieve combos/templates/llms instead of rebuilding from allowlists by hand.
   - Ensure manifest serialization still uses sorted outputs from the helper (extend the dataclass if necessary).

4. **Consolidate Validation Helpers**
   - Move or wrap `validate_cohort_membership` so it can be reused outside `group_cohorts.py` if needed (e.g., relocate under `cohorts/` or expose via the new helper module).
   - Remove dead functions (`_read_templates_safe` variants) if their duties shift to the shared helper; otherwise keep them private to the helper module.

5. **Testing & Cleanup**
   - Update affected tests (`assets/tests/test_cohort_membership.py`, `assets/tests/test_cohort_id_asset.py`, etc.) to work with the new helper interface.
   - Run the cohort asset unit tests plus a CLI smoke test for `build_pivot_tables.py` to ensure downstream flows still execute.
   - Capture follow-up notes if further simplification (e.g., moving `CohortBuilder` out of assets) is desired.

## Out of Scope
- Large-scale redesign of the cohort membership engine or manifest schema.
- Broader refactors of replication logic beyond moving the validation checks.
