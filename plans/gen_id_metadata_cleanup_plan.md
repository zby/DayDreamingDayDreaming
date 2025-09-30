# Gen ID Metadata Cleanup Plan

## Goal

Ensure deterministic ID recomputation works for every legacy generation by enforcing stage-appropriate metadata and documenting the guarantee. Specifically, only drafts keep `combo_id`, while essays and evaluations derive it from parents during migration utilities.

## Scope & Assumptions

- Repository is the canonical source of truth; all metadata lives under `data/gens/`.
- Legacy generations exist but are static; no new legacy IDs will be created.
- We already have deterministic ID helpers in `daydreaming_dagster.utils.ids` and readers in `utils.generation`.

## Execution Steps

1. **Audit Metadata Completeness**
   - Script: `python scripts/validate_migration_metadata.py` (to be written if missing) to assert required keys per stage.
   - Confirm essays/evaluations without `combo_id` still reach it via parent chain.

2. **Code Adjustments**
   - `assets/group_cohorts.py`: seed `combo_id` only for drafts; rely on parent lookups for essays/evals.
   - `utils/evaluation_scores.py` & related loaders: ensure they fetch combo from draft metadata when absent.

3. **Add Regression Tests**
   - Extend `assets/tests/test_cohort_membership.py` to assert essay/eval metadata lacks direct combo while drafts retain it.
   - Include a unit test around deterministic ID recomputation that walks parent chain to supply combo.

4. **Documentation Update**
   - Refresh `docs/guides/gen_id_migration_analysis.md` to reflect stage-specific signatures and removal of essay combo fields.
   - Cross-link from the migration plan to the metadata validation script.

5. **Backfill (if necessary)**
   - Run metadata fixer script on any legacy essays/evaluations still storing `combo_id` directly; remove the field and re-save metadata.
   - Commit the script under `scripts/` (idempotent, dry-run mode by default).

6. **Validation & Sign-off**
   - Re-run the audit from Step 1; expect zero failures.
   - Execute targeted tests: `.venv/bin/pytest src/daydreaming_dagster/assets/tests/test_cohort_membership.py`.
   - Smoke-test deterministic recomputation using a small sample: `python scripts/recompute_gen_ids.py --check-only`.

## Risks / Mitigations

- **Risk**: Removing combo_id from essays/evals breaks ad-hoc tools that read metadata directly.
  - *Mitigation*: Provide helper function `get_combo_for_gen(gen_id)` and update scripts to use it.
- **Risk**: Legacy metadata may have malformed parent pointers.
  - *Mitigation*: Audit script should flag missing parents; decide whether to backfill or quarantine.

## Out of Scope

- Renaming existing gen directories or migrating legacy IDs.
- Dagster partition updates beyond metadata consistency.

