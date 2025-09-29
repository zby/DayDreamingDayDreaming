# Deterministic Generation IDs Roadmap

## Objective
Derive generation IDs deterministically from task parameters so cohorts become pure selectors over a global task universe. Historical generations will be migrated to the new scheme; re-running cohorts should reuse those IDs automatically via Dagster's "skip existing" logic.

## Phase 1 – Deterministic ID Helpers (Feature Flagged)
- **Signature Schema**
  - Draft: `(combo_id.lower(), draft_template_id.lower(), generation_model_id.lower(), replicate_index:int)`
  - Essay: `(draft_gen_id:str, essay_template_id.lower(), replicate_index:int)`
  - Evaluation: `(essay_gen_id:str, eval_template_id.lower(), eval_model_id.lower(), replicate_index:int)`
  - Replicate indices are 1-based; ensure they do not exceed `replication_config.csv` values. Missing metadata is a hard error raised during cohort build.
- **Deterministic ID Function**
  - Canonical string format: `"<stage>|" + "|".join(signature_fields)`.
  - Hash using BLAKE2b (10-byte digest) and encode as base36; prepend stage prefix (`d_`, `e_`, `v_`).
  - Collision policy: if a collision is detected, append deterministic suffix `-1`, `-2` while logging a blocking alert for manual follow-up.
- **Helper APIs**
  - Implement `compute_signature(...)` utilities and a shared `compute_gen_id(stage, signature)` function.
  - Extend `CohortScope` with lookup helpers (`signature_for_gen`, `find_existing_gen_id(signature)`) returning structured results including `origin_cohort_id` and `is_reused` flag.
- **Feature Flag**
  - Introduce a toggle (e.g., env var) so cohort assets/tests can opt into deterministic ID generation while legacy IDs remain default.

## Phase 2 – Cohort Asset Cutover
- Enable deterministic ID generation by default (remove flag), ensuring `cohort_membership` enumerates signatures without the legacy `reserve_gen_id` path.
- Update `CohortScope`, reporting scripts, and tests to rely on the new helpers.
- Validate in staging by materializing sample cohorts and confirming deterministic IDs plus Dagster skip behavior.

## Phase 3 – Data Migration
- **Dry-Run Report**
  - Script to traverse `data/gens/<stage>/*`, compute deterministic IDs, generate an old→new mapping CSV, and detect collisions or missing parents.
- **Migration Execution**
  1. Snapshot/backup `data/gens`, `cohorts`, and derived outputs (`5_parsing`, `6_summary`, `7_reporting`).
  2. Execute `scripts/migrations/run_deterministic_id_migrations.sh` which performs, in order:
     - Replicate index normalization (`scripts/migrate_replicate_indexes.py`).
     - Deterministic ID dry-run + mapping export.
     - Synthetic draft backfill for copy-mode essays (`scripts/migrations/backfill_copy_mode_drafts.py`).
     - Deterministic rename + metadata rewrite.
  3. Regenerate downstream artifacts by re-running assets/scripts (aggregated scores, pivot tables, cohort manifests) instead of editing in place.
  4. Update test fixtures (`tests/data_pipeline_test/gens/**`) via the same renaming script.
- **Validation & Rollout**
  - Perform the migration in staging first; verify parent chains, run targeted asset tests, and confirm Dagster skips reruns with the new IDs.
  - Production rollout: execute migration with logging, backups, synthetic-draft summary CSVs, and automatic rollback on failure; keep an old→new mapping until stable.

## Phase 4 – Cohort Simplification
- `cohort_membership` now always computes deterministic IDs directly and seeds metadata for any missing generations; reuse counters and signature lookups have been removed.
- Tests exercise curated and cartesian flows using deterministic IDs only; evaluation fill-up relies on the same deterministic helpers.
- Dagster's partition skipping handles reruns via deterministic IDs; no bespoke reuse logic remains.
- Legacy `model_id` fields were scrubbed from generation metadata (see `scripts/migrations/remove_model_id_fields.py`); assets now rely solely on `llm_model_id`.
- Curated cohorts support explicit modes (`regenerate`, `reuse-drafts`, `reuse-essays`) so operators choose whether to mint fresh drafts/essays/evals or only top up evaluations; `replication_config.csv` now applies per cohort build by scanning for the next free deterministic replicate.

## Phase 5 – Documentation & Guardrails
- Update architecture docs with the deterministic ID contract, collision handling policy, and migration procedure.
- Add automated checks (unit tests or lint rules) to confirm newly produced gen IDs match the deterministic function.
- Acceptance criteria:
  - Re-running the same cohort yields identical deterministic IDs and Dagster emits `SKIPPED` events.
  - Migration script completes without orphaned parent links, confirmed by automated validation.
  - Test coverage includes collision handling, missing metadata failures, and cross-cohort reuse scenarios.
