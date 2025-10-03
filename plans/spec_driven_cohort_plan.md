# Plan: Spec-Driven Cohort Materialization

_Last updated: 2025-02-14_

## 1. Status Recap
- ✅ Dagster now materializes cohorts **exclusively** from DSL specs under `data/cohorts/<cohort_id>/spec/config.yaml`.
- ✅ All `@file:` includes use CSV with headers; specs reference deterministic `items/*.csv` files.
- ✅ Raw catalog readers reject the legacy `active` column; catalogs are treated as immutable inputs.
- ✅ Migration tooling (`scripts/migrations/generate_cohort_spec.py`) generates CSV-backed specs that align with the runtime loader.
- ✅ Tests and fixtures exercise the spec-driven path end to end (unit + integration).

## 2. Current Architecture
- **Spec layout**: single `config.yaml` plus optional CSV files referenced via `@file:`. Single-column CSV → axis levels; multi-column CSV → tuple/pair values in header order.
- **Planner flow**: `cohort_plan` loads the spec, `compile_design` emits fully expanded rows (ties/pairs/tuples expanded, synthetic axes dropped). Stage grouping uses `(draft_template, draft_llm, draft_template_replicate)`, etc.
- **Catalog integration**: Axis names double as catalog keys. Catalogs passed via CLI flags (`--catalog`, `--catalog-csv`, `--data-root`) or Dagster resources.
- **Outputs**: `membership.csv`, cohort manifest, and seeded `gens/` metadata remain the contract; they are deterministic from spec+catalogs.

## 3. Remaining Follow-Up
- 📌 Backfill committed specs for all production cohorts (track in ops board).
- 📌 Update or retire ancillary scripts (`check_evaluation_template.py`, `rerun_high_score_evaluations.sh`) that still reference `active` semantics.
- 📌 Add a CI check that validates every committed spec (`load_spec` + `compile_design`) against the latest catalogs.
- 📌 Document the new workflow in `docs/cohorts.md` once production cohorts are migrated.

## 4. References
- Implementation: `src/daydreaming_dagster/cohorts/spec_planner.py`, `src/daydreaming_dagster/assets/group_cohorts.py`
- DSL docs: `docs/spec_dsl.md`
- Spec fixtures: `tests/fixtures/spec_dsl/`
- Migration tooling: `scripts/migrations/generate_cohort_spec.py`
