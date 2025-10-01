# Plan: Spec-Driven Cohort Materialization (Inputs embedded in spec)

## 1. Current State Inventory (as of 2025-02-14)
- Cohort materialization (`src/daydreaming_dagster/assets/group_cohorts.py::cohort_membership`) reads:
  - Global CSVs under `data/1_raw/` (concepts, templates, models) and respects their `active` columns.
  - Optional curated files (`data/2_tasks/selected_essays.txt`, `selected_drafts.txt`, `selected_combo_mappings.csv`).
  - `selected_combo_mappings` asset (now in-memory) for Cartesian mode combos.
- Outputs: `data/cohorts/<cohort_id>/manifest.json`, `membership.csv`, gens metadata seeded via `seed_cohort_metadata`.
- Cohort definition is implicit—no single spec file; configuration spread across CSV toggles and curated lists.
- Tests: `src/daydreaming_dagster/assets/tests/test_cohort_membership.py`, integration suites, rely on `active` semantics.

## 2. Goals
- Replace scattered configuration with a single declarative cohort spec file (YAML/JSON) containing explicit combo/template/model IDs and per-stage settings.
- Remove `active` columns from global CSVs; treat them as immutable catalogs.
- Materialization uses the spec to pull the exact rows it needs from the catalogs, producing `membership.csv`, manifest, and metadata under `cohorts/<id>/`.
- Ensure reproducibility: spec + catalogs → deterministic cohort outputs; no cross-cohort overwrites.
- Align cohort planning with the tuple-capable experiment DSL (see `plans/tuple_design_dsl_plan.md`) so both specs share rule semantics and compiler.

## 3. Target Architecture

### 3.1 Cohort Spec Layout
- Spec lives under `data/cohorts/<cohort_id>/spec/` as a DSL bundle (see `docs/spec_dsl.md`).
  - `config.yaml` expresses axes, rules, replicates, and output options using the DSL vocabulary. Longer lists or tuple bundles can live adjacent to the config and be referenced with `@file:`.
  - Optional helper folders (`axes/`, `rules/`, `items/`) just feed additional fragments—the compiler consumes the combined DSL document.
  - Curated flows emit tuple bundles (essay template + essay LLM + evaluation template + evaluation LLM) instead of legacy `gen_id`s.
- Validation is delegated to the DSL compiler: catalog lookups, rule intersections, and replication metadata are verified before the planner runs.

### 3.2 Materialization Flow
- `cohort_plan` asset loads the DSL spec, compiles it with the shared module, and receives deterministic rows describing every draft/essay/evaluation combination plus replicate indices.
- Planner groups rows by stage: drafts use `(draft_template, draft_llm, draft_replicate)`, essays use the tuple-expanded essay columns, evaluations use `(evaluation_template, evaluation_llm)`.
- No implicit Cartesian logic remains in the planner—the DSL already captures structured couplings and eliminates unintended combinations.
- `materialize_cohort` consumes the grouped rows to produce `membership.csv`, manifest, and seeded metadata.
- Spec directory is the sole configuration surface; catalogs remain immutable references.

### 3.3 Global Catalog Behavior
- `data/1_raw/*.csv` keep all entries; `active` column removed.
- Catalog data is surfaced through the DSL via `catalog_lookup` and CLI flags (`--catalog`, `--catalog-csv`, `--data-root`).
- `combo_mappings.csv` remains append-only; specs reference IDs directly.
- Users maintain specs per cohort; catalogs stay global references.

### 3.4 DSL Touchpoints (current API)
- `daydreaming_dagster.spec_dsl.load_spec(path)` loads either a single config file or a directory bundle, returning an `ExperimentSpec` with ordered axes, rule list, replicate config, and output hints.
- `daydreaming_dagster.spec_dsl.compile_design(spec, *, catalogs=None, seed=None)` produces `list[OrderedDict]` rows while enforcing catalog lookups via optional `catalogs` mapping. Rows are already expanded for ties/pairs/tuples and include replicate columns when configured.
- `scripts/compile_experiment_design.py` wraps the same API; exposes `--catalog`, `--catalog-csv`, and `--data-root` flags that we can reuse for planner tooling and validation CLIs.
- Planner work should stay inside this public surface (no private helpers) so future packaging remains straightforward.

## 4. Migration Strategy
1. **Schema & Planner Introduction**
   - Define Pydantic models for the spec; add new planner module (e.g., `cohorts/spec_planner.py`).
2. **Spec Generation Tool**
   - Create CLI (`scripts/migrations/generate_cohort_spec.py`) to read legacy cohorts + active flags and emit spec directories with the required list files.
   - Tool validates files against catalogs and writes them under `cohorts/<id>/spec/`.
3. **Dual-Mode Operation**
   - Planner reads spec inputs directly (`load_spec` → `compile_design`), while the legacy active-based path continues to run in the same codebase until we finish backfilling specs.
   - Integration tests compare outputs for both modes to ensure parity during the overlap period.
4. **Remove Active Columns**
   - After specs are adopted, migrate catalogs by dropping `active` column; update loaders to stop expecting it.
   - Provide script to strip columns and warn users if spec refers to nonexistent IDs.
5. **Default Flip & Cleanup**
   - Make spec-driven planner the only code path once parity tests pass.
   - Remove legacy code and documentation references to `active` toggles.

## 5. Implementation Steps (High-Level)
1. **PR1 – Spec Schema & Planner Skeleton**
   - Introduce `cohorts/spec_planner.py` with thin orchestrator calling `load_spec()` and `compile_design()` from `daydreaming_dagster.spec_dsl`.
   - Build light Pydantic/attrs models for planner inputs/outputs (e.g., `CohortPlanRow`, `StageBundle`).
   - Unit-test catalog validation by passing explicit dicts to `compile_design`.
2. **PR2 – Dagster Integration (dual execution)**
   - Refactor `cohort_membership` into loader (`load_spec` fallback to legacy) + planner (`compile_design`) + materializer.
   - Ensure both spec and legacy planners can return `CohortPlan` objects; gate selection by presence of spec directory.
   - Extend Dagster tests to exercise both paths using fixtures under `tests/fixtures/spec_dsl/`.
3. **PR3 – Spec Generation & Catalog Tooling**
   - Implement CLI to generate specs from existing cohorts.
   - Document migration steps.
   - Run tool for in-repo cohorts (commit specs in follow-up).
4. **PR4 – Drop Active Columns & Update Docs**
   - Remove `active` usage from code and tests.
   - Update docs (`docs/cohorts.md`, operating guide) to describe spec workflow.
   - Ensure tests rely solely on spec-provided IDs.
5. **PR5 – Cleanup**
   - Remove legacy path, flag, and related tests.
   - Simplify data-layer readers to stop checking `active`.

## 6. Testing & Validation
- Unit tests for spec parsing, error handling (missing IDs, invalid combinations).
- Integration tests verifying cohorts built from specs match legacy outputs (during dual-mode phase).
- Regression tests: curated essays/drafts, evaluation parsing behaviors.
- CLI migration tests to ensure generated spec round-trips produce identical membership.

## 7. Risks & Mitigations
- **Spec Drift**: enforce schema version and validation, add linting/CI check to validate specs.
- **Missing IDs**: planner should error clearly if spec references catalog entries that don’t exist; provide migration diff output.
- **User Adoption**: supply examples and automation to scaffold specs; degrade gracefully if spec missing (legacy fallback during transition).
- **Catalog Changes**: document requirement that adding new combos/templates requires updating spec as well; consider helper scripts.

## 8. Deliverables
- DSL-backed cohort planner (load spec → DSL compile → Dagster plan).
- Migration tooling and documentation.
- Updated tests ensuring spec-driven flow is the default.
