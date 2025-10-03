# Plan: Simplify Experiment Spec YAML and Catalog Wiring

## Context
- Current specs require deeply nested YAML (`axes -> axis_name -> {levels, catalog_lookup}`) and list-wrapped rules.
- Loader/ compiler still depend on optional `catalog_lookup` metadata even though axis names already match catalog tables.
- Goal: flatten YAML authoring while keeping canonical `ExperimentSpec` compatible for existing callers.

## Desired Outcomes
1. Authors describe axes as plain lists and rules as named maps without redundant nesting.
2. Loader/parser only accepts the simplified shape (no legacy schema).
3. Catalog validation happens via axis-name inference and an explicit catalog repository rather than per-axis metadata.
4. Docs/tests reflect the simplified schema exclusively.

## Scope & Assumptions
- Touch `spec_dsl` (loader, models, compiler) plus cohort planning utilities.
- No changes to Dagster asset signatures; downstream callers still receive `ExperimentSpec`.
- No backward compatibility layer—the legacy nested schema and `catalog_lookup` plumbing will be removed.

## Work Items

### 1. Data Model Updates
- Remove `catalog_lookup` from `AxisSpec`; replace with lightweight metadata (if any) needed by compiler.
- Ensure helper utilities infer catalog names purely from axis identifiers.

### 2. Loader Rewrite (`spec_dsl/loader.py`)
- Require `axes: { axis_name: [level, ...] }` as the only accepted representation.
- Define rule sections (`pairs`, `subsets`, `ties`, `tuples`, etc.) as top-level mappings; convert them into the internal rule representation.
- Require `output.field_order`; error if deprecated keys (e.g., `order`) appear.
- Strip all `@file` helpers into dedicated loaders if still needed; otherwise accept direct lists only.
- Update loader tests to cover failures for legacy shapes.

### 3. Compiler Catalog Logic (`spec_dsl/compiler.py`)
- Infer catalog names from axis identifiers exclusively.
- Accept the new rule format from the loader.
- Add tests for inference + failure cases when levels are missing from catalogs.

### 4. Catalog Repository
- Introduce an interface (e.g., `spec_dsl/catalogs.py` or `cohorts/catalog_repository.py`) exposing `get(axis_name) -> Iterable[str]`.
- Provide filesystem-backed implementation reading `Paths` tables on demand.
- Update `cohorts/spec_planner.py` (and any other compile entry points) to use the repository and pass `catalogs` into `compile_design`.
- Adjust related tests (`cohorts/tests/test_spec_planner.py`, integration fixtures) to inject in-memory catalogs via the new interface.

### 5. Fixture & Doc Updates
- Convert spec fixtures under `tests/fixtures/spec_dsl/` and unit tests to the simplified YAML.
- Update `docs/spec_dsl.md` with new syntax examples and catalog inference notes.
- Remove legacy tutorials and any references to deprecated schema.

### 6. Validation & Cleanup
- Run unit suite `.venv/bin/pytest src/daydreaming_dagster/spec_dsl/tests/ src/daydreaming_dagster/cohorts/tests/test_spec_planner.py`.
- Spot-check integration tests that load specs (`tests/test_pipeline_integration.py`) for fixture alignment.
- Add assertion/linter ensuring specs fail fast if deprecated keys appear.

## Risks & Mitigations
- **Risk:** Axis naming diverges from catalog table names. → Document assumption (axis name == catalog table) and add guard rails in repository.
- **Risk:** Downstream assets expecting `output.field_order` may break if specs omit it. → Fail fast in loader and call out requirement in docs.

## Out of Scope
- Rewriting specs into a brand-new DSL (JSON, CUE, etc.).
- Removing `catalog_lookup` field entirely (defer until backcompat window closes).

## Exit Criteria
- Specs can be authored with flattened YAML; docs/tests demonstrate usage.
- Simplified specs load exclusively; legacy formats raise immediate errors.
- Cohort planner and compiler validate levels via the repository.
- All relevant pytest suites green.
