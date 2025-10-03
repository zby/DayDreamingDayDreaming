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

- Remove `catalog_lookup` from `AxisSpec`; rely on axis identifiers and spec-provided lists (inline or via file reference).

### 2. Loader Rewrite (`spec_dsl/loader.py`)
- Require `axes: { axis_name: [level, ...] }` as the primary representation, while allowing a string value like `@file:levels.yaml` to pull levels from a sibling file (simplified compared to current nested payloads).
- Define rule sections (`pairs`, `subsets`, `ties`, `tuples`, etc.) as top-level mappings; convert them into the internal rule representation.
- Require `output.field_order`; error if deprecated keys (e.g., `order`) appear.
- Simplify file loading helpers to accept the `@file:` shorthand only; drop legacy nested structures.
- Update loader tests to cover failures for legacy shapes.

### 3. Compiler Logic (`spec_dsl/compiler.py`)
- Assume axis identifiers already reference validated level lists (inline or file-backed) and remove dependencies on catalog metadata.
- Accept the new rule format from the loader.
- Add tests covering failure cases when rule references unknown levels.

### 4. Fixture & Doc Updates
- Convert spec fixtures under `tests/fixtures/spec_dsl/` and unit tests to the simplified YAML.
- Update `docs/spec_dsl.md` with new syntax examples and catalog inference notes.
- Remove legacy tutorials and any references to deprecated schema.

### 5. Validation & Cleanup
- Run unit suite `.venv/bin/pytest src/daydreaming_dagster/spec_dsl/tests/ src/daydreaming_dagster/cohorts/tests/test_spec_planner.py`.
- Spot-check integration tests that load specs (`tests/test_pipeline_integration.py`) for fixture alignment.
- Add assertion/linter ensuring specs fail fast if deprecated keys appear.

## Risks & Mitigations
- **Risk:** Axis naming diverges from catalog expectations. → Document axis naming conventions and fail fast when rule references unknown axes.
- **Risk:** Downstream assets expecting `output.field_order` may break if specs omit it. → Fail fast in loader and call out requirement in docs.

## Out of Scope
- Rewriting specs into a brand-new DSL (JSON, CUE, etc.).

## Exit Criteria
- Specs can be authored with flattened YAML; docs/tests demonstrate usage.
- Simplified specs load exclusively; legacy formats raise immediate errors.
- Cohort planner and compiler validate levels via the repository.
- All relevant pytest suites green.
