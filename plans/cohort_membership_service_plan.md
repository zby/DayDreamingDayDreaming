# Cohort Membership Service Extraction Plan

## Goal
Isolate the cohort membership generation workflow from the Dagster asset so that orchestration logic becomes a thin boundary while reusable service code handles coordination of specs, catalogs, and output persistence.

## Current Pain Points
- `cohort_membership` asset loads specs, validates catalogs, writes outputs, and emits metadata, making it hard to reuse the flow elsewhere.
- Tight coupling to Dagster context and IO managers prevents lightweight testing of membership behaviors.
- Validation and persistence live inline, obscuring error handling contracts and retry semantics.

## Proposed Architecture Sketch
- Introduce a `CohortMembershipBuilder` service under `src/daydreaming_dagster/services/` that encapsulates:
  - Loading cohort specs and catalog slices via injected readers.
  - Validating membership inputs (catalog coverage, cohort constraints).
  - Generating membership rows via existing helpers.
  - Persisting outputs to the configured storage backend.
  - Returning structured results (paths, counts, metadata) for the caller.
- Define slim protocols/interfaces for dependencies the service needs (spec reader, catalog accessor, writer) so assets and CLIs can supply concrete implementations.
- Update the Dagster `cohort_membership` asset to:
  - Resolve concrete dependencies from resources.
  - Invoke the builder and forward resulting metadata to Dagster.
  - Remain responsible only for Dagster-specific concerns (asset materialization, metadata emission).

## Implementation Steps
1. **Create protocols** describing the required behaviors (e.g., `CohortSpecReader`, `MembershipCatalogReader`, `MembershipWriter`). Provide adapters for existing resources.
2. **Implement `CohortMembershipBuilder`** that accepts these protocols plus configuration parameters (cohort ID, run context) and produces a structured response object.
3. **Refactor the asset** to construct the builder (through dependency injection), call it, and translate the response into Dagster outputs/metadata.
4. **Add focused tests** for the builder using in-memory fakes covering success, validation failures, and error propagation.
5. **Update existing asset tests** (or create new ones) to ensure the asset integrates with the builder and surfaces metadata correctly.

## Risks & Mitigations
- **Resource wiring churn:** Mitigated by introducing adapters that wrap current resource implementations without changing their public APIs.
- **Behavior regressions:** Covered through builder unit tests and regression tests for the Dagster asset.
- **Interface proliferation:** Keep protocols minimal and colocate them with the service to avoid scattering abstractions.

## Success Metrics
- Asset function shrinks to orchestration duties only (ideally <40 lines of logic).
- Builder unit tests cover the main membership scenarios without Dagster fixtures.
- Future CLI or batch jobs can reuse the builder without importing Dagster-specific modules.
