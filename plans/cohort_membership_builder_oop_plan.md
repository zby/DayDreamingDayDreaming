# Cohort Membership Builder – Object-Oriented Refactor Plan

## Background
The prior callable-based cohort membership builder accepted ad-hoc functions for resolving specs, loading catalog slices, and writing artifacts. In commit `ed5cde5` we refactored the builder to use explicit collaborator objects so the orchestration flow is easier to test, mock, and extend. Because downstream branches hit large merge conflicts, we reverted the code while capturing a detailed plan that lets us re-implement the same refactor intentionally.

## Goals
- Preserve the refactor’s intent: a clean orchestration layer that leans on parsed cohort specs and Pandas DataFrames.
- Standardize collaborator interfaces so multiple runtimes (Dagster assets, CLIs) can drive the same builder.
- Retain validation guardrails for the global combo catalog and metadata assembly so callers can audit runs.
- Keep the builder stateless and dependency-injected to simplify unit testing.

## Guiding Principles
- **Keep data typed:** Builders consume parsed cohort spec objects and Pandas DataFrames rather than raw YAML or CSV text.
- **Isolate side effects:** Persistence and metadata fan-out happen through a single collaborator object injected into the builder.
- **Stateless runs:** Every invocation constructs the collaborating objects up front; no hidden caches or shared state.
- **Lean contracts:** Only return what the caller needs (paths, counts, metadata blob) so multiple front-ends can reuse the builder without Dagster baggage.

## Collaborating Objects
| Component | Responsibility | Key Methods | Notes |
| --- | --- | --- | --- |
| `CohortSpecRepository` | Resolve a cohort identifier to a parsed spec payload. | `get(cohort_id: str) -> CohortSpec` | Spec parsing and schema validation live outside the builder; the repository returns the parsed object or mapping. |
| `CatalogBundleLoader` | Provide the necessary slices (members, signals) as Pandas DataFrames plus optional metadata. | `load_for_spec(spec) -> (frames, metadata)` | Frames are keyed by logical name (including the append-only combo catalog) and the builder raises `DDError("catalog_missing")` if slices are absent. |
| `MembershipArtifactStore` | Persist computed membership artifacts. | `persist_membership(cohort_id, df, metadata) -> Mapping[str, Any]` | Returns paths/handles; the builder decides whether to call this based on `dry_run`. |
| `RegistryFactory` | Produce the generation registry used while computing membership. | `create_registry(spec, frames) -> GenerationRegistry` | Defaults to an in-memory registry for isolated unit tests, but production wiring can substitute alternatives. |

These objects only do existence checks and resource translation. Any business-level validation (cohort coverage, disallowed filters, row thresholds) remains inside the builder so callers have a single error surface.

## High-Level Architecture
1. **Core builder** — `CohortMembershipBuilder` coordinates collaborators to assemble a `CohortMembershipResult` dataclass.
2. **Collaborator interfaces**
   - `CohortSpecRepository`: resolves `cohort_id` to parsed spec objects.
   - `CatalogBundleLoader`: returns `{slice_name: DataFrame}` plus optional metadata for a given spec.
   - `MembershipArtifactStore`: persists membership DataFrames when not in dry-run mode.
   - `RegistryFactory`: produces a `GenerationRegistry`; defaults to `InMemoryGenerationRegistry`.
3. **Factory helper** — `create_cohort_membership_builder(**kwargs)` for ergonomic wiring from Dagster assets.
4. **Validation helpers** — leverage existing `validate_cohort_definition` and `validate_membership_against_catalog` to enforce schema and combo coverage.
5. **Metadata assembly** — builder aggregates spec metadata, catalog metadata, combo catalog summary, timestamps, and feature flags into a serializable dictionary.

## Execution Flow
1. **Fetch spec:** Call `CohortSpecRepository.get(cohort_id)` and assume the returned `CohortSpec` passed schema validation.
2. **Load catalogs:** Invoke `CatalogBundleLoader.load_for_spec(spec)` to receive the Pandas DataFrame mapping (plus metadata). The builder verifies presence of each required frame—including the global combo mappings DataFrame mirrored from `data/combo_mappings.csv`—and raises `DDError("catalog_incomplete")` with context if anything is missing.
3. **Compute membership:** Use existing selection helpers to transform the inputs into a Pandas DataFrame of cohort members. Any validation failures (e.g., cohort rules referencing unknown columns) raise `DDError` codes scoped to builder logic.
4. **Persist (optional):** When `dry_run` is `False`, call `MembershipArtifactStore.persist_membership(...)` and collect artifact handles.
5. **Assemble result:** Return a lightweight dataclass `CohortMembershipResult` capturing
   - `cohort_id`
   - `row_count`
   - `membership_preview` (Pandas DataFrame, limited to head `n` if size is large)
   - `artifacts` (paths/URIs from the writer, empty on dry run)
   - `metadata` (dict with run timestamp, validation warnings, arbitrary key/values such as combo catalog version/count so drift in the global mappings is auditable)
   Dagster-specific helpers can translate this into `Output` metadata separately.

## Implementation Steps
1. **Define interfaces and result dataclass**
   - Create abstract base classes in `src/daydreaming_dagster/cohorts/membership_builder.py` for the four collaborators above.
   - Introduce a frozen dataclass `CohortMembershipResult` capturing `cohort_id`, `row_count`, `membership_preview`, `artifacts`, and `metadata`.
   - Provide `InMemoryRegistryFactory` as the default implementation returning `InMemoryGenerationRegistry`.
2. **Implement the builder**
   - Accept collaborators (repository, catalog loader, optional artifact store, optional registry factory) and `preview_rows`.
   - `build()` flow:
     1. Resolve the spec via the repository.
     2. Load catalog frames and metadata, ensuring `combo_mappings` exists, is non-empty, and includes `combo_id` column.
     3. Extract definition from the spec (mapping attribute fallback) and fail fast with `DDError(Err.INVALID_CONFIG)` if missing.
     4. Validate definition with `validate_cohort_definition` and compute membership rows via `generate_membership` using the registry from the factory.
     5. Validate resulting rows with `validate_membership_against_catalog`.
     6. Convert rows to a DataFrame with the canonical `MEMBERSHIP_COLUMNS`, dropping duplicate `(stage, gen_id)` pairs.
     7. Assemble metadata by merging spec metadata, catalog metadata, derived combo catalog summary, optional feature flags, run timestamp, and row count.
     8. Persist via artifact store only when provided and not running `dry_run`; collect returned handles as `artifacts`.
     9. Return `CohortMembershipResult` with a head-based preview copy limited to `preview_rows`.
3. **Expose builder from package init**
   - Update `src/daydreaming_dagster/cohorts/__init__.py` to export the builder, interfaces, registry factory, and result type.
4. **Unit tests**
   - Create `src/daydreaming_dagster/cohorts/tests/test_membership_builder.py` with in-memory fakes for each collaborator.
   - Cover scenarios: persisted run, dry run skipping writer, missing combo catalog (`Err.DATA_MISSING`), malformed combo schema (`Err.INVALID_CONFIG`), metadata aggregation (combo summary, row counts), and preview length cap.
5. **Documentation updates**
   - Remove the decision review doc (now merged into this plan) and keep plan changes localized here.

## Global Catalog Alignment
- Treat the append-only combo catalog stored at `data/combo_mappings.csv` as a first-class slice: ensure the `CatalogBundleLoader` always returns it so the builder can detect missing combos up front rather than during Dagster orchestration.
- Ensure the combo DataFrame preserves schema columns (`combo_id`, `version`, `concept_id`, `description_level`, `k_max`, `created_at`) and fail fast when the sheet is missing or empty.
- When assembling result metadata, include a compact summary (e.g., combo count and highest catalog version) so downstream runs can confirm they used the intended global catalog snapshot.

## Error Handling Expectations
- Collaborating objects raise `DDError` with narrow codes (`"spec_not_found"`, `"catalog_missing"`).
- Builder converts validation issues into `DDError` codes (`"invalid_constraint"`, `"coverage_gap"`) and passes through adapter errors untouched.
- Only the Dagster boundary formats user-facing messages or logs; tests assert error codes and context keys.

## Testing Strategy
- Primary coverage via `.venv/bin/pytest src/daydreaming_dagster/cohorts/tests/test_membership_builder.py`.
- Full regression suite via `.venv/bin/pytest` once integrated into Dagster assets.
- Maintain a small `tests/fakes/` module with reusable fake readers/writers to keep future tests readable.

## Rollout & Follow-Up
- After re-implementing, update Dagster assets to instantiate the builder with real repository/loader/store implementations.
- Capture any intentionally dropped behaviors in `REFactor_NOTES.md` if they surface during the redo.
- Monitor combo catalog drift by ensuring metadata includes `combo_count`, `rows`, and `max_version`.

## Reversion Plan
- Because collaborators are injected, revert by swapping back to simple callables and deleting the new interfaces/tests. The plan captured here enables recreating the object-oriented version without retaining the implementation in git history.
