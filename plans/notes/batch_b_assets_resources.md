# Batch B — Assets & Resources Review

## Highlights
- `assets/group_cohorts.py` is the primary complexity hotspot (Radon F63). Nested builder functions and manual path plumbing make it hard to reason about curated vs. cartesian flows.
- Several asset modules inline filesystem literals (`Path("data")/...`) instead of reusing `Paths`, limiting configurability.
- Resource layer mixes structured errors with ad-hoc logging; retry logic deserves its own helper to keep Dagster resources slim.
- Some guardrails rely on dead or unreachable code (e.g., `_existing_evaluations_by_combo` never populates results due to `continue`).

## Assets
- **group_cohorts** (`src/daydreaming_dagster/assets/group_cohorts.py`)
  - `_existing_evaluations_by_combo` never adds entries because `combos[(tpl, model)].add(...)` sits after a `continue` (`group_cohorts.py:244-249`). Fix allows fill-up mode to perceive existing runs; add a focused unit around the helper before refactor.
  - `cohort_membership` packs >400 lines of nested closures (`group_cohorts.py:528`). Split into smaller pure helpers (builder for curated essays, cartesian expansion, evaluation fan-out). Surface a lightweight orchestration object so tests can exercise modes without Dagster context stubs.
  - Path management mixes `Paths.from_context` with raw `data_root / "gens"` calls (`group_cohorts.py:596`, `group_cohorts.py:657`). Centralize via a `CohortPaths` helper to drop string concatenation and improve portability.
  - Replication logic repeats task-id formatting (`group_cohorts.py:634-756`). Consider a table-driven approach (per-stage config listing template axes, replicate counts, generator) to collapse duplicate loops.
  - Error handling raises `DDError` inline even for repeated metadata checks (`group_cohorts.py:274-339`). Extract validators that return structured ctx dictionaries to shorten the main asset.
  - Tests: `assets/tests/test_cohort_membership.py` is broad but slow. Add narrow units for `_parse_selection_file`, `_existing_evaluations_by_combo`, and `_ReplicateAllocator` to accelerate verification of refactors.

- **results_summary** (`src/daydreaming_dagster/assets/results_summary.py`)
  - Duplicate import of `asset_with_boundary` (`results_summary.py:2-3`) and unused `Path` import indicate drift; clean alongside pivot refactor.
  - Guard clauses log warnings in-line for every early return (`results_summary.py:30-47`). Move to helper that returns `EmptyPivot(reason)` with structured metadata so boundary logging remains centralized.
  - Column suffix merging repeats pandas pivot boilerplate (`results_summary.py:64-155`). Extract a `build_pivot(df, index, values, stats=[mean,min,max,count])` utility shared between `generation_scores_pivot`, `final_results`, and other summary assets.

- **documents_checks** (`src/daydreaming_dagster/checks/documents_checks.py`)
  - `_resolve_doc_id` and the `pandas` dependency are unused (`documents_checks.py:10-38`). Removing both simplifies deployment footprint. Replace dynamic `globals()` mutation with explicit check registry returned to `Definitions` (`documents_checks.py:59-70`).
  - Context probing (`documents_checks.py:21-22`) can leverage Dagster's public API (`context.partition_key`) once unit tests cover the behavior.

- **raw_schedule** (`src/daydreaming_dagster/schedules/raw_schedule.py`)
  - Literal paths and helper sprawl persist; earlier batch A notes apply. For assets boundary, consider storing fingerprint helpers alongside `utils/raw_state.py` so schedule module only orchestrates Dagster constructs (`raw_schedule.py:36-80`).

- **assets/_helpers** (`src/daydreaming_dagster/assets/_helpers.py`)
  - `get_run_id` defensively introspects context (`_helpers.py:15-32`). Provide a shared boundary helper returning `context.run_id` with graceful fallback to keep asset code clean.
  - `build_stage_artifact_metadata` mixes metadata normalization with string suffix rules (`_helpers.py:34-87`). Moving suffix logic to a reusable formatter will reduce duplication across *_raw/*_parsed assets.

## Resources
- **LLMClientResource** (`src/daydreaming_dagster/resources/llm_client.py`)
  - Tenacity + ratelimit decorators are defined inside `_make_api_call_info` (`llm_client.py:118-140`). Extract a reusable `call_with_retry` helper (pure function) so resource stays declarative and tests can mock retry policy. Add focused tests for retry classification beyond current coverage.
  - Logging uses module-level `logger` with info-level messages (`llm_client.py:145-183`). Consider emitting structured metadata (model, duration, truncated) via Dagster logging boundary instead of printing per-call strings.
  - `_is_retryable_error` relies on substring heuristics (`llm_client.py:198-223`). Replace with explicit exception/status matching and centralize in a helper so other clients can share logic.
  - Config attribute `data_root` is typed as `str` while consumers expect `Path` (`llm_client.py:34-35`). Align types and delegate filesystem lookups to `Paths` for consistency.

- **IO managers** (`src/daydreaming_dagster/resources/io_managers.py`)
  - `CSVIOManager.load_input` performs ad-hoc polling for missing files (`io_managers.py:35-53`). Move to a shared `wait_for_file(path, attempts, delay)` utility with logging hooks.
  - `InMemoryIOManager.load_input` repeats identical `DDError` payloads for missing values (`io_managers.py:83-111`). Extract `_missing(partition_key, stage)` helper to keep ctx consistent and shorten the method.
  - Fallback to gens-store paths builds strings manually (`io_managers.py:94-103`). Route through `Paths.from_str` so future directory changes stay centralized.

- **GensPromptIOManager** (`src/daydreaming_dagster/resources/gens_prompt_io_manager.py`)
  - Stage validation uses runtime checks against `STAGES` (`gens_prompt_io_manager.py:23-33`). Consider a typed enum or constrained config schema so misconfiguration fails earlier.
  - IO manager instantiates `GensDataLayer` eagerly (`gens_prompt_io_manager.py:31-32`); allow dependency injection for tests to remove filesystem coupling.

## Testing & Coverage
- Add lightweight units beside helpers (`group_cohorts`, `documents_checks`, `llm_client`) before large refactors. Existing tests in `assets/tests/test_cohort_membership.py` and `resources/tests/test_in_memory_io_manager.py` can stay as integration backstops.
- When restructuring `cohort_membership`, create scenario fixtures for curated reuse/regenerate modes to avoid restaging the entire CSV hierarchy on every test run.

## Confusing Constructs to Catalog
- Manual mutation of `globals()` for asset checks (`documents_checks.py:66-68`).
- Nested closures in `cohort_membership` capturing outer scope state; complicates unit testing (`group_cohorts.py:598-756`).
- Rate-limit `mandatory_delay` baked into resource defaults slows unit suites; treat as `TEMPORARY:` config until overrides exist (`llm_client.py:33`).

## Bold (Contract-Breaking) Ideas
- Remove deprecated re-export `src/daydreaming_dagster/config/paths.py` once downstream callers migrate.
- Revisit `membership.csv` schema to include combo_id/parent columns if needed; would require downstream updates but simplifies evaluation fan-out logic.
- Introduce typed resource configs or smaller resource wrappers (e.g., `PromptWriter`, `LLMRunner`) instead of monolithic resources; would shift Definitions wiring.
