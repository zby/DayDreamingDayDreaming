# Colocated Test Inventory — src/daydreaming_dagster

## Present Colocated Unit Suites
- `assets/tests/`: coverage for cohort membership, partitions wiring, document reporting, results summary, essay pipelines, asset helper utilities.
- `config/tests/`: path helper smoke tests.
- `models/tests/`: content combination construction.
- `resources/tests/`: in-memory + prompt IO managers, LLM client parameter validation.
- `unified/tests/`: stage input/raw/parsed/services unit checks.
- `utils/tests/`: evaluation parsers/processing/scores, cohort scope helpers, ID utilities, generation helpers.

## Notably Missing (No colocated tests detected)
- `data_layer/`: relies on integration coverage under `tests/data_layer` but lacks fast unit coverage for path readers/writers, adapters, error surfaces.
- `checks/`, `jobs/`, `schedules/`: orchestration shell behavior untested outside integration smoke (`tests/test_import_definitions.py`).
- `definitions.py`: no direct guardrails for repository wiring beyond integration coverage.
- `constants.py`, `types.py`: no lightweight tests verifying serialized schemas or constant catalogs.

## Integration Test Backstops (tests/)
- `tests/test_pipeline_integration.py`, `tests/integration/**`: exercise full Dagster pipelines; useful but slow for refactor feedback.
- `tests/test_csv_reader_unit.py` and related fixtures: cover CSV parsing but live outside source tree; consider mirroring key cases as colocated units when touching data-layer helpers.

## Coverage Risks to Track
- Data-layer serialization/deserialization lacks fast regression suite; prioritize when refactoring IO helpers.
- Dagster schedule/check/job wiring depends on manual inspection; add targeted smoke units when simplifying orchestration shell.
- Structured error handling changes should include unit assertions on `DDError.code` once helpers are refactored.
