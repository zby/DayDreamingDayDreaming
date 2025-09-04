# content_combinations input mode: DataFrame vs CSV

## Background
We want `content_combinations` to be a single‑source reader of the selected combination mapping. There are two ways to feed it:

1) File contract: read `data/2_tasks/selected_combo_mappings.csv` from disk.
2) In‑graph DataFrame: take the `selected_combo_mappings` asset as a DataFrame input.

The architectural goal is (2), because it keeps the asset graph explicit and avoids implicit file reads. However, the test harness currently uses `dagster.materialize([...])` to create ephemeral jobs with ad‑hoc lists of assets, which caused dependency resolution issues when we tried (2).

## Symptom in tests
When `content_combinations` accepted a DataFrame input `selected_combo_mappings`, and tests attempted to run:

```
result = materialize([
  selected_combo_mappings,
  content_combinations,
  content_combinations_csv,
  ...
], resources=..., instance=...)
```

we saw this error:

> DagsterInvalidDefinitionError: Input asset '["selected_combo_mappings"]' for asset '["content_combinations"]' is not produced by any of the provided asset ops and is not one of the provided sources.

Even though both assets were provided, the ephemeral job composition didn’t wire the input as expected. Variations using `AssetIn(key=...)` and auto‑inference yielded the same failure in this test setup.

## Why this happens (likely)
- The tests build ephemeral runs directly from a list of assets rather than from the project `Definitions`. In this mode, Dagster sometimes can’t reconcile asset inputs unless the job is built via a graph/Definitions where dependencies are explicit and resolvable.
- The `csv_io_manager` writes/reads CSVs for DataFrames, but that does not help dependency wiring for in‑memory inputs — the job still needs to know which op produces the `selected_combo_mappings` input.

## Current approach (keeps tests green)
- `content_combinations` reads the selected CSV from disk (single‑source contract, strict subset validation); it no longer has a k‑max fallback.
- The optional `selected_combo_mappings` asset generates the CSV deterministically when you want in‑pipeline selection.
- Integration tests first materialize `selected_combo_mappings` (to seed the CSV), then materialize the task definition assets — this sequence works reliably with ephemeral jobs.

## Path to DataFrame input (preferred end‑state)
Once the test composition is adjusted, we should switch `content_combinations` back to taking the DataFrame input:

- Build jobs from `Definitions` (or a composed job) that include both `selected_combo_mappings` and `content_combinations`, so Dagster can wire inputs cleanly.
- Alternatively, provide `SourceAsset`s explicitly for any required file‑backed inputs when using `materialize([...])` directly.
- After this change, remove the direct file read from `content_combinations` and rely solely on the DataFrame input.

## Action items (when ready)
- Update tests to build runs from `Definitions` including both assets, or define a small job that chains `selected_combo_mappings -> content_combinations -> tasks`.
- Change `content_combinations` to: `def content_combinations(context, selected_combo_mappings: pd.DataFrame) -> ...`.
- Drop the file read path and keep the strict subset validation on the in‑memory DataFrame (checking against `data/combo_mappings.csv` when present).

Until then, the CSV contract keeps curated and generated flows symmetric and avoids test fragility.
