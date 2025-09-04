# Implementation surprises (append-only)

- 2025-09-04: Wiring `content_combinations` to take `selected_combo_mappings` as a DataFrame input failed under ephemeral `materialize([...])` jobs, even when both assets were included and when using `AssetIn` with explicit keys. Dagster raised `DagsterInvalidDefinitionError` that the input asset was not produced by provided ops nor a source. Reverting to a file-based read (and materializing the CSV producer first) kept tests green.
