# content_combinations input mode: In‑graph DataFrame

## Current behavior
`content_combinations` now consumes the `selected_combo_mappings` asset as an in‑graph `DataFrame` input and applies strict subset validation against `data/combo_mappings.csv` (when present). There is no k‑max fallback inside `content_combinations`.

## Tests and ephemeral jobs
When using `dagster.materialize([...])` with an ad‑hoc list of assets, include both assets in the same call so the dependency is wired:

```
result = materialize([
  selected_combo_mappings,
  content_combinations,
  # downstream task assets (draft_generation_tasks, essay_generation_tasks, ...)
], resources=..., instance=...)
```

This pattern works reliably in ephemeral jobs; there’s no need to build a Definitions‑based job for this specific dependency.

## Notes
- `selected_combo_mappings` produces a DataFrame and (as a side effect) writes `data/2_tasks/selected_combo_mappings.csv`. The latter helps external scripts but is not required by `content_combinations`.
- Downstream task assets (`draft_generation_tasks`, `essay_generation_tasks`) depend on `content_combinations` as before; essay generation itself uses only the draft text at runtime.
- If you run curated flows that produce `selected_combo_mappings.csv` externally, ensure your pipeline also materializes the `selected_combo_mappings` asset (or add a small loader asset) so `content_combinations` receives a DataFrame in‑graph.
