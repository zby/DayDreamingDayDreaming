# Cohorts

Cohorts are explicit, reproducible identifiers that bind a complete run of the pipeline (task set and generated artifacts) to a single ID.

What the asset does
- Asset `cohort_id` (group `task_definitions`) computes a deterministic ID from a manifest of:
  - combo IDs (from `content_combinations`)
  - active draft/essay/evaluation templates
  - active generation/evaluation model IDs
- Writes the manifest to `data/cohorts/<cohort_id>/manifest.json`.
- Returns the cohort ID string and surfaces it in the Dagster UI.

How it propagates
- `draft_generation_tasks`, `essay_generation_tasks`, and `evaluation_tasks` reserve gen IDs with `run_id=cohort_id` and add a `cohort_id` column.
- All gens `metadata.json` files include `cohort_id`.

Overrides
- Environment: `DD_COHORT` forces an explicit ID (e.g., curated re‑run name).
- Config: set `ops.cohort_id.config.override` to replace the computed ID in the UI or run config.

Recommended policy
- Deterministic (default) for “full cube/baseline” runs: ensures idempotent re‑runs when the manifest doesn’t change.
- Explicit/timestamped IDs for curated or ad‑hoc re‑runs to avoid overwrites and keep histories separate.

CLI examples
```bash
# Show/compute cohort and write manifest
uv run dagster asset materialize --select cohort_id -f daydreaming_dagster/definitions.py

# Materialize task definitions that inherit the same cohort
uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py

# Curated run override (env based)
export DD_COHORT=curated-2025-09-09
uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py
```

Implementation notes
- If a subset materialization runs tasks without the `cohort_id` asset, tasks will compute and persist the cohort manifest automatically (unless `DD_COHORT` is set), to keep subsets/tests ergonomic.
- The deterministic ID changes when any manifest component changes (combos/templates/models, or a pipeline version constant for material changes).

