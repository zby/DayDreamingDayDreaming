# Cohorts

Cohorts are explicit, reproducible identifiers that bind a complete run of the pipeline (task set and generated artifacts) to a single ID.

See also
- Active Experiment Cube: docs/architecture/active_experiment_cube.md
- Curated Runs Quickstart: docs/guides/selection_and_cube.md

What the assets do
- Asset `cohort_id` (group `task_definitions`) computes a deterministic ID from a manifest of:
  - combo IDs (from `content_combinations`)
  - active draft/essay/evaluation templates
  - active generation/evaluation model IDs
  - Writes the manifest to `data/cohorts/<cohort_id>/manifest.json` and returns the cohort ID.
- Asset `cohort_membership` (group `task_definitions`) builds an authoritative membership file and registers dynamic partitions:
  - Reads `data/2_tasks/selected_essays.txt` (one gen_id per line) when present; otherwise uses the active axes (Cartesian).
  - Writes `data/cohorts/<cohort_id>/membership.csv` with wide rows per stage (no task_id columns):
    - Common: `stage`, `gen_id`, `cohort_id`
    - Draft: `combo_id`, `draft_template`, `generation_model`, `generation_model_name`
    - Essay: `parent_gen_id` (draft), `combo_id`, `draft_template`, `essay_template`, `generation_model`, `generation_model_name`
    - Evaluation: `parent_gen_id` (essay), `evaluation_template`, `evaluation_model`, `evaluation_model_name`, optional `parser`
  - Registers dynamic partitions add‑only for draft/essay/evaluation.
  - Enforces parent integrity (essays → drafts; evaluations → essays) within the same cohort.

How it propagates
- Task assets (`draft_generation_tasks`, `essay_generation_tasks`, `evaluation_tasks`) project their tables directly from membership.csv when present and compute task_id columns from the other fields. Otherwise they fall back to legacy active‑axes derivation.
- All gens `metadata.json` files include `cohort_id`.

Overrides
- Environment: `DD_COHORT` forces an explicit ID (e.g., curated re‑run name).
- Config: set `ops.cohort_id.config.override` to replace the computed ID in the UI or run config.

Recommended policy
- Deterministic (default) for “full cube/baseline” runs: ensures idempotent re‑runs when the manifest doesn’t change.
- Explicit/timestamped IDs for curated or ad‑hoc re‑runs to avoid overwrites and keep histories separate.

CLI examples
```bash
# Curated: write selected essays then build cohort
uv run python scripts/select_top_prior_art.py --top-n 25 --parsed-scores data/7_cross_experiment/parsed_scores.csv
uv run dagster asset materialize --select "cohort_id,cohort_membership,group:task_definitions" -f daydreaming_dagster/definitions.py

# Cartesian: no selection file; cohort_membership derives from active axes
uv run dagster asset materialize --select "cohort_id,cohort_membership,group:task_definitions" -f daydreaming_dagster/definitions.py
```

Implementation notes
- If a subset materialization runs tasks without the `cohort_id` asset, tasks will compute and persist the cohort manifest automatically (unless `DD_COHORT` is set), to keep subsets/tests ergonomic.
- The deterministic ID changes when any manifest component changes (combos/templates/models, or a pipeline version constant for material changes).
- Task assets read membership.csv implicitly (using the resolved cohort id) when present and only fall back to legacy active‑axes derivation when membership is absent.
