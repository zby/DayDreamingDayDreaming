# Curated Selection and Partitions

This guide shows the current, simple flow to run curated drafts/essays/evaluations without touching the full cube. It now centers on:
- `scripts/select_top_prior_art.py` — writes `data/2_tasks/selected_essays.txt` (one essay gen_id per line)
- `cohort_membership` Dagster asset — builds membership.csv and registers dynamic partitions directly inside Dagster

See also
- Cohorts & membership: docs/cohorts.md
- Full Active Experiment Cube: docs/architecture/active_experiment_cube.md

Note on cohorts
- Curated runs now carry a `cohort_id` to keep runs isolated and reproducible. By default, curated scripts use a timestamped cohort (or you can pass an explicit name). The full Active Experiment Cube (AEC) path uses a deterministic cohort by manifest hash. See `docs/architecture/active_experiment_cube.md` for details.

## Prerequisites

- Set `DAGSTER_HOME` to a writable directory, e.g. `export DAGSTER_HOME=$(pwd)/dagster_home`.
- Ensure `data/7_cross_experiment/parsed_scores.csv` exists (produced by prior runs). If missing, rebuild it via `./scripts/rebuild_results.sh`.
- Start Dagster if you want the UI: `uv run dagster dev -f daydreaming_dagster/definitions.py`.

## Step 1: Select Top Prior-Art Winners

Use `scripts/select_top_prior_art.py` to pick the top‑N by prior‑art scores. The script writes `data/2_tasks/selected_essays.txt` with one essay gen_id per line. Pivots and selections are keyed by `parent_gen_id` (the essay generation id), which is the first token of the `evaluation_task_id` in the gen‑id‑first scheme.

Usage
```bash
uv run python scripts/select_top_prior_art.py \
  --top-n 25 \
  --parsed-scores data/7_cross_experiment/parsed_scores.csv \
  # optional: --prior-art-templates gemini-prior-art-eval gemini-prior-art-eval-v2
```

Notes
- The file `selected_essays.txt` serves as the input signal for the cohort builder.
- When joining or pivoting across results, prefer `parent_gen_id` over task ids for stable aggregation.

## Step 2: Build Cohort and Register Partitions (inside Dagster)

Use the Dagster asset `cohort_membership` to build membership.csv (wide rows per stage) and register dynamic partitions directly:

```bash
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f daydreaming_dagster/definitions.py
```

What it does
- Reads `data/2_tasks/selected_essays.txt` (if present) to build a curated cohort; otherwise builds a Cartesian cohort from active axes.
- Writes `data/cohorts/<cohort_id>/membership.csv` with full task columns per stage.
- Registers dynamic partitions for draft/essay/evaluation add‑only.
- Enforces parent integrity: essays must point to cohort drafts; evaluations must point to cohort essays.

## Running the Curated Set

From the Dagster UI, materialize only the partitions you just registered. Or use the CLI, for example:

Drafts and essays
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "draft_prompt,draft_response" --partition "<draft_task_id>"

uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "essay_prompt,essay_response" --partition "<essay_task_id>"
```

Evaluations
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "evaluation_prompt,evaluation_response" \
  --partition "<parent_gen_id>__<evaluation_template>__<evaluation_model_id>"
```

Parsing and summaries
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select parsed_scores,final_results
```

Pivoting by parent_gen_id
- Use `scripts/build_pivot_tables.py` (or downstream analysis) to build pivots that index rows by `parent_gen_id` for deterministic grouping across attempts and reruns.
- Include `cohort_id` in your pivots when comparing different curated runs or baselines.

Environment tip
- You can set `DD_COHORT=<cohort_id>` when materializing assets so generation/evaluation assets reserve `gen_id`s seeded by the same cohort. If unset, task assets compute `cohort_id` deterministically; cohort_membership persists the manifest.

That’s it: one selection script plus a Dagster asset to register only what you want to run — independent of `k_max` or the full cube — with cohorts keeping curated runs isolated and reproducible.
