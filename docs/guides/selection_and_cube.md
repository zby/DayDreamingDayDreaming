# Curated Selection and Partitions

This guide shows the current, simple flow to run curated drafts/essays/evaluations without touching the full cube. It centers on two scripts: `scripts/select_top_prior_art.py` and `scripts/register_partitions_for_generations.py`.

## Prerequisites

- Set `DAGSTER_HOME` to a writable directory, e.g. `export DAGSTER_HOME=$(pwd)/dagster_home`.
- Ensure `data/7_cross_experiment/parsed_scores.csv` exists (produced by prior runs). If missing, rebuild it via `./scripts/rebuild_results.sh`.
- Start Dagster if you want the UI: `uv run dagster dev -f daydreaming_dagster/definitions.py`.

## Step 1: Select Top Prior-Art Winners

Use `scripts/select_top_prior_art.py` to pick the top‑N by prior‑art scores and write curated task CSVs.

What it does
- Reads cross‑experiment scores, considers prior‑art templates (`gemini-prior-art-eval`, `gemini-prior-art-eval-v2` by default), and selects top‑N `document_id`s.
- Writes `data/2_tasks/essay_generation_tasks.csv` (always) and `data/2_tasks/draft_generation_tasks.csv` (default on).
- Registers dynamic partitions for `draft_tasks` and `essay_tasks` by default so you can run them immediately in Dagster.

Usage
```bash
uv run python scripts/select_top_prior_art.py \
  --top-n 25 \
  --parsed-scores data/7_cross_experiment/parsed_scores.csv \
  # optional: --no-register-partitions  # skip Dagster registration
  # optional: --prior-art-templates gemini-prior-art-eval gemini-prior-art-eval-v2
```

Notes
- Draft writing is enabled by default; disable with `--no-write-drafts` if you only want essays.
- If any referenced generation files are missing under `data/3_generation/essay_responses/` or `data/3_generation/draft_responses/` (or legacy `links_responses/`), the script will warn.

## Step 2: Register Curated Partitions (and Evaluations)

Use `scripts/register_partitions_for_generations.py` when you want to:
- Register evaluation partitions for selected documents across active evaluation templates × models.
- Reset dynamic partitions (clean slate) or clean `data/2_tasks` before writing curated CSVs.
- Drive from a list/CSV of IDs instead of re‑selecting.

Typical usage (drive from the curated CSV written in Step 1)
```bash
uv run python scripts/register_partitions_for_generations.py \
  --input data/2_tasks/essay_generation_tasks.csv
```

Key flags
- `--no-register`: write CSVs only, skip partition registration.
- `--no-reset-partitions`: keep existing partitions; default is reset (fresh curated set).
- `--no-clean-2-tasks`: do not clear `data/2_tasks`; default cleans and preserves only `selected_generations.txt/.csv` if present.
- `--eval-templates ...` and `--eval-models ...`: override active evaluation axes.
- `--dry-run`: print what would be written/registered without making changes.
- `--write-keys-dir DIR`: also write partition key lists to files.

Inputs it accepts
- A text file of `document_id`s (one per line), or a CSV with one of: `document_id`, `essay_task_id`, or `draft_task_id`.

Outputs it writes
- `data/2_tasks/essay_generation_tasks.csv` and (unless disabled) `data/2_tasks/draft_generation_tasks.csv` (de‑duplicated by task id).
- Dynamic partitions for `draft_tasks`, `essay_tasks`, and `evaluation_tasks` (active templates × models, or your overrides).

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
  --partition "<document_id>__<evaluation_template>__<evaluation_model_id>"
```

Parsing and summaries
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select parsed_scores,final_results
```

That’s it: two scripts to select winners and register only what you want to run, independent of `k_max` or the full cube.
