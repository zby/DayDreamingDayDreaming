# Cohorts

Cohorts are explicit, reproducible identifiers that bind a complete run of the pipeline (task set and generated artifacts) to a single ID.

See also
- Curated Runs Quickstart: docs/guides/selection_and_cube.md
  (this content has been merged below; see Curated Selection Quickstart)

What the assets do
- Asset `cohort_id` (group `task_definitions`) computes a deterministic ID from a manifest of:
  - combo IDs (from `content_combinations`)
  - active draft/essay/evaluation templates
  - active generation/evaluation model IDs
  - Writes the manifest to `data/cohorts/<cohort_id>/manifest.json` and returns the cohort ID.
- Asset `cohort_membership` (group `task_definitions`) builds an authoritative membership file and registers dynamic partitions:
  - Reads `data/2_tasks/selected_essays.txt` (one gen_id per line) when present; otherwise uses the active axes (Cartesian).
  - Writes `data/cohorts/<cohort_id>/membership.csv` with normalized rows (same columns for all stages, no task_id columns):
    - `stage`, `gen_id`, `cohort_id`, `parent_gen_id`, `combo_id`, `template_id`, `llm_model_id`
    - `stage` is one of `draft|essay|evaluation`.
    - `template_id` is the stage’s template; `llm_model_id` is the stage’s model id.
  - Registers dynamic partitions add‑only for draft/essay/evaluation.
  - Enforces parent integrity (essays → drafts; evaluations → essays) within the same cohort.

Two ways to build a cohort
- Curated mode (selection-driven):
  - Input: write essay `gen_id`s to `data/2_tasks/selected_essays.txt` (one per line).
  - Behavior: reconstructs draft/essay rows from the gens store metadata for the selected essays, then expands evaluations across the active evaluation axes (templates × models).
  - When to use: reproducing or re-evaluating a specific subset of historical essays; migrating legacy outputs; ad‑hoc comparisons.
  - Pros: no Cartesian explosion; exactly the rows you want. Cons: requires existing gens and accurate parent links in metadata.
- Cartesian mode (active-axes-driven):
  - Input: no `selected_essays.txt`. Cohort derives from active rows in `data/1_raw/*.csv` and the curated `selected_combo_mappings.csv`.
  - Behavior: builds drafts from `content_combinations × draft_templates × generation_models`, essays from `drafts × essay_templates`, and evaluations from `essays × evaluation_templates × evaluation_models`.
  - When to use: fresh experiments over a controlled search space (explicit “cube”). Pros: reproducible full-factor run. Cons: can get large quickly; you must manage which templates/models are marked `active=true`.

Practical tips
- Always start Dagster with the daemon so `cohort_membership` can register dynamic partitions automatically when `data/1_raw/**/*` changes.
- Use `DD_COHORT` to separate curated reruns from baseline cohorts and avoid partition churn across contexts.
- For tight, reproducible subsets, prefer curated mode with `selected_essays.txt` or narrow actives in the CSVs.

How it propagates
- Generation/evaluation assets read cohort membership at runtime to resolve templates, models, parents, and combo IDs. Task CSVs are optional curated inputs only.
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
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f daydreaming_dagster/definitions.py

# Cartesian: no selection file; cohort_membership derives from active axes
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f daydreaming_dagster/definitions.py
```

Implementation notes
- If a subset materialization runs without `cohort_id`, assets still resolve membership via the latest cohort files unless `DD_COHORT` pins an explicit ID.
- The deterministic ID changes when any manifest component changes (combos/templates/models, or a pipeline version constant for material changes).
- Generation/evaluation assets consult membership.csv directly and keep narrow CSV fallbacks for back‑compat only.

Operational flow
- Set actives in `data/1_raw/*.csv` and ExperimentConfig (k_max, level).
- Optional curated selection: write essay gen_ids to `data/2_tasks/selected_essays.txt`.
- Materialize `cohort_id,cohort_membership` to register dynamic partitions by `gen_id`.
- Materialize per-stage assets by partition key (`gen_id`): drafts, essays, evaluations.
- Parse and summarize results (`parsed_scores`, `final_results`).

Getting partition keys (gen_ids)
- Drafts: `awk -F',' 'NR==1 || $1=="draft"' data/cohorts/*/membership.csv | cut -d',' -f2 | tail -n +2`
- Essays: `awk -F',' 'NR==1 || $1=="essay"' data/cohorts/*/membership.csv | cut -d',' -f2 | tail -n +2`
- Evaluations: `awk -F',' 'NR==1 || $1=="evaluation"' data/cohorts/*/membership.csv | cut -d',' -f2 | tail -n +2`

Evaluating historical essays
- Create or edit `data/2_tasks/selected_essays.txt` with one essay `gen_id` per line.
- Materialize `cohort_id,cohort_membership` to register evaluation partitions for active axes.
- Materialize `evaluation_prompt,evaluation_response` for the desired evaluation `gen_id`s.

## Curated Selection Quickstart

This section summarizes the curated workflow previously documented in the selection guide.

Prerequisites
- Set `DAGSTER_HOME` to a writable directory, e.g. `export DAGSTER_HOME=$(pwd)/dagster_home`.
- Optional: build cross‑experiment scores if you plan to select by prior‑art top‑N:
  `uv run python scripts/aggregate_scores.py --output data/7_cross_experiment/parsed_scores.csv`.
- Start Dagster for a richer experience: `uv run dagster dev -f daydreaming_dagster/definitions.py`.

Step 1 — Select essay gen_ids
- Use `scripts/select_top_prior_art.py` to pick top‑N by prior‑art scores. The script writes `data/2_tasks/selected_essays.txt` with one essay `gen_id` per line.

Example
```bash
uv run python scripts/select_top_prior_art.py \
  --top-n 25 \
  --parsed-scores data/7_cross_experiment/parsed_scores.csv \
  # optional: --prior-art-templates gemini-prior-art-eval gemini-prior-art-eval-v2
```

Notes
- `selected_essays.txt` is the input signal for curated cohort builds.
- When pivoting or aggregating parsed results, prefer `parent_gen_id` (the essay `gen_id`) for stable grouping.

Step 2 — Build cohort and register partitions
```bash
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f daydreaming_dagster/definitions.py
```

What happens
- Reads `data/2_tasks/selected_essays.txt` (if present) to build a curated cohort; otherwise falls back to Cartesian from active axes.
- Writes `data/cohorts/<cohort_id>/membership.csv`;
  registers dynamic partitions add‑only for draft/essay/evaluation; validates parent integrity.

Running the curated set
- From the UI, materialize partitions using the registered `gen_id`s (see “Getting partition keys”). Or via CLI:

Drafts and essays (by `gen_id`)
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "draft_prompt,draft_response" --partition "<draft_gen_id>"

uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "essay_prompt,essay_response" --partition "<essay_gen_id>"
```

Evaluations (by `gen_id`)
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "evaluation_prompt,evaluation_response" --partition "<evaluation_gen_id>"
```

Parsing and summaries
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select parsed_scores,final_results
```

Environment tip
- Set `DD_COHORT=<cohort_id>` when materializing to ensure new generations reserve IDs under the same cohort. If unset, `cohort_id` is computed deterministically from the manifest, and `cohort_membership` writes both the manifest and membership.

Analysis tip
- When comparing different curated runs or baselines, include `cohort_id` in your pivots and group by `parent_gen_id` to keep comparisons stable across attempts.

One‑phase essay (copy) vs two‑phase essay (LLM)
- The essay stage supports two generator modes configured per essay template in `data/1_raw/essay_templates.csv` via the `generator` column:
  - `copy`: the essay is a verbatim copy of the parsed draft text (one‑phase pipeline). No LLM call happens in the essay stage; evaluation targets the essay copy.
  - `llm`: the essay is generated by an LLM using the draft as input (two‑phase pipeline).
- In Cartesian mode, every active draft will pair with every active essay template, regardless of mode. To avoid mixing one‑phase and two‑phase essays in the same cohort:
  - Option A: activate only the desired essay templates (e.g., keep only a `copy` template active for a pure one‑phase cohort), or
  - Option B: use curated mode (selected_essays.txt) to include only the essays you want, or
  - Option C: build separate cohorts (IDs) for one‑phase and two‑phase runs.
- See also: docs/architecture/architecture.md, section “Two‑Phase LLM Generation” for where `copy`/`llm` is enforced at runtime.
