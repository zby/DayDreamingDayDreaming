# DayDreaming Operating Guide

This comprehensive guide covers everything you need to know to set up, run, and troubleshoot DayDreaming experiments.

See also
- Documentation Index: docs/index.md
- Curated Runs Quickstart: docs/guides/selection_and_cube.md
- Cohorts & membership: docs/cohorts.md

## Table of Contents

1. [Setup & Configuration](#setup--configuration)
2. [Running Experiments](#running-experiments)
3. [Troubleshooting Common Issues](#troubleshooting-common-issues)
4. [Monitoring & Recovery](#monitoring--recovery)

---

## Setup & Configuration

### Prerequisites

- Python 3.11+ with `uv` package manager
- OpenRouter API key set in environment
- Dagster home directory configured

### Initial Setup

1. **Install dependencies:**
   ```bash
   uv sync
   ```

2. **Set environment variables:**
   ```bash
   export OPENROUTER_API_KEY="your_api_key_here"
  export DAGSTER_HOME="$(pwd)/dagster_home"
   ```

3. **Verify configuration:**
   ```bash
   uv run dagster dev -f src/daydreaming_dagster/definitions.py
   ```

### Controlling Overwrites of Generated Files

By default, generated responses (links/essays/evaluations) are write‑once for safety: existing files are not overwritten. To allow overwriting during reruns (useful for experimentation):

```bash
# Artifacts are versioned automatically as {id}_vN.txt; no overwrite flag needed
uv run dagster dev -f src/daydreaming_dagster/definitions.py
```

Notes:
- Prompts always overwrite to reflect current templates.
- CSV outputs (task tables, pivots) are rewritten as part of normal materialization.

### Two-Phase Generation System 🚀

The pipeline uses a two‑phase generation approach for quality and control:

**Phase 1 — Draft Generation**: LLMs brainstorm structured notes/points based on the concept combination.

**Phase 2 — Essay Generation**: LLMs compose an essay using the draft as the source block.

#### Template Structure

Templates are organized by phase:
```
data/1_raw/templates/
├── draft/                    # Phase 1: Draft templates
│   ├── creative-synthesis-v7.txt
│   └── systematic-analytical.txt
└── essay/                    # Phase 2: Essay composition templates
    ├── creative-synthesis-v7.txt
    └── systematic-analytical.txt
```

#### Running Two-Phase Generation

**Recommended** — Use the two‑phase assets:
```bash
# Generate a single partition (replace TASK_ID)
uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
  --select "draft_prompt,draft_raw,draft_parsed,essay_prompt,essay_raw,essay_parsed" \
  --partition "TASK_ID"

# Or run by asset group
uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
  --select "group:generation_draft,group:generation_essays" \
  --partition "TASK_ID"
```


#### Quality Validation

The two‑phase system includes automatic quality validation:
- Phase‑1 (drafts) enforces minimum content (e.g., >= 3 non‑empty lines) and applies parser extraction when configured
- Clear error messages with resolution steps; RAW drafts are still saved on parse errors
- Individual phase caching allows efficient recovery

### Experiment Configuration (No-Code Workflow)

The system uses CSV-based configuration for easy experiment setup without code changes.

#### 1. Choose Active Concepts

Edit `data/1_raw/concepts_metadata.csv` and set `active` to:
- `default-mode-network`: True
- `mind-wandering`: False  
- `combinatorial-creativity`: False
- Keep other required concepts active (e.g., `dearth-ai-discoveries`, `adversarial-evaluation`, `economic-moat`)

This drives the first pipeline step to regenerate `content_combinations` using only the active concepts.

#### 2. Restrict Generation Templates

Edit the template CSV files to control which templates are active:

**Draft Templates** (`data/1_raw/draft_templates.csv`):
- Columns: `template_id`, `template_name`, `description`, `active`, `parser`.
- Control which Phase‑1 draft templates are used by setting `active: true` (set others to `false`).
- Parser column: if the draft output requires extraction, set a valid `parser` name (e.g., `essay_idea_last`). Parsing happens in Phase‑1 and failures there will fail the draft with a clear error. RAW LLM output remains saved under `data/gens/draft/<gen_id>/raw.txt` for debugging.
- Parser registry: supported parser names are defined in `daydreaming_dagster/utils/draft_parsers.py`. To add a new parser, implement and register it there.
- To introduce a new draft template:
  - Add a file under `data/1_raw/templates/draft/<template_id>.txt`.
  - Add a row to `data/1_raw/draft_templates.csv` with the same `template_id`, set `active=true`, and set `parser` if needed.

**Essay Templates** (`data/1_raw/essay_templates.csv`):
- Set desired essay-phase templates to `active: true`.
- Generator column (`generator`): `llm` (default) uses the parsed draft as input; `copy` returns the parsed draft verbatim. Essay‑level parser mode is deprecated after parser‑first.

This limits generation to specific prompt styles for each phase of the two-phase generation process. You can also override the template root by setting `GEN_TEMPLATES_ROOT` (defaults to `data/1_raw/templates`).

#### 3. Mark Experiment Metadata

Use a Dagster run tag to label the run, e.g. `experiment_id=exp_default_mode_only` (via Dagit Launchpad or job definition).

Optionally stash selection files for traceability:
- `data/experiments/exp_default_mode_only/active_concepts.csv`
- `data/experiments/exp_default_mode_only/active_draft_templates.csv`
- `data/experiments/exp_default_mode_only/active_essay_templates.csv`
- `data/experiments/exp_default_mode_only/active_evaluation_templates.csv`

---

## Running Experiments

### Basic Pipeline Execution

1. **Setup auto-updates (daemon):**
   Ensure the daemon is running so raw data loaders and cohort assets auto-update when `data/1_raw/**/*` changes.
   ```bash
   export DAGSTER_HOME=$(pwd)/dagster_home
   uv run dagster dev -f src/daydreaming_dagster/definitions.py
   ```

  Raw inputs are tracked as `SourceAssets` (see `raw_data.py`). After editing files under `data/1_raw/**/*`, re-materialize the cohort bootstrapping assets so the new configuration flows downstream:
  ```bash
  uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
    --select "selected_combo_mappings,content_combinations,cohort_id,cohort_membership,register_cohort_partitions"
  ```

   Optional one-time seed (registers cohort partitions):
   ```bash
   uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py --select "cohort_id,cohort_membership"
   ```

2. **Run generation assets:**
   ```bash
   uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
    --select "group:generation_draft,group:generation_essays"
   ```

3. **Run evaluation assets:**
   ```bash
   uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
    --select "group:evaluation"
   ```

4. **Process results and run analysis:**
   ```bash
   uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
     --select "group:results_processing,group:results_summary"
   ```

### Auto-Materializing Assets

Raw inputs are not auto-materialized; instead, re-run the cohort setup assets whenever raw CSVs or template files change:

- `selected_combo_mappings`, `content_combinations`, `cohort_id`, `cohort_membership`, and `register_cohort_partitions` should be materialized after editing `data/1_raw/**/*` or `data/2_tasks/selected_combo_mappings.csv`.
- This sequence registers dynamic partitions for the downstream generation and evaluation groups.

Cross‑experiment tracking no longer uses auto‑appenders. Use analysis assets (`filtered_evaluation_results`, `template_version_comparison_pivot`) and scripts for backfills under `data/7_cross_experiment/`. These analyses read scores strictly from `data/gens/evaluation/<gen_id>/parsed.txt` and do not parse `raw.txt` — ensure evaluation assets have produced parsed outputs before running cross‑experiment analysis.

---

## Evaluation Flow (Membership‑First)

Evaluation is cohort‑driven and gen‑id keyed:

- `cohort_membership` expands evaluation rows from essay parents × active evaluation templates × evaluation models, and registers dynamic partitions for those `gen_id`s.
- `evaluation_prompt` loads the source essay via `parent_gen_id` from the gens store and renders the evaluation template with the essay text.
- `parsed_scores` parses outputs from `data/gens/evaluation/<gen_id>` and filters to evaluation `gen_id`s present in cohort membership when available.
- `parsed_scores` contains normalized outputs and sets `generation_response_path` to the essay’s `parsed.txt` under the gens store. Downstream pivots should key by `parent_gen_id` (the essay `gen_id`) for deterministic grouping across runs.

Lineage and IDs (gen‑id first)
- `gen_id`: unique identifier of a concrete generation under `data/gens/<stage>/<gen_id>/`.
- `parent_gen_id`:
  - Drafts: none (no parent).
  - Essays: `parent_gen_id` = `gen_id` of the draft refined into the essay.
  - Evaluations: `parent_gen_id` = `gen_id` of the essay being evaluated.
- Tasks and assets must pass/require `parent_gen_id` for essays and evaluations (fail fast if missing). This removes all “latest‑by‑task” ambiguity and makes pivots deterministic.

Note on legacy data:
- If you need to evaluate historical essays not part of the current cohort, write their essay `gen_id`s into `data/2_tasks/selected_essays.txt`, then materialize `cohort_id,cohort_membership` to register evaluation partitions for the active evaluation axes. Use `# mode: reuse-essays` to reuse the original drafts/essays and schedule fresh evaluations. Combine it with `# fill-up` if you only want to top up missing evaluator combinations (instead of creating brand-new replicates). Alternatively, point `selected_drafts.txt` at deterministic draft IDs to reuse the drafts directly (default mode `reuse-drafts`).

### Targeted Evaluations (No full cube)

To run a specific evaluation (e.g., `novelty`) only on chosen documents (e.g., prior-art winners):

1. Ensure the evaluation template exists and is active in `data/1_raw/evaluation_templates.csv` and the evaluation models are flagged `for_evaluation` in `llm_models.csv`.
2. Build cohort membership (either Cartesian from actives or curated via `selected_essays.txt`):
   ```bash
   uv run dagster asset materialize --select "cohort_id,cohort_membership" -f src/daydreaming_dagster/definitions.py
   ```
3. Materialize the evaluation assets for the registered partitions (by `gen_id`). To target a subset, select specific partition keys from `data/cohorts/<cohort_id>/membership.csv` where `stage == 'evaluation'`:
   ```bash
uv run dagster asset materialize --select "evaluation_prompt,evaluation_raw,evaluation_parsed" \
     --partition "<evaluation_gen_id>" -f src/daydreaming_dagster/definitions.py
   ```
4. Re-run `parsed_scores` to ingest the new results.

For cross-experiment winners, include their essay `gen_id`s in `data/2_tasks/selected_essays.txt` and rebuild cohort membership to register evaluation partitions for the active axes.

### Curated Selection Quick Start (Drafts, Essays, Evaluations)

Use the selection script to write a list of essay gen_ids, then let Dagster build the cohort and register partitions (no need to change `k_max`). If `data/7_cross_experiment/aggregated_scores.csv` is missing, build it first:
`uv run python scripts/aggregate_scores.py --output data/7_cross_experiment/aggregated_scores.csv`.
In all examples below, treat `parent_gen_id` as the canonical key for evaluation pivots and selections.

1) Select top‑N prior‑art winners (editable list)
```bash
uv run python scripts/select_top_prior_art.py --top-n 30 --parsed-scores data/7_cross_experiment/aggregated_scores.csv
# Edit data/2_tasks/selected_essays.txt if desired
```

2) Build cohort membership and register partitions (inside Dagster)
```bash
export DAGSTER_HOME="$(pwd)/dagster_home"
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f src/daydreaming_dagster/definitions.py
```

What it does
- Writes `data/cohorts/<cohort_id>/membership.csv` with normalized rows (same columns for all stages):
  `stage, gen_id, cohort_id, parent_gen_id, combo_id, template_id, llm_model_id`.
- Registers dynamic partitions add‑only for draft/essay/evaluation.
- Enforces parent integrity within the cohort.

3) Run in Dagster
- Drafts: materialize `draft_prompt,draft_raw,draft_parsed` for selected partitions (by gen_id)
- Essays: materialize `essay_prompt,essay_raw,essay_parsed`
- Evaluations: materialize `evaluation_prompt,evaluation_raw,evaluation_parsed`

Tip
- Set `DD_COHORT=<cohort_id>` to bind generation/evaluation seeds; task assets compute/persist a deterministic cohort id when not provided.
- Use `--dry-run` to preview changes.

### Where Assets Live

- cohort (membership/selection): `daydreaming_dagster/assets/group_cohorts.py`
- generation_draft: `daydreaming_dagster/assets/group_draft.py`
- generation_essays: `daydreaming_dagster/assets/group_essay.py`
- evaluation: `daydreaming_dagster/assets/group_evaluation.py`
- results_processing: `daydreaming_dagster/assets/results_processing.py`
- results_summary: `daydreaming_dagster/assets/results_summary.py`
- cross_experiment: `daydreaming_dagster/assets/cross_experiment.py`

Quick navigation
- Asset map: `uv run python scripts/asset_map.py` prints `asset_key | group | file:line`.
- CLI grep: `rg -n '@asset\(' daydreaming_dagster/assets` or `rg -n 'group_name="generation_essays"' daydreaming_dagster/assets`.

**Note**: Auto-materialization requires the Dagster daemon to be running. In development, you can manually trigger assets if needed:
```bash
# Manually materialize a specific asset
uv run dagster asset materialize --select "cohort_id,cohort_membership,content_combinations" -f src/daydreaming_dagster/definitions.py
```

### Free vs Paid LLM Runs (Separate Pools)

To observe free-tier queuing vs paid parallelism, configure pools in `dagster_home/dagster.yaml`:

```yaml
concurrency:
  pools:
    llm_api_free:
      max_concurrent: 1
    llm_api_paid:
      max_concurrent: 5
```

Then materialize generation in two steps:

```bash
# Free-tier generation (serialized globally)
uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
  --select "content_combinations,draft_prompt,draft_raw,draft_parsed" \
  --tag experiment_id=exp_free_vs_paid

# Paid generation (parallel per pool)
uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
  --select "content_combinations,draft_prompt,draft_raw,draft_parsed" \
  --tag experiment_id=exp_free_vs_paid
```

For evaluation with a paid model:
```bash
uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
  --select "evaluation_prompt,evaluation_raw,evaluation_parsed" \
  --tag experiment_id=exp_free_vs_paid
```

### Output Locations

- Generated/evaluated files are stored in the gens store and summary folders:
  - `data/gens/` - Canonical gens store
    - `draft/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`
    - `essay/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`
    - `evaluation/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`
  - `data/5_parsing/` - Cohort-scoped parsed evaluation scores (Dagster asset output)
  - `data/6_summary/` - Final aggregated results
  - `data/7_cross_experiment/` - Cross-experiment tracking tables and score rebuilds

- If you used a run tag, the tag appears in Dagster's run metadata for filtering

### Bulk Results Table Generation

For initial setup or when you need to rebuild the cross‑experiment outputs from the gens store:

```bash
# Build parsed scores and pivot from the gens store
uv run python scripts/aggregate_scores.py --output data/7_cross_experiment/aggregated_scores.csv
uv run python scripts/build_pivot_tables.py --parsed-scores data/7_cross_experiment/aggregated_scores.csv
```

These scripts scan existing gens and produce canonical outputs under `data/7_cross_experiment/`.

### Optional Reporting

Use the scripts/assets that construct reports under `reports/`. Add an `experiment_id` column to any CSV you export for later cross-experiment analysis.

---

## Troubleshooting Common Issues

### 1. Missing Generation Response (FileNotFoundError)

**Error Message:**
```
Missing parent essay parsed.txt for evaluation gen_id '<EVAL_GEN_ID>' (parent_gen_id '<ESSAY_GEN_ID>')
```

**Symptoms:**
- `evaluation_prompt` asset fails with FileNotFoundError
- Error metadata shows expected file path and available partitions
- FK relationship is shown in error details

**Root Causes:**
- Referenced `generation_response` partition was never materialized
- `generation_response` partition failed and needs to be re-run
- File was deleted or moved outside of Dagster

**Diagnostic Steps:**
1. Find the parent essay for the evaluation gen_id in membership:
   ```bash
   EVAL=<evaluation_gen_id>
   awk -F',' 'NR==1 || ($1=="evaluation" && $2==envvar("EVAL"))' data/cohorts/*/membership.csv
   ```

2. Check that the expected essay parsed file exists in the gens store:
   ```bash
   ESSAY=<parent_gen_id>
   ls -l data/gens/essay/${ESSAY}/parsed.txt
   ```

3. Verify parent links in membership:
   ```bash
   awk -F',' 'NR==1 || ($1=="evaluation" && $4!="")' data/cohorts/*/membership.csv | head -10
   ```

**Solutions:**
```bash
# Option 1: Materialize the parent essay partition by gen_id
uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
  --select "essay_prompt,essay_raw,essay_parsed" --partition "<ESSAY_GEN_ID>"

# Option 2: If the essay depends on a missing draft, materialize the draft first
uv run dagster asset materialize -f src/daydreaming_dagster/definitions.py \
  --select "draft_prompt,draft_raw,draft_parsed" --partition "<DRAFT_GEN_ID>"

# Option 3: Rebuild cohort membership (registers partitions); then materialize essays
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f src/daydreaming_dagster/definitions.py
```

**Prevention:**
- Always materialize generation assets before evaluation assets
- Use asset group materialization: `group:generation_draft,group:generation_essays` before `group:evaluation`
- Set up monitoring alerts for failed generation partitions

### 2. Invalid Parent Link in Membership

**Error Message:**
```
Invalid essay_task_id referenced by evaluation task 'eval_001': ''
```

**Symptoms:**
- `evaluation_prompt` fails early with a message about missing `parent_gen_id` or missing parent essay
- Error occurs before any file access attempts

**Root Causes:**
- Missing or incorrect `parent_gen_id` in `cohort_membership`
- Essay row not present for the referenced `parent_gen_id`
- Inconsistent active axes vs. curated selection

**Diagnostic Steps:**
1. Inspect cohort membership:
   ```bash
   awk -F',' 'NR==1 || $1=="evaluation"' data/cohorts/*/membership.csv | head -20
   ```

2. Verify the essay parent exists in membership:
   ```bash
   PARENT=<essay_gen_id>
   grep ",$PARENT," data/cohorts/*/membership.csv
   ```

**Solutions:**
```bash
# Rebuild cohort membership after fixing raw actives or curated selection
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f src/daydreaming_dagster/definitions.py

# Inspect raw inputs for issues
head data/1_raw/essay_templates.csv
head data/1_raw/evaluation_templates.csv
head data/1_raw/llm_models.csv
```

### 3. Evaluation Partition Not Found

**Error Message:**
```
Evaluation task 'eval_123_creativity_claude' not found in task database
```

**Symptoms:**
- Asset fails at start when resolving membership for `gen_id`
- Partition exists in Dagster but row is missing in `membership.csv`

**Root Causes:**
- Stale partition definitions (CSV updated but partitions not refreshed)
- Race condition between partition creation and CSV generation
- Manual partition creation without corresponding data

**Diagnostic Steps:**
1. Compare Dagster partitions to membership rows (evaluation stage):
   ```bash
   awk -F',' 'NR==1 || $1=="evaluation"' data/cohorts/*/membership.csv | wc -l
   ```

2. Look for the specific `gen_id` in membership:
   ```bash
   grep ",<evaluation_gen_id>," data/cohorts/*/membership.csv
   ```

**Solutions:**
```bash
# Refresh cohort partitions (prunes cohort-scoped stale keys, re-registers)
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f src/daydreaming_dagster/definitions.py

# Restart Dagster to clear in-memory partition caches if needed
```

### 4. Base Directory Not Found

**Error Message:**
```
Base directory exists: False
Expected path: /path/to/data/gens/essay/<ESSAY_GEN_ID>/parsed.txt
```

**Symptoms:**
- IO manager cannot find the base directory
- Directory structure is missing or incorrect
- File paths don't match expected structure

**Root Causes:**
- Data directories not created by pipeline
- Incorrect `data_root` configuration in resources
- Permission issues or disk space problems

**Diagnostic Steps:**
1. Check directory structure:
   ```bash
   tree data/gens -L 3
   ls -la data/gens/essay/
   ```

2. Verify IO manager configuration:
   ```bash
   # Check the definitions.py for correct paths
   grep "generation_response_io_manager" src/daydreaming_dagster/definitions.py
   ```

3. Check permissions and disk space:
   ```bash
   df -h .
   ls -ld data/gens/essay/
   ```

**Solutions:**
```bash
# Rebuild cohort membership (registers partitions) and materialize required assets
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f src/daydreaming_dagster/definitions.py
uv run dagster asset materialize --select "essay_prompt,essay_raw,essay_parsed" --partition "<ESSAY_GEN_ID>" -f src/daydreaming_dagster/definitions.py
```

---

## Monitoring & Recovery

### General Debugging Techniques

#### Enable Verbose Logging

Add temporary debug logging to assets:
```python
context.log.info(f"Available partitions: {available_partitions}")
context.log.info(f"Current working directory: {os.getcwd()}")
context.log.info(f"IO manager path: {gen_response_io_manager.base_path}")
```

#### Check Asset Dependencies

Verify the dependency chain is correct:
```bash
# Check materialization order
uv run dagster asset materialize --select "+evaluation_prompt" -f src/daydreaming_dagster/definitions.py --dry-run
```

#### Validate Data Integrity

Create a validation script:
```python
import pandas as pd

# Load both task tables
from pathlib import Path
membership = pd.read_csv(next(Path("data/cohorts").glob("*/membership.csv")))
eval_tasks = membership[membership["stage"]=="evaluation"][[
    "gen_id","parent_gen_id","template_id","llm_model_id"
]].rename(columns={
    "template_id":"evaluation_template",
    "llm_model_id":"evaluation_llm_model"
})

# Check parent link integrity (each eval's parent_gen_id must be present as an essay gen_id)
essay_ids = set(membership[membership["stage"]=="essay"]["gen_id"].astype(str))
eval_parents = set(eval_tasks["parent_gen_id"].astype(str))
orphaned = sorted(eval_parents - essay_ids)
print(f"Orphaned evaluation parents: {orphaned[:10]} (showing up to 10)")

# Check for duplicate evaluation gen_ids
dups = eval_tasks["gen_id"].duplicated().sum()
print(f"Duplicate evaluation gen_ids: {dups}")
```

#### Monitor Resource Usage

Check if resource constraints are causing issues:
```bash
# Check disk space
df -h data/

# Check memory usage during materialization
htop

# Check for large files in gens store
du -sh data/gens | head -20
```

### Recovery Procedures

#### Complete Pipeline Reset

If multiple issues persist:
```bash
# 1. Clear cohort membership and gens store (keep raw data)
rm -rf data/cohorts/*
rm -rf data/gens/*

# 2. Rebuild cohort and verify
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f src/daydreaming_dagster/definitions.py
head data/cohorts/*/membership.csv

# 3. Run a small subset of LLM assets to test (by gen_id)
uv run dagster asset materialize --select draft_prompt,draft_raw,draft_parsed --partition <DRAFT_GEN_ID> -f src/daydreaming_dagster/definitions.py
```

#### Selective Partition Recovery

For specific broken partitions:
```bash
# 1. Identify the broken partition (by gen_id)
ESSAY_GEN_ID=<id>

# 2. Rematerialize the essay assets for that gen_id
uv run dagster asset materialize --select essay_prompt,essay_raw,essay_parsed --partition ${ESSAY_GEN_ID} -f src/daydreaming_dagster/definitions.py

# 3. If the parent draft is missing, materialize it first
uv run dagster asset materialize --select draft_prompt,draft_raw,draft_parsed --partition <DRAFT_GEN_ID> -f src/daydreaming_dagster/definitions.py
```

### When to Escalate

Contact a developer if:
- Multiple partitions consistently fail after following recovery procedures
- Data corruption persists after complete pipeline reset
- Resource configuration issues require code changes
- New error patterns not covered in this guide
- Foreign key validation logic needs updates

### Monitoring Recommendations

Set up monitoring for:
- High failure rate on evaluation assets (>5% of partitions failing)
- Increasing numbers of orphaned FK references
- Unusual patterns in essay_task_id references
- IO manager path configuration drift
- Asset dependency chain breaks

---

## Notes

- This is a no-code-change workflow: flipping concept/template `active` flags controls which combinations are built and executed
- Stable combo IDs are versioned and persisted in `data/combo_mappings.csv` for cross-run analysis
- If you later want stronger isolation (per-experiment folders and automatic propagation of `experiment_id` into paths/metadata), see `plans/experiment_management_plan.md` for a low-friction enhancement using Dagster run tags

For more detailed technical architecture, see `docs/architecture/architecture.md`.
