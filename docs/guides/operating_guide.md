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
   uv run dagster dev -f daydreaming_dagster/definitions.py
   ```

### Controlling Overwrites of Generated Files

By default, generated responses (links/essays/evaluations) are write‑once for safety: existing files are not overwritten. To allow overwriting during reruns (useful for experimentation):

```bash
# Artifacts are versioned automatically as {id}_vN.txt; no overwrite flag needed
uv run dagster dev -f daydreaming_dagster/definitions.py
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
data/1_raw/generation_templates/
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
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "draft_prompt,draft_response,essay_prompt,essay_response" \
  --partition "TASK_ID"

# Or run by asset group
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
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
  - Add a file under `data/1_raw/generation_templates/draft/<template_id>.txt`.
  - Add a row to `data/1_raw/draft_templates.csv` with the same `template_id`, set `active=true`, and set `parser` if needed.

**Essay Templates** (`data/1_raw/essay_templates.csv`):
- Set desired essay-phase templates to `active: true`.
- Generator column (`generator`): `llm` (default) uses the parsed draft as input; `copy` returns the parsed draft verbatim. Essay‑level parser mode is deprecated after parser‑first.

This limits generation to specific prompt styles for each phase of the two-phase generation process. You can also override the template root by setting `GEN_TEMPLATES_ROOT` (defaults to `data/1_raw/generation_templates`).

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
   Ensure the daemon is running so raw data loaders and task definitions auto-update when `data/1_raw/**/*` changes.
   ```bash
   export DAGSTER_HOME=$(pwd)/dagster_home
   uv run dagster dev -f daydreaming_dagster/definitions.py
   ```

   Raw loaders are standalone (no observable sources). When files under `data/1_raw/**/*` change, re‑materialize `group:raw_data` to refresh downstream tasks.

   Optional one-time seed (creates initial task CSVs and partitions):
   ```bash
   uv run dagster asset materialize -f daydreaming_dagster/definitions.py --select "group:task_definitions"
   ```

2. **Run generation assets:**
   ```bash
   uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
    --select "group:generation_draft,group:generation_essays"
   ```

3. **Run evaluation assets:**
   ```bash
   uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
    --select "group:evaluation"
   ```

4. **Process results and run analysis:**
   ```bash
   uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
     --select "group:results_processing,group:results_summary"
   ```

### Auto-Materializing Assets

Only `raw_data` and `task_definitions` use eager auto‑materialization during development:

- Raw loaders (`group:raw_data`) can be re‑materialized after editing files under `data/1_raw/**/*`.
- Task definitions (`group:task_definitions`) refresh when raw assets update.

Cross‑experiment tracking no longer uses auto‑appenders. Use analysis assets (`filtered_evaluation_results`, `template_version_comparison_pivot`) and scripts for backfills under `data/7_cross_experiment/`.

---

## Unified Evaluation Update (2025-09)

This repository now uses a generation-centric evaluation flow:

- `document_index` unifies drafts, two-phase essays, and legacy one-phase documents with normalized columns and concrete `file_path`.
- `evaluation_tasks` creates partitions with IDs formatted as `{parent_gen_id}__{evaluation_template}__{evaluation_model_id}` (gen‑id first).
- `evaluation_prompt` loads the source document by `file_path` and renders the evaluation template (`response=<document text>`). No cross-partition IO is required for evaluation.
- `parsed_scores` contains normalized outputs and `generation_response_path` sourced from the document `file_path`. When available, use `parent_gen_id` as the canonical key (first token of `evaluation_task_id`).
- Pivots and selection scripts should key rows by `parent_gen_id` (the essay generation id for evaluations) for deterministic grouping across runs.

Lineage and IDs (gen‑id first)
- `gen_id`: unique identifier of a concrete generation under `data/gens/<stage>/<gen_id>/`.
- `parent_gen_id`:
  - Drafts: none (no parent).
  - Essays: `parent_gen_id` = `gen_id` of the draft refined into the essay.
  - Evaluations: `parent_gen_id` = `gen_id` of the essay being evaluated.
- Tasks and assets must pass/require `parent_gen_id` for essays and evaluations (fail fast if missing). This removes all “latest‑by‑task” ambiguity and makes pivots deterministic.

Note on legacy directory scan:
- Evaluation no longer discovers documents by scanning `data/3_generation/generation_responses/`. To evaluate historical outputs, write standard generation task CSVs (draft/essay) and place/symlink their texts under `draft_responses/` (or legacy `links_responses/`) or `essay_responses/`. Then materialize `evaluation_tasks` to register only those curated documents.

### Targeted Evaluations (No full cube)

To run a specific evaluation (e.g., `novelty`) only on chosen documents (e.g., prior-art winners):

1. Ensure the evaluation template exists and is active in `data/1_raw/evaluation_templates.csv`.
2. Materialize `evaluation_tasks` once to register partitions.
3. Materialize only the desired partitions by key (gen‑id first):
   ```bash
   uv run dagster asset materialize --select "evaluation_prompt,evaluation_response" \
     --partition "{parent_gen_id}__novelty__{evaluation_model_id}" \
     -f daydreaming_dagster/definitions.py
   ```
4. Re-run `parsed_scores` to ingest the new results.

For cross-experiment winners, place or symlink their generation texts under the canonical folders (`draft_responses/` (or legacy `links_responses/`), `essay_responses/`, or `generation_responses/`) so they appear in `document_index`/`evaluation_tasks` without changing CSV actives.

### Curated Selection Quick Start (Drafts, Essays, Evaluations)

Use the selection script to write a list of essay gen_ids, then let Dagster build the cohort and register partitions (no need to change `k_max`). If `data/7_cross_experiment/parsed_scores.csv` is missing, build it first:
`uv run python scripts/aggregate_scores.py --output data/7_cross_experiment/parsed_scores.csv`.
In all examples below, treat `parent_gen_id` as the canonical key for evaluation pivots and selections.

1) Select top‑N prior‑art winners (editable list)
```bash
uv run python scripts/select_top_prior_art.py --top-n 30 --parsed-scores data/7_cross_experiment/parsed_scores.csv
# Edit data/2_tasks/selected_essays.txt if desired
```

2) Build cohort membership and register partitions (inside Dagster)
```bash
export DAGSTER_HOME="$(pwd)/dagster_home"
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f daydreaming_dagster/definitions.py
```

What it does
- Writes `data/cohorts/<cohort_id>/membership.csv` with normalized rows (same columns for all stages):
  `stage, gen_id, cohort_id, parent_gen_id, combo_id, template_id, llm_model_id`.
- Registers dynamic partitions add‑only for draft/essay/evaluation.
- Enforces parent integrity within the cohort.

3) Run in Dagster
- Drafts: materialize `draft_prompt,draft_response` for selected partitions (by gen_id)
- Essays: materialize `essay_prompt,essay_response`
- Evaluations: materialize `evaluation_prompt,evaluation_response`

Tip
- Set `DD_COHORT=<cohort_id>` to bind generation/evaluation seeds; task assets compute/persist a deterministic cohort id when not provided.
- Use `--dry-run` to preview changes.

### Where Assets Live

- task_definitions: `daydreaming_dagster/assets/group_task_definitions.py`
- generation_draft: `daydreaming_dagster/assets/group_generation_draft.py`
- generation_essays: `daydreaming_dagster/assets/group_generation_essays.py`
- evaluation: `daydreaming_dagster/assets/group_evaluation.py`
- results_processing: `daydreaming_dagster/assets/group_results_processing.py`
- results_summary: `daydreaming_dagster/assets/group_results_summary.py`
- cross_experiment: `daydreaming_dagster/assets/group_cross_experiment.py`

Quick navigation
- Asset map: `uv run python scripts/asset_map.py` prints `asset_key | group | file:line`.
- CLI grep: `rg -n '@asset\(' daydreaming_dagster/assets` or `rg -n 'group_name="generation_essays"' daydreaming_dagster/assets`.

**Note**: Auto-materialization requires the Dagster daemon to be running. In development, you can manually trigger assets if needed:
```bash
# Manually materialize a specific asset
uv run dagster asset materialize --select "content_combinations,draft_generation_tasks,essay_generation_tasks,evaluation_tasks" -f daydreaming_dagster/definitions.py
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
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "content_combinations,generation_tasks,generation_prompt,generation_response_free" \
  --tag experiment_id=exp_free_vs_paid

# Paid generation (parallel per pool)
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "content_combinations,generation_tasks,generation_prompt,generation_response_paid" \
  --tag experiment_id=exp_free_vs_paid
```

For evaluation with a paid model:
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "evaluation_tasks,evaluation_prompt,evaluation_response_paid" \
  --tag experiment_id=exp_free_vs_paid
```

### Output Locations

- Generated/evaluated files follow the existing `data/` folder conventions:
  - `data/3_generation/` - Generation prompts and responses
    - **Two‑Phase**: `draft_prompts/`, `draft_responses/`, `essay_prompts/`, `essay_responses/`
    - **Legacy**: `generation_prompts/`, `generation_responses/` (deprecated)
    - **Legacy interface**: `parsed_generation_responses/` (historical; two‑phase writes directly to draft_/essay_)
  - `data/4_evaluation/` - Evaluation prompts and responses
  - `data/5_parsing/` - Parsed evaluation scores
  - `data/6_summary/` - Final aggregated results
  - `data/7_cross_experiment/` - Cross-experiment tracking tables (NEW)

- If you used a run tag, the tag appears in Dagster's run metadata for filtering

### Bulk Results Table Generation

For initial setup or when you need to rebuild the cross‑experiment outputs from existing gens:

```bash
# Build parsed scores and pivot from the gens store
uv run python scripts/aggregate_scores.py --output data/7_cross_experiment/parsed_scores.csv
uv run python scripts/build_pivot_tables.py --parsed-scores data/7_cross_experiment/parsed_scores.csv
```

These scripts scan existing gens and produce canonical outputs under `data/7_cross_experiment/`.

### Optional Reporting

Use the scripts/assets that construct reports under `reports/`. Add an `experiment_id` column to any CSV you export for later cross-experiment analysis.

---

## Troubleshooting Common Issues

### 1. Missing Generation Response (FileNotFoundError)

**Error Message:**
```
Missing generation response required for evaluation task 'eval_001_creativity_claude' (FK: combo_v1_1f3a9c2d7b2c_essay-inventive-synthesis_claude_f)
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
1. Check if the referenced `essay_task_id` exists:
   ```bash
   ls data/3_generation/generation_responses/ | grep combo_001_essay-inventive-synthesis_claude_f
   ```

2. Check Dagster logs for `generation_response` materialization:
   ```bash
   # In Dagster UI: Assets -> generation_response -> filter by partition
   ```

3. Verify the FK relationship in evaluation_tasks:
   ```bash
   grep "combo_.*_essay-inventive-synthesis_claude_f" data/2_tasks/evaluation_tasks.csv
   ```

**Solutions:**
```bash
# Option 1: Materialize the specific missing partition
uv run dagster asset materialize --select generation_response --partition combo_001_essay-inventive-synthesis_claude_f -f daydreaming_dagster/definitions.py

# Option 2: Materialize all generation responses (if many are missing)
uv run dagster asset materialize --select generation_response -f daydreaming_dagster/definitions.py

# Option 3: Re-materialize the entire generation chain
uv run dagster asset materialize --select "+generation_response" -f daydreaming_dagster/definitions.py
```

**Prevention:**
- Always materialize generation assets before evaluation assets
- Use asset group materialization: `group:generation_draft,group:generation_essays` before `group:evaluation`
- Set up monitoring alerts for failed generation partitions

### 2. Invalid Foreign Key Reference

**Error Message:**
```
Invalid essay_task_id referenced by evaluation task 'eval_001': ''
```

**Symptoms:**
- `evaluation_prompt` fails immediately with FK validation error
- `essay_task_id` field is empty, null, or malformed
- Error occurs before any file access attempts

**Root Causes:**
- Data corruption in `evaluation_tasks.csv`
- Bug in evaluation task generation logic
- CSV parsing issues (missing quotes, special characters)

**Diagnostic Steps:**
1. Inspect the evaluation tasks CSV directly:
   ```bash
   head -10 data/2_tasks/evaluation_tasks.csv
   grep "eval_001" data/2_tasks/evaluation_tasks.csv
   ```

2. Check for empty or malformed FK values:
   ```bash
   awk -F',' '$2 == "" {print "Empty FK in line: " NR ": " $0}' data/2_tasks/evaluation_tasks.csv
   ```

3. Validate all FK references:
   ```bash
   # Extract all essay_task_ids from evaluation_tasks
   cut -d',' -f2 data/2_tasks/evaluation_tasks.csv | sort | uniq > eval_fks.txt
   
   # Extract all essay_task_ids from essay_generation_tasks  
   cut -d',' -f1 data/2_tasks/essay_generation_tasks.csv | sort | uniq > essay_ids.txt
   
   # Find orphaned FKs
   comm -23 eval_fks.txt essay_ids.txt
   ```

**Solutions:**
```bash
# Option 1: Re-materialize task creation pipeline (tasks only)
uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py

# Option 2: Check for underlying data issues
# Inspect concepts, templates, and models for corruption
head data/1_raw/concepts_metadata.csv
head data/1_raw/draft_templates.csv
head data/1_raw/essay_templates.csv
head data/1_raw/llm_models.csv

# Option 3: Clear and rebuild all task data
rm -f data/2_tasks/*.csv
uv run dagster asset materialize --select generation_tasks,evaluation_tasks -f daydreaming_dagster/definitions.py
```

### 3. Evaluation Task Not Found in DataFrame

**Error Message:**
```
Evaluation task 'eval_123_creativity_claude' not found in task database
```

**Symptoms:**
- Asset fails at the beginning when looking up partition in evaluation_tasks
- Error shows available task samples
- Partition exists in Dagster but not in CSV data

**Root Causes:**
- Stale partition definitions (CSV updated but partitions not refreshed)
- Race condition between partition creation and CSV generation
- Manual partition creation without corresponding data

**Diagnostic Steps:**
1. Check partition count vs CSV row count:
   ```bash
   # Count CSV rows
   wc -l data/2_tasks/evaluation_tasks.csv
   
   # Check Dagster partitions (use Dagster UI Assets page)
   ```

2. Look for the specific partition in CSV:
   ```bash
   grep "eval_123_creativity_claude" data/2_tasks/evaluation_tasks.csv
   ```

3. Check for recent task regeneration:
   ```bash
   ls -la data/2_tasks/evaluation_tasks.csv  # Check modification time
   ```

**Solutions:**
```bash
# Option 1: Refresh evaluation_tasks and partitions
uv run dagster asset materialize --select evaluation_tasks -f daydreaming_dagster/definitions.py

# Option 2: If partitions are out of sync, restart Dagster
# (This clears in-memory partition caches)

# Option 3: Full task rebuild
uv run dagster asset materialize --select generation_tasks,evaluation_tasks -f daydreaming_dagster/definitions.py
```

### 4. Base Directory Not Found

**Error Message:**
```
Base directory exists: False
Expected path: /path/to/data/3_generation/generation_responses/combo_001.txt
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
   tree data/ -L 3
   ls -la data/3_generation/
   ```

2. Verify IO manager configuration:
   ```bash
   # Check the definitions.py for correct paths
   grep "generation_response_io_manager" daydreaming_dagster/definitions.py
   ```

3. Check permissions and disk space:
   ```bash
   df -h .
   ls -ld data/3_generation/generation_responses/
   ```

**Solutions:**
```bash
# Option 1: Create missing directories
mkdir -p data/3_generation/generation_responses
mkdir -p data/4_evaluation/evaluation_prompts
mkdir -p data/4_evaluation/evaluation_responses

# Option 2: Seed task definitions (creates expected directories under data/2_tasks/)
uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py

# Option 3: Check and fix permissions
chmod 755 data/3_generation/generation_responses/
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
uv run dagster asset materialize --select "+evaluation_prompt" -f daydreaming_dagster/definitions.py --dry-run
```

#### Validate Data Integrity

Create a validation script:
```python
import pandas as pd

# Load both task tables
essay_tasks = pd.read_csv("data/2_tasks/essay_generation_tasks.csv")
eval_tasks = pd.read_csv("data/2_tasks/evaluation_tasks.csv")

# Check FK integrity
essay_ids = set(essay_tasks["essay_task_id"])
eval_fks = set(eval_tasks["essay_task_id"])

orphaned = eval_fks - essay_ids
print(f"Orphaned FKs: {orphaned}")

# Check for duplicates
print(f"Duplicate eval tasks: {eval_tasks['evaluation_task_id'].duplicated().sum()}")
```

#### Monitor Resource Usage

Check if resource constraints are causing issues:
```bash
# Check disk space
df -h data/

# Check memory usage during materialization
htop

# Check for large files
du -sh data/3_generation/generation_responses/ | head -20
```

### Recovery Procedures

#### Complete Pipeline Reset

If multiple issues persist:
```bash
# 1. Clear all generated data (keep raw data)
rm -rf data/2_tasks/*.csv
rm -rf data/3_generation/*
rm -rf data/4_evaluation/*

# 2. Seed task definitions (daemon will keep them updated)
uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py

# 3. Verify task integrity before proceeding
head data/2_tasks/generation_tasks.csv
head data/2_tasks/evaluation_tasks.csv

# 4. Run a small subset of LLM assets to test
uv run dagster asset materialize --select generation_prompt --partition combo_001_essay-inventive-synthesis_claude_f -f daydreaming_dagster/definitions.py
```

#### Selective Partition Recovery

For specific broken partitions:
```bash
# 1. Identify the broken partition
PARTITION="combo_001_essay-inventive-synthesis_claude_f"

# 2. Clean up any partial files
rm -f data/3_generation/generation_responses/${PARTITION}.txt
rm -f data/4_evaluation/evaluation_prompts/*${PARTITION}*

# 3. Re-materialize the chain
uv run dagster asset materialize --select generation_prompt,generation_response --partition ${PARTITION} -f daydreaming_dagster/definitions.py
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
