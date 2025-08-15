# DayDreaming Operating Guide

This comprehensive guide covers everything you need to know to set up, run, and troubleshoot DayDreaming experiments.

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
   export DAGSTER_HOME="./dagster_home"
   ```

3. **Verify configuration:**
   ```bash
   uv run dagster dev -f daydreaming_dagster/definitions.py
   ```

### Experiment Configuration (No-Code Workflow)

The system uses CSV-based configuration for easy experiment setup without code changes.

#### 1. Choose Active Concepts

Edit `data/1_raw/concepts/concepts_metadata.csv` and set `active` to:
- `default-mode-network`: True
- `mind-wandering`: False  
- `combinatorial-creativity`: False
- Keep other required concepts active (e.g., `dearth-ai-discoveries`, `adversarial-evaluation`, `economic-moat`)

This drives the first pipeline step to regenerate `content_combinations` using only the active concepts.

#### 2. Restrict Generation Templates

Edit `data/1_raw/generation_templates.csv` and ensure only desired templates are active:
- `creative-synthesis-v2`: true
- `essay-inventive-synthesis`: true
- All others: false

This limits generation to specific prompt styles.

#### 3. Mark Experiment Metadata

Use a Dagster run tag to label the run, e.g. `experiment_id=exp_default_mode_only` (via Dagit Launchpad or job definition).

Optionally stash selection files for traceability:
- `data/experiments/exp_default_mode_only/active_concepts.csv`
- `data/experiments/exp_default_mode_only/active_generation_templates.csv`
- `data/experiments/exp_default_mode_only/active_evaluation_templates.csv`

---

## Running Experiments

### Basic Pipeline Execution

1. **Generate setup assets and tasks:**
   ```bash
   uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
     --select "group:raw_data,group:llm_tasks"
   ```

2. **Run generation assets:**
   ```bash
   uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
     --select "group:llm_generation"
   ```

3. **Run evaluation assets:**
   ```bash
   uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
     --select "group:llm_evaluation"
   ```

4. **Process results and run analysis:**
   ```bash
   uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
     --select "group:results_processing,group:results_summary"
   ```

### Auto-Materializing Results Tracking

**NEW**: The pipeline now includes auto-materializing assets that automatically track results:

- **`generation_results_append`**: Automatically appends a row to `generation_results.csv` when any `generation_response` completes
- **`evaluation_results_append`**: Automatically appends a row to `evaluation_results.csv` when any `evaluation_response` completes

These assets run automatically without manual intervention and maintain comprehensive cross-experiment tracking tables.

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
  --select "content_combinations,content_combinations_csv,generation_tasks,generation_prompt,generation_response_free" \
  --tag experiment_id=exp_free_vs_paid

# Paid generation (parallel per pool)
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "content_combinations,content_combinations_csv,generation_tasks,generation_prompt,generation_response_paid" \
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
  - `data/4_evaluation/` - Evaluation prompts and responses
  - `data/5_parsing/` - Parsed evaluation scores
  - `data/6_summary/` - Final aggregated results
  - `data/7_cross_experiment/` - Cross-experiment tracking tables (NEW)

- If you used a run tag, the tag appears in Dagster's run metadata for filtering

### Bulk Results Table Generation

For initial setup or when you need to rebuild the cross-experiment tracking tables from existing data:

```bash
# Generate generation_results.csv from all existing generation responses
./scripts/rebuild_generation_results.sh

# Generate evaluation_results.csv from all existing evaluation responses  
python scripts/build_evaluation_results_table.py
```

These scripts scan existing response files and rebuild the comprehensive tracking tables. Useful for:
- Initial migration to the new tracking system
- Recovery after table corruption
- Rebuilding tables when adding new columns

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
1. Check if the referenced `generation_task_id` exists:
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
- Use asset group materialization: `group:llm_generation` before `group:llm_evaluation`
- Set up monitoring alerts for failed generation partitions

### 2. Invalid Foreign Key Reference

**Error Message:**
```
Invalid generation_task_id referenced by evaluation task 'eval_001': ''
```

**Symptoms:**
- `evaluation_prompt` fails immediately with FK validation error
- `generation_task_id` field is empty, null, or malformed
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
   # Extract all generation_task_ids from evaluation_tasks
   cut -d',' -f2 data/2_tasks/evaluation_tasks.csv | sort | uniq > eval_fks.txt
   
   # Extract all generation_task_ids from generation_tasks  
   cut -d',' -f1 data/2_tasks/generation_tasks.csv | sort | uniq > gen_ids.txt
   
   # Find orphaned FKs
   comm -23 eval_fks.txt gen_ids.txt
   ```

**Solutions:**
```bash
# Option 1: Re-materialize task creation pipeline
uv run dagster asset materialize --select "group:raw_data,group:llm_tasks" -f daydreaming_dagster/definitions.py

# Option 2: Check for underlying data issues
# Inspect concepts, templates, and models for corruption
head data/1_raw/concepts/concepts_metadata.csv
head data/1_raw/generation_templates.csv
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

# Option 2: Re-run raw data materialization (creates directories)
uv run dagster asset materialize --select "group:raw_data" -f daydreaming_dagster/definitions.py

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
gen_tasks = pd.read_csv("data/2_tasks/generation_tasks.csv")
eval_tasks = pd.read_csv("data/2_tasks/evaluation_tasks.csv")

# Check FK integrity
gen_ids = set(gen_tasks["generation_task_id"])
eval_fks = set(eval_tasks["generation_task_id"])

orphaned = eval_fks - gen_ids
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

# 2. Restart from raw data
uv run dagster asset materialize --select "group:raw_data,group:llm_tasks" -f daydreaming_dagster/definitions.py

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
- Unusual patterns in generation_task_id references
- IO manager path configuration drift
- Asset dependency chain breaks

---

## Notes

- This is a no-code-change workflow: flipping concept/template `active` flags controls which combinations are built and executed
- Stable combo IDs are versioned and persisted in `data/combo_mappings.csv` for cross-run analysis
- If you later want stronger isolation (per-experiment folders and automatic propagation of `experiment_id` into paths/metadata), see `plans/experiment_management_plan.md` for a low-friction enhancement using Dagster run tags

For more detailed technical architecture, see `docs/architecture/architecture.md`.
