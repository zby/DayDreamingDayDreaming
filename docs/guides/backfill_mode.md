# Backfill Mode: Reusing Existing Evaluations

## Overview

When creating evaluation cohorts in "backfill mode" (using the `# include-existing-evaluations` directive), the system includes existing evaluations in the cohort membership and skips regenerating them during materialization.

## Problem: Partition Materialization Status

### What We Tried (And Why It Didn't Work)

Initially, we attempted to use Dagster's `report_runless_asset_event()` API to mark existing partitions as materialized when re-registering them in a new cohort:

```python
# This approach did NOT work
from dagster import AssetKey, AssetMaterialization

mat = AssetMaterialization(
    asset_key=AssetKey(["evaluation_parsed"]),
    partition=gen_id,
    description="Existing evaluation marked as materialized",
)
instance.report_runless_asset_event(mat)
```

**Why this failed:**
- Dagster's partition materialization status is computed from the event log of actual **runs**, not runless events
- When partitions are pruned and re-registered, historical run data is lost
- `report_runless_asset_event()` reports events to the log, but doesn't affect the UI's partition status view
- Partitions showed as "unmaterialized" even after reporting events, causing unnecessary backfill attempts

### Solution: Skip Logic in Assets

Instead of trying to manipulate Dagster's materialization tracking, we check for existing work at the start of each stage asset:

```python
def evaluation_prompt(context) -> str:
    data_layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = str(context.partition_key)

    # Skip if parsed already exists (backfill mode support)
    if data_layer.parsed_exists(EVALUATION_STAGE, gen_id):
        input_text = data_layer.read_input(EVALUATION_STAGE, gen_id)
        context.log.info(f"Skipping {gen_id}: parsed.txt already exists")
        return input_text

    # ... normal processing
```

**Why this works:**
- Assets actually execute and return data (even if from existing files)
- Dagster marks them as materialized through normal execution flow
- The event log correctly reflects that the asset ran
- Partition status updates naturally in the UI
- No API hacks or workarounds needed

## Implementation Details

### Where Skip Logic Lives

The skip logic is implemented in all three evaluation stage assets (as of commit `29549ec`):

1. **`evaluation_prompt`** (`src/daydreaming_dagster/assets/group_evaluation.py:41-45`)
   - Checks `data_layer.parsed_exists()`
   - Returns existing `input.txt` if parsed exists

2. **`evaluation_raw`** (`src/daydreaming_dagster/assets/group_evaluation.py:70-74`)
   - Checks `data_layer.parsed_exists()`
   - Returns existing `raw.txt` if parsed exists

3. **`evaluation_parsed`** (`src/daydreaming_dagster/assets/group_evaluation.py:109-113`)
   - Checks `data_layer.parsed_exists()`
   - Returns existing `parsed.txt` if parsed exists

### Helper Method

The `GensDataLayer.parsed_exists()` method (`src/daydreaming_dagster/data_layer/gens_data_layer.py:123-126`):

```python
def parsed_exists(self, stage: str, gen_id: str) -> bool:
    """Check if parsed.txt exists for the given stage and gen_id."""
    target = self._paths.parsed_path(stage, gen_id)
    return target.exists()
```

## Usage

### Creating a Backfill Cohort

1. **Generate the selection file:**
   ```bash
   uv run python scripts/select_missing_novelty_v2.py \
     --cohort novelty_v2_backfill \
     --top-n 30
   ```

2. **The script writes to `data/2_tasks/selected_essays.txt`:**
   ```
   # mode: evaluation-only
   # include-existing-evaluations
   e_1hbnjo1jogt1adhn
   e_2coj0l79j102vbge
   ...
   ```

3. **Set cohort environment variable:**
   ```bash
   export DD_COHORT=novelty_v2_backfill
   ```

4. **Materialize cohort assets:**
   - In Dagster UI: Materialize `group:cohort`
   - This creates membership with existing evaluations limited by `replication_config.csv`

5. **Run evaluation backfill:**
   - Select all three evaluation assets or use the `evaluation` asset group
   - Launch backfill for all partitions
   - Existing evaluations will log: `Skipping {gen_id}: parsed.txt already exists`
   - Only missing evaluations will call the LLM

### Checking Skip Behavior

**In Dagster UI:**
1. Go to the backfill run
2. Click on a partition that should be skipped (one with existing `parsed.txt`)
3. View logs for `evaluation_prompt` asset
4. Look for: `Skipping {gen_id}: parsed.txt already exists`

**From command line:**
```bash
# List evaluations that should be skipped
grep "^evaluation," data/cohorts/novelty_v2_backfill/membership.csv \
  | cut -d, -f2 \
  | xargs -I {} sh -c 'if [ -f "data/gens/evaluation/{}/parsed.txt" ]; then echo "{}"; fi'
```

## Cohort Membership Limiting

The `_existing_evaluations_by_combo()` function finds all existing evaluations for an essay, but the backfill logic limits them to the replication count (as of commit `3a31397`):

```python
# Only include up to eval_reps existing evaluations
existing_to_include = sorted(current)[:eval_reps]
for existing_gen_id in existing_to_include:
    rows.append({
        "stage": "evaluation",
        "gen_id": existing_gen_id,
        ...
    })
```

This ensures that if 50 evaluations exist for an essay+template+model but `replication_config.csv` says `evaluation=4`, only the first 4 (lexicographically sorted by gen_id) are included in the cohort.

> **Reminder:** backfills never rewrite `origin_cohort_id` inside existing `metadata.json` files. That field records the cohort that first produced an artifact and is not used to register partitions. Always rely on the freshly written `data/cohorts/<cohort_id>/membership.csv` (and the `cohort_id` column) to understand which runs belong to the current cohort.

## Related Files

- `src/daydreaming_dagster/assets/group_evaluation.py` - Skip logic in evaluation assets
- `src/daydreaming_dagster/assets/group_cohorts.py:906-922` - Limiting existing evaluations to replicate count
- `src/daydreaming_dagster/data_layer/gens_data_layer.py:123-126` - `parsed_exists()` method
- `tests/data_layer/test_gens_data_layer.py:69-78` - Test coverage for `parsed_exists()`
- `scripts/select_missing_novelty_v2.py` - Script for generating backfill selection files

## Design Philosophy

This approach follows the principle of **working with Dagster's execution model** rather than trying to manipulate its internal state:

- ✅ Assets execute normally and return data
- ✅ Event log reflects actual execution
- ✅ Partition status updates through standard flow
- ✅ No reliance on undocumented or fragile APIs
- ✅ Clear, maintainable code that's easy to debug

Rather than asking "How do we trick Dagster into thinking this ran?", we ask "How do we make the asset run in a way that skips unnecessary work?"
