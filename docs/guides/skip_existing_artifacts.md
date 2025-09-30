# Skip Existing Artifacts - Implementation Summary

## Overview

Stage-level guards that reuse previously materialized artifacts by default. If `raw.txt` or `parsed.txt` already exists, the Dagster asset returns the stored data instead of invoking the LLM or parser again.

**Status:** Implemented ✅

## Key Changes

### 1. Data Layer Helpers (`gens_data_layer.py`)

Added existence check methods:
- `raw_exists(stage, gen_id) -> bool`
- `raw_metadata_exists(stage, gen_id) -> bool`
- `parsed_exists(stage, gen_id)` (already existed)

### 2. StageSettings Configuration (`experiment_config.py`)

Added `force` flag to `StageSettings`:

```python
class StageSettings(Config):
    generation_max_tokens: int | None = None
    min_lines: int | None = None
    force: bool = False  # NEW: explicit regeneration flag
```

**Default:** `force=False` (reuse existing artifacts)
**Override:** Set `force=True` to regenerate regardless of existing files

### 3. Stage Implementation

#### `_stage_raw_asset` (stage_raw.py)

```python
# Check if we should skip regeneration
force = stage_settings.force if stage_settings else False
if not force and data_layer.raw_exists(stage, gen_id):
    # Reuse existing artifact
    raw_text = data_layer.read_raw(stage, gen_id)
    # Try to read metadata, warn if missing but don't fail
    raw_metadata["reused"] = True
    return raw_text, raw_metadata
# ... normal generation path
raw_metadata["reused"] = False
```

**Behavior:**
- Checks for `raw.txt` existence before generating
- Reads existing file if present (unless `force=True`)
- Attempts to read `raw_metadata.json`, warns if missing but proceeds
- Emits `reused: true/false` in metadata

#### `_stage_parsed_asset` (stage_parsed.py)

Same pattern as raw stage:
- Checks for `parsed.txt` existence
- Reuses if present and `force=False`
- Warns if `parsed_metadata.json` missing
- Emits `reused` metadata flag

### 4. Testing

**Unit Tests** (`test_stage_raw.py`, `test_stage_parsed.py`):
- `test_stage_raw_skips_existing_artifact` - verifies skip with full metadata
- `test_stage_raw_force_regenerates` - verifies `force=True` override
- `test_stage_raw_skips_with_missing_metadata` - verifies skip with warning
- Equivalent tests for parsed stage

**Results:** All 32 unified stage tests pass ✅

### 5. Documentation

Updated:
- `docs/guides/operating_guide.md` - Operator guide with usage examples
- `src/daydreaming_dagster/resources/experiment_config.py` - Inline docstring
- `src/daydreaming_dagster/assets/group_cohorts.py` - Note about mode simplification

## Usage

### Default (Reuse Existing)

```bash
# Second materialization reuses existing files
dagster asset materialize --select draft_raw --partition D123
# Output metadata: reused: true (LLM not called)
```

### Force Regeneration

```python
# In definitions.py or run config
ExperimentConfig(
    stage_config={
        "draft": StageSettings(force=True),  # Always regenerate
    }
)
```

### New Variants (Replication)

```csv
# data/1_raw/replication_config.csv
stage,replicates
draft,3  # Incrementing this creates new gen_ids -> new files
```

## Design Decisions

### Skip when metadata missing?
**Decision:** Yes, skip with a warning.

**Rationale:**
- Primary artifact is the text file (what downstream stages consume)
- Metadata is supplementary (timestamps, model info)
- Forcing regeneration wastes LLM calls
- Warning provides visibility without blocking

### Extend to other derivatives?
**Decision:** Not initially - handle on-demand.

**Rationale:**
- YAGNI principle
- Current pain point is raw/parsed regeneration (expensive ops)
- Other derivatives are fast or don't exist yet
- Apply same pattern when needed

## Benefits

1. **Cost savings:** Avoid redundant LLM API calls
2. **Time savings:** Instant reuse vs. seconds/minutes of generation
3. **Safe reruns:** Can re-materialize downstream assets without regenerating upstream
4. **Visibility:** `reused` metadata flag shows what was reused
5. **Explicit control:** `force` flag for deliberate regeneration

## Migration Notes

- **Backward compatible:** Existing workflows unchanged (default is skip)
- **No data migration:** Works with existing artifacts
- **Mode directives:** Curated modes (`evaluation-only`, etc.) still work but are less critical for avoiding redundant calls

## Future Work (Optional)

Per plan section 5 (Cohort Simplification):
- Review need for `mode:` directives in `selected_essays.txt`
- Most mode usage was to avoid re-running evaluations
- With automatic skip, modes could potentially be simplified
- Tagged with NOTE in `group_cohorts.py` for future consideration

## Files Modified

- `src/daydreaming_dagster/data_layer/gens_data_layer.py`
- `src/daydreaming_dagster/resources/experiment_config.py`
- `src/daydreaming_dagster/unified/stage_raw.py`
- `src/daydreaming_dagster/unified/stage_parsed.py`
- `src/daydreaming_dagster/unified/tests/test_stage_raw.py`
- `src/daydreaming_dagster/unified/tests/test_stage_parsed.py`
- `src/daydreaming_dagster/assets/group_cohorts.py` (NOTE added)
- `docs/guides/operating_guide.md`
- `plans/skip_existing_artifacts_plan.md` (design decisions resolved)
