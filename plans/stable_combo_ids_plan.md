# Stable Combo IDs Implementation Plan

## Goal
Replace sequential combo IDs with stable hash-based identifiers to enable reliable cross-experiment analysis with minimal system changes.

## Problem
Current combo IDs (`combo_1`, `combo_2`, etc.) change when the concept universe changes, making it impossible to compare results across different experiment runs that use different concept sets.

## Solution: Stable Hash-Based Combo IDs

### New Combo ID Format
- **Current**: `combo_1`, `combo_2`, `combo_3...` (sequential, unstable)
- **New**: `combo_v1_1f3a9c2d7b2c` (hash-based, versioned, stable)

### Algorithm
```python
def generate_combo_id(concept_ids: List[str], description_level: str, k_max: int) -> str:
    """Generate deterministic combo ID from concept combination parameters"""
    sorted_concepts = sorted(concept_ids)
    hash_input = "|".join(sorted_concepts) + "|" + description_level + "|" + str(k_max)
    hash_obj = hashlib.sha256(hash_input.encode())
    return f"combo_v1_{hash_obj.hexdigest()[:12]}"
```

## File Structure Changes

### New Global Mapping Table
**File**: `data/combo_mappings.csv`
**Columns**: `combo_id, version, concept_id, description_level, k_max, created_at`
**Behavior**: Append-only (never delete or modify existing entries)

**Example**:
```csv
combo_id,version,concept_id,description_level,k_max,created_at
combo_v1_1f3a9c2d7b2c,v1,concept_001,brief,2,2025-08-08T10:00:00
combo_v1_1f3a9c2d7b2c,v1,concept_002,brief,2,2025-08-08T10:00:00
combo_v1_a7e8f9d1c3b5,v1,concept_001,detailed,3,2025-08-08T10:01:00
combo_v1_a7e8f9d1c3b5,v1,concept_003,detailed,3,2025-08-08T10:01:00
combo_v1_a7e8f9d1c3b5,v1,concept_005,detailed,3,2025-08-08T10:01:00
```

### Existing Files (Updated)
- `data/2_tasks/content_combinations_csv.csv` - Now uses stable combo IDs instead of sequential
- All downstream files continue to reference combos by ID (no other changes needed)

## Implementation Steps

### Step 1: Write Failing Tests (TDD)

Split tests by type to match our convention (colocate pure unit tests; keep file I/O tests under `tests/`).

1) Colocated pure unit tests (no external data): `daydreaming_dagster/assets/test_utilities_unit.py`
```python
import pytest
from daydreaming_dagster.utils.combo_ids import generate_combo_id

class TestComboIDGeneration:
    def test_combo_id_deterministic(self):
        concepts = ["concept_001", "concept_002"]
        id1 = generate_combo_id(concepts, "brief", 2)
        id2 = generate_combo_id(concepts, "brief", 2)
        assert id1 == id2
        assert id1.startswith("combo_v1_")
        assert len(id1) == len("combo_v1_") + 12

    def test_combo_id_order_independent(self):
        id1 = generate_combo_id(["concept_001", "concept_002"], "brief", 2)
        id2 = generate_combo_id(["concept_002", "concept_001"], "brief", 2)
        assert id1 == id2

    def test_combo_id_different_params_different_ids(self):
        concepts = ["concept_001", "concept_002"]
        id1 = generate_combo_id(concepts, "brief", 2)
        id2 = generate_combo_id(concepts, "detailed", 2)
        id3 = generate_combo_id(concepts, "brief", 3)
        assert len({id1, id2, id3}) == 3
```

2) File I/O manager tests: `tests/test_stable_combo_ids.py`
```python
import tempfile
import pandas as pd
from pathlib import Path
from daydreaming_dagster.utils.combo_ids import ComboIDManager

class TestComboIDManager:
    def test_append_only_behavior(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            mappings_path = Path(tmp_dir) / "combo_mappings.csv"
            manager = ComboIDManager(str(mappings_path))

            combo_id1 = manager.get_or_create_combo_id(["concept_001", "concept_002"], "brief", 2)
            combo_id2 = manager.get_or_create_combo_id(["concept_001", "concept_002"], "brief", 2)
            assert combo_id1 == combo_id2

            mappings = pd.read_csv(mappings_path)
            combo_rows = mappings[mappings["combo_id"] == combo_id1]
            assert len(combo_rows) == 2
            assert set(combo_rows["concept_id"]) == {"concept_001", "concept_002"}

    def test_cross_experiment_stability(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            mappings_path = Path(tmp_dir) / "combo_mappings.csv"
            id_exp1 = ComboIDManager(str(mappings_path)).get_or_create_combo_id(["c1", "c2"], "brief", 2)
            id_exp2 = ComboIDManager(str(mappings_path)).get_or_create_combo_id(["c1", "c2"], "brief", 2)
            assert id_exp1 == id_exp2
```

### Step 2: Implement Stable ID System

Create utility functions in `daydreaming_dagster/utils/combo_ids.py`:
```python
import hashlib
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List

def generate_combo_id(concept_ids: List[str], description_level: str, k_max: int) -> str:
    """Generate deterministic combo ID from concept combination parameters"""
    sorted_concepts = sorted(concept_ids)
    hash_input = "|".join(sorted_concepts) + "|" + description_level + "|" + str(k_max)
    hash_obj = hashlib.sha256(hash_input.encode())
    return f"combo_v1_{hash_obj.hexdigest()[:12]}"

class ComboIDManager:
    def __init__(self, mappings_path: str = "data/combo_mappings.csv"):
        self.mappings_path = Path(mappings_path)
    
    def get_or_create_combo_id(self, concept_ids: List[str], description_level: str, k_max: int) -> str:
        """Get existing combo ID or create new one with stable hash"""
        combo_id = generate_combo_id(concept_ids, description_level, k_max)
        
        # Check if this combo_id exists and whether it represents the same parameters
        if self.mappings_path.exists():
            existing_mappings = pd.read_csv(self.mappings_path)
            existing_combo = existing_mappings[existing_mappings["combo_id"] == combo_id]
            if not existing_combo.empty:
                # Optional: verify no collision (different params with same combo_id)
                sample = existing_combo.iloc[0]
                same_params = (
                    sample["description_level"] == description_level
                    and int(sample["k_max"]) == int(k_max)
                )
                if same_params:
                    return combo_id
                # If collision ever detected, either lengthen hex or raise
                raise ValueError(
                    f"combo_id collision detected for {combo_id}. Consider increasing hash length."
                )
        
        # Add new mapping entries (one row per concept)
        new_rows = []
        for concept_id in sorted(concept_ids):
            new_rows.append({
                "combo_id": combo_id,
                "version": "v1",
                "concept_id": concept_id,
                "description_level": description_level,
                "k_max": k_max,
                "created_at": datetime.now().isoformat()
            })
        
        new_df = pd.DataFrame(new_rows)
        
        # Append to existing file or create new
        if self.mappings_path.exists():
            existing_df = pd.read_csv(self.mappings_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df
            # Ensure directory exists
            self.mappings_path.parent.mkdir(parents=True, exist_ok=True)
        
        combined_df.to_csv(self.mappings_path, index=False)
        return combo_id
    
    def load_mappings(self) -> pd.DataFrame:
        """Load existing combo mappings"""
        if self.mappings_path.exists():
            return pd.read_csv(self.mappings_path)
        return pd.DataFrame(columns=["combo_id", "version", "concept_id", "description_level", "k_max", "created_at"])
```

### Step 3: Update Content Combinations Asset

Modify the existing `content_combinations` asset to generate stable IDs at source, and let `content_combinations_csv` continue to serialize `combo_id` and `concept_id` as-is.

```python
# In daydreaming_dagster/assets/core.py
from daydreaming_dagster.utils.combo_ids import ComboIDManager

@asset(
    group_name="llm_tasks",
    required_resource_keys={"experiment_config"}
)
def content_combinations(context, concepts: List[Concept]) -> List[ContentCombination]:
    experiment_config = context.resources.experiment_config
    k_max = experiment_config.k_max
    description_level = experiment_config.description_level

    manager = ComboIDManager()
    content_combos: List[ContentCombination] = []

    for combo in combinations(concepts, k_max):
        concept_ids = [c.concept_id for c in combo]
        stable_id = manager.get_or_create_combo_id(concept_ids, description_level, k_max)
        content_combo = ContentCombination.from_concepts(
            list(combo),
            description_level,
            combo_id=stable_id
        )
        content_combos.append(content_combo)

    # Optionally log the stable ID format
    context.add_output_metadata({
        "stable_id_format": MetadataValue.text("combo_v1_<12-hex>")
    })

    return content_combos
```

### Step 4: Create Global Mapping Asset

Add new asset to maintain the global mapping table:
```python
@asset
def combo_mappings_csv(context, content_combinations_csv) -> pd.DataFrame:
    """Maintain global append-only mapping of combo IDs to concepts"""
    
    manager = ComboIDManager()
    mappings_df = manager.load_mappings()
    
    # This asset just ensures the mapping file exists and is up-to-date
    # The actual mapping creation happens in content_combinations_csv
    
    context.add_output_metadata({
        "total_mappings": len(mappings_df),
        "unique_combos": mappings_df["combo_id"].nunique() if not mappings_df.empty else 0,
        "mapping_file": "data/combo_mappings.csv"
    })
    
    return mappings_df
```

## Cross-Experiment Analysis Pattern

With stable combo IDs, you can now reliably compare results across different runs:

```python
# Load results from different experiment runs
results_run1 = pd.read_csv("data/5_parsing/evaluation_scores_20250808.csv")
results_run2 = pd.read_csv("data/5_parsing/evaluation_scores_20250809.csv")

# Add run identifiers
results_run1["run_id"] = "run_20250808"
results_run2["run_id"] = "run_20250809"

# Concatenate results - stable combo IDs enable reliable joins
all_results = pd.concat([results_run1, results_run2])

# Analyze performance across runs for the same concept combinations
combo_analysis = all_results.groupby("combo_id").agg({
    "score": ["mean", "std", "count"],
    "run_id": lambda x: list(x.unique())  # Which runs included this combo
})

# Find combos that appeared in both runs
both_runs = combo_analysis[combo_analysis[("score", "count")] >= 2]
print(f"Found {len(both_runs)} combos tested in multiple runs")
```

## Benefits

1. **Cross-Experiment Analysis**: Stable IDs enable reliable comparison across runs
2. **Minimal Risk**: Only changes combo ID generation, rest of pipeline unchanged
3. **Backward Compatible**: Can run alongside existing sequential IDs during transition
4. **Foundation**: Creates basis for future experiment management features
5. **Append-Only**: Never lose historical combo mappings

## Acceptance Criteria

- [ ] Tests written and initially failing
- [ ] `generate_combo_id()` function produces deterministic, order-independent IDs
- [ ] `ComboIDManager` implements append-only behavior with collision check
- [ ] Global mapping table created at `data/combo_mappings.csv` with `version` column
- [ ] `content_combinations_csv` asset updated to use stable IDs
- [ ] Cross-experiment analysis pattern verified with sample data
- [ ] All existing downstream assets continue to work unchanged
- [ ] Documentation updated with new ID format and usage patterns

## Testing Commands

```bash
# Run colocated unit tests (pure logic)
uv run pytest daydreaming_dagster/assets/test_utilities_unit.py -v

# Run manager tests (file I/O)
uv run pytest tests/test_stable_combo_ids.py -v

# Run all tests to ensure no regressions
uv run pytest tests/ -v

# Test specific combo ID functionality
uv run pytest tests/test_stable_combo_ids.py::TestComboIDGeneration::test_combo_id_deterministic -v
```