## Experiment Management Implementation Plan

### Goals
- Define experiments by active elements: LLM models, concepts, templates (generation and evaluation), plus `experiment_id` and description
- Ensure reproducible, isolated experiments with stable identifiers
- Enable cross-experiment analysis and artifact tracking
- Maintain minimal code changes and leverage existing file-based architecture

### Core Architecture
- **Experiment ID**: Prefer Dagster partitions (partition key) for experiments; fall back to run tag `experiment_id` (format: `exp_YYYY-MM-DD_<suffix>`)
- **Stable combo IDs**: Deterministic, versioned, collision-aware identifiers for concept combinations
- **Active selections**: CSV files per experiment defining active components  
- **Processing layout**: Stage directories remain at root; each stage writes under a per-experiment subdirectory to avoid collisions
- **Cross-experiment analysis**: Stable combo IDs enable concatenation of results across experiments

### File Structure
```
data/
├── 1_raw/                             # Global raw data (unchanged)
│   ├── concepts/concepts_metadata.csv
│   └── llm_models.csv
├── combo_mappings.csv                 # Global stable combo ID mappings (append-only)
├── experiments/
│   ├── experiments.csv                # Registry of all experiments
│   └── {experiment_id}/
│       ├── active_generation_models.csv
│       ├── active_evaluation_models.csv
│       ├── active_generation_templates.csv
│       ├── active_evaluation_templates.csv
│       ├── active_concepts.csv
│       └── filtered_combinations.csv  # Combo IDs used in this experiment
├── 2_tasks/
│   └── {experiment_id}/               # Task artifacts for this experiment
├── 3_generation/
│   └── {experiment_id}/               # Generations for this experiment
├── 4_evaluation/
│   └── {experiment_id}/               # Evaluations for this experiment
└── 5_parsing/
    └── {experiment_id}/               # Parsed/aggregated outputs per experiment
```

**Key Design Principles:**
- **Global combo IDs**: Enable cross-experiment joins and analysis
- **Root-level processing**: All pipeline stages remain at `data/{stage}/` for consistency  
- **Experiment metadata**: Outputs include `experiment_id` column for filtering/grouping
- **Cross-experiment analysis**: Concatenate parsed results tables with stable combo IDs

### Stable Combo ID System (Required Implementation)
- **Deterministic, versioned combo IDs**: Compute from sorted `concept_id`s + `description_level` + `k_max`
  - Format: `combo_v1_<12-hex>` (e.g., `combo_v1_1f3a9c2d7b2c`)
  - Algorithm: SHA-256 of a stable string: `"|".join(sorted_concepts) + "|" + description_level + "|" + str(k_max)`; take first 12 hex chars
  - Include `version` column in the mapping for forward-compatibility (start with `v1`)
  - On collision (same `combo_id` with different parameters), automatically increase hex length to 16 and/or raise with actionable message
- **Global mapping table**: `data/combo_mappings.csv` (append-only)
  - Columns: `combo_id, version, concept_id, description_level, k_max, created_at`
  - One row per concept per combo (normalized format)
  - Never deleted, accumulates across all experiments
- **Per-experiment filtering**: `data/experiments/{experiment_id}/filtered_combinations.csv`
  - Contains only `combo_id`s used by that experiment
  - Downstream assets join with global mapping table for concept details
- **Optional convenience index**: `data/combo_index.csv` with one row per `combo_id` (aggregated concept list) to simplify joins
- **Benefits**: Stable cross-experiment references, greatly reduced collision risk, reproducible analysis

### Cross-Experiment Analysis Pattern
With stable combo IDs, you can easily combine results across experiments:

```python
# Load parsed scores from multiple experiments
exp_a_scores = pd.read_csv("data/5_parsing/evaluation_scores_exp_2025-08-08_a.csv")
exp_b_scores = pd.read_csv("data/5_parsing/evaluation_scores_exp_2025-08-08_b.csv")

# Concatenate for cross-experiment analysis
all_scores = pd.concat([exp_a_scores, exp_b_scores])

# Join with combo mappings for concept details
combo_mappings = pd.read_csv("data/combo_mappings.csv")
analysis_df = all_scores.merge(combo_mappings, on="combo_id")

# Analyze by experiment, model, combo, etc.
results = analysis_df.groupby(["experiment_id", "combo_id"]).agg({
    "score": ["mean", "std", "count"]
})
```

### Dagster Integration
- **Experiment ID retrieval**: `exp = context.partition_key or context.run.tags.get("experiment_id", "exp_default")`
- **Output paths**: Write under per-experiment subdirectories (e.g., `data/3_generation/{exp}/{combo_id}_{template}_{model}.txt`) to avoid collisions
- **Output metadata**: Include `experiment_id` in all MaterializeResult metadata
- **Filtering logic**: Load active selections from `data/experiments/{exp}/active_*.csv`
- **Combo filtering**: Generate `data/experiments/{exp}/filtered_combinations.csv` for the run
- **Partitions**: Define an `experiment_partitions_def` from the registered experiment IDs for partitioned materializations

### Optional schemas (only if needed)
- If you need SQL-like joins across experiments, add lightweight mapping tables:
  - `data/experiments/generations_map.csv` – `experiment_id, generation_id, path`
  - `data/experiments/evaluations_map.csv` – `experiment_id, evaluation_id, path`
- Otherwise, rely on the folder structure + metadata for simplicity.

### Minimal code changes
- Update assets that:
  - build or iterate combos → read experiment-scoped `filtered_combinations.csv` when present, else fall back to global combos
  - write files → include `{experiment_id}` in paths
  - return `MaterializeResult` → include `experiment_id` in metadata
- Provide a small util: `get_experiment_id(context) -> str` and `get_experiment_dirs(exp)`

Example (pattern only)
```python
from pathlib import Path
from dagster import MaterializeResult, MetadataValue

def get_experiment_id(context):
    return getattr(context, "partition_key", None) or context.run.tags.get("experiment_id", "exp_default")

def experiment_dir(exp: str) -> Path:
    return Path("data/experiments") / exp

# Inside an asset
exp = get_experiment_id(context)
out_dir = Path("data/3_generation") / exp
out_dir.mkdir(parents=True, exist_ok=True)
path = out_dir / f"{combo_id}_{template}_{model}.txt"
path.write_text(generation)
yield MaterializeResult(metadata={
    "experiment_id": exp,
    "output_path": MetadataValue.path(str(path))
})
```

## Implementation Steps

### Step 1: Create Combo ID System (Pragmatic TDD)

#### 1.1 Write Failing Tests First
```python
# daydreaming_dagster/assets/test_utilities_unit.py  # colocated unit tests for pure logic
import pytest
from daydreaming_dagster.utils.combo_ids import generate_combo_id, ComboIDManager

class TestComboIDGeneration:
    def test_generate_combo_id_deterministic(self):
        """Same inputs always produce same combo ID"""
        concepts = ["concept_001", "concept_002", "concept_003"]
        id1 = generate_combo_id(concepts, "brief", 3)
        id2 = generate_combo_id(concepts, "brief", 3)
        assert id1 == id2
        assert id1.startswith("combo_v1_")
        assert len(id1) == len("combo_v1_") + 12
    
    def test_generate_combo_id_order_independent(self):
        """Order of concept IDs doesn't affect combo ID"""
        concepts1 = ["concept_001", "concept_002", "concept_003"]
        concepts2 = ["concept_003", "concept_001", "concept_002"]
        id1 = generate_combo_id(concepts1, "brief", 3)
        id2 = generate_combo_id(concepts2, "brief", 3)
        assert id1 == id2
    
    def test_generate_combo_id_different_params_different_ids(self):
        """Different parameters produce different combo IDs"""
        concepts = ["concept_001", "concept_002"]
        id1 = generate_combo_id(concepts, "brief", 2)
        id2 = generate_combo_id(concepts, "detailed", 2)
        id3 = generate_combo_id(concepts, "brief", 3)
        assert id1 != id2
        assert id1 != id3
        assert id2 != id3
    
    def test_combo_id_manager_append_only(self):
        """ComboIDManager only adds new entries, never overwrites"""
        manager = ComboIDManager("test_combo_mappings.csv")
        
        # First addition
        combo_id1 = manager.get_or_create_combo_id(
            ["concept_001", "concept_002"], "brief", 2
        )
        
        # Same combo should return same ID without adding duplicate
        combo_id2 = manager.get_or_create_combo_id(
            ["concept_001", "concept_002"], "brief", 2
        )
        
        assert combo_id1 == combo_id2
        # Should only have one entry for this combo
        mappings = manager.load_mappings()
        matching_entries = mappings[mappings["combo_id"] == combo_id1]
        assert len(matching_entries) == 2  # One row per concept
    
    def test_combo_mappings_csv_format(self):
        """Global mappings CSV has correct format"""
        manager = ComboIDManager("test_combo_mappings.csv")
        combo_id = manager.get_or_create_combo_id(
            ["concept_001", "concept_002"], "brief", 2
        )
        
        mappings = manager.load_mappings()
        required_columns = {"combo_id", "concept_id", "description_level", "k_max", "created_at"}
        assert set(mappings.columns) == required_columns
        
        combo_rows = mappings[mappings["combo_id"] == combo_id]
        assert len(combo_rows) == 2
        assert set(combo_rows["concept_id"]) == {"concept_001", "concept_002"}
        assert combo_rows["description_level"].iloc[0] == "brief"
        assert combo_rows["k_max"].iloc[0] == 2

class TestComboIDStability:
    def test_combo_ids_stable_across_concept_universe_changes(self):
        """Adding new concepts doesn't change existing combo IDs"""
        # Create initial state
        manager = ComboIDManager("test_combo_mappings.csv")
        original_id = manager.get_or_create_combo_id(
            ["concept_001", "concept_002"], "brief", 2
        )
        
        # Simulate adding new concepts to the universe
        new_id_same_combo = manager.get_or_create_combo_id(
            ["concept_001", "concept_002"], "brief", 2
        )
        
        # Add completely different combo
        manager.get_or_create_combo_id(
            ["concept_003", "concept_004", "concept_005"], "detailed", 3
        )
        
        # Original combo ID should remain unchanged
        final_id = manager.get_or_create_combo_id(
            ["concept_001", "concept_002"], "brief", 2
        )
        
        assert original_id == new_id_same_combo == final_id
```

#### 1.2 Implement Combo ID System
After writing failing tests, implement:

```python
# daydreaming_dagster/utils/combo_ids.py
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
        # Implementation here
        pass
    
    def load_mappings(self) -> pd.DataFrame:
        # Implementation here
        pass
```

#### 1.3 Create Global Mapping Asset
- Asset: `combo_mappings_csv`
- Output: `data/combo_mappings.csv`
- Logic: Generate all combos, assign stable IDs, append new entries only

### Step 2: Experiment Infrastructure (Pragmatic TDD)

#### 2.1 Write Infrastructure Tests First
```python
# tests/test_experiment_infrastructure.py
import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock
from daydreaming_dagster.utils.combo_ids import (
    get_experiment_id, 
    experiment_dir,
    ExperimentManager,
    load_active_selection
)

class TestExperimentUtilities:
    def test_get_experiment_id_from_context(self):
        """Extract experiment_id from Dagster context tags"""
        mock_context = Mock()
        mock_context.run.tags = {"experiment_id": "exp_2025-08-08_a"}
        assert get_experiment_id(mock_context) == "exp_2025-08-08_a"
    
    def test_get_experiment_id_default_fallback(self):
        """Fall back to default when no experiment_id tag"""
        mock_context = Mock()
        mock_context.run.tags = {}
        assert get_experiment_id(mock_context) == "exp_default"
    
    def test_experiment_dir_path_construction(self):
        """Construct correct experiment directory path"""
        path = experiment_dir("exp_2025-08-08_a")
        expected = Path("data/experiments/exp_2025-08-08_a")
        assert path == expected

class TestActiveSelectionLoading:
    def test_load_active_selection_existing_file(self):
        """Load active selection from existing CSV file"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create test CSV with active selections
            selection_file = Path(tmp_dir) / "active_concepts.csv"
            selection_file.write_text("concept_001\nconcept_002\nconcept_003\n")
            
            # Mock experiment_dir to return our temp directory
            def mock_exp_dir(exp_id):
                return Path(tmp_dir)
            
            # Should load the three concepts
            selections = load_active_selection("test_exp", "concepts", mock_exp_dir)
            assert selections == ["concept_001", "concept_002", "concept_003"]
    
    def test_load_active_selection_missing_file_fallback(self):
        """Fall back to empty list when selection file missing"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            def mock_exp_dir(exp_id):
                return Path(tmp_dir)
            
            # File doesn't exist, should return empty list
            selections = load_active_selection("test_exp", "concepts", mock_exp_dir)
            assert selections == []
    
    def test_load_active_selection_empty_file(self):
        """Handle empty selection files gracefully"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            selection_file = Path(tmp_dir) / "active_models.csv"
            selection_file.write_text("")
            
            def mock_exp_dir(exp_id):
                return Path(tmp_dir)
            
            selections = load_active_selection("test_exp", "models", mock_exp_dir)
            assert selections == []

class TestExperimentManager:
    def test_experiment_registration(self):
        """Register new experiment with proper metadata"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry_path = Path(tmp_dir) / "experiments.csv"
            manager = ExperimentManager(registry_path)
            
            manager.register_experiment(
                "exp_2025-08-08_a", 
                "Test Experiment",
                "Testing stable combo IDs",
                "researcher"
            )
            
            # Should create registry with proper format
            assert registry_path.exists()
            
            # Should contain the registered experiment
            experiments = manager.load_experiments()
            assert len(experiments) == 1
            assert experiments.iloc[0]["experiment_id"] == "exp_2025-08-08_a"
            assert experiments.iloc[0]["name"] == "Test Experiment"
    
    def test_experiment_id_uniqueness_validation(self):
        """Prevent duplicate experiment IDs"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            registry_path = Path(tmp_dir) / "experiments.csv"
            manager = ExperimentManager(registry_path)
            
            # First registration should succeed
            manager.register_experiment("exp_test", "Test 1", "Description", "user")
            
            # Duplicate should raise error
            with pytest.raises(ValueError, match="Experiment ID.*already exists"):
                manager.register_experiment("exp_test", "Test 2", "Description", "user")
    
    def test_create_experiment_directory_structure(self):
        """Create proper directory structure for new experiment"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            manager = ExperimentManager()
            exp_id = "exp_2025-08-08_a"
            
            # Override base path for testing
            manager.experiments_base = Path(tmp_dir) / "experiments"
            
            manager.create_experiment_structure(exp_id)
            
            exp_dir = manager.experiments_base / exp_id
            assert exp_dir.exists()
            
            # Should create placeholder files for active selections
            expected_files = [
                "active_generation_models.csv",
                "active_evaluation_models.csv", 
                "active_generation_templates.csv",
                "active_evaluation_templates.csv",
                "active_concepts.csv"
            ]
            
            for filename in expected_files:
                file_path = exp_dir / filename
                assert file_path.exists()
```

#### 2.2 Implement Experiment Infrastructure
```python
# daydreaming_dagster/utils/combo_ids.py (continued)
def get_experiment_id(context) -> str:
    return context.run.tags.get("experiment_id", "exp_default")

def experiment_dir(exp_id: str) -> Path:
    return Path("data/experiments") / exp_id

def load_active_selection(exp_id: str, selection_type: str, exp_dir_func=experiment_dir) -> List[str]:
    """Load active selection CSV, falling back to empty list if missing"""
    path = exp_dir_func(exp_id) / f"active_{selection_type}.csv"
    if path.exists():
        df = pd.read_csv(path, header=None)
        return df[0].dropna().tolist() if not df.empty else []
    return []

class ExperimentManager:
    def __init__(self, registry_path: str = "data/experiments/experiments.csv"):
        self.registry_path = Path(registry_path)
        self.experiments_base = Path("data/experiments")
    
    def register_experiment(self, exp_id: str, name: str, description: str, author: str):
        # Implementation here
        pass
    
    def create_experiment_structure(self, exp_id: str):
        # Implementation here  
        pass
```

### Step 3: Asset Filtering System (TDD Approach)

#### 3.1 Write Filtering Tests First
```python
# tests/test_asset_filtering.py
import pytest
import tempfile
import pandas as pd
from pathlib import Path
from unittest.mock import Mock
from daydreaming_dagster.assets.utilities import FilteredCombinationsAsset

class TestFilteredCombinationsAsset:
    def test_filtered_combinations_respects_active_selections(self):
        """Filter combinations based on active concept/template/model selections"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Setup global combo mappings
            combo_mappings = pd.DataFrame({
                "combo_id": ["combo_abc123", "combo_def456", "combo_ghi789"],
                "concept_id": ["concept_001", "concept_002", "concept_003"], 
                "description_level": ["brief", "brief", "detailed"],
                "k_max": [2, 2, 3]
            })
            mappings_path = Path(tmp_dir) / "combo_mappings.csv"
            combo_mappings.to_csv(mappings_path, index=False)
            
            # Setup experiment active selections
            exp_dir = Path(tmp_dir) / "experiments" / "exp_test"
            exp_dir.mkdir(parents=True)
            
            # Only activate concept_001 and concept_002
            active_concepts = exp_dir / "active_concepts.csv"
            active_concepts.write_text("concept_001\nconcept_002\n")
            
            # Create asset and test filtering
            asset = FilteredCombinationsAsset(
                combo_mappings_path=str(mappings_path),
                experiments_base_path=str(Path(tmp_dir) / "experiments")
            )
            
            mock_context = Mock()
            mock_context.partition_key = "exp_test"
            
            filtered_combos = asset.materialize(mock_context)
            
            # Should only include combos with active concepts
            expected_combo_ids = {"combo_abc123", "combo_def456"}  # concept_001, concept_002
            actual_combo_ids = set(filtered_combos["combo_id"])
            assert actual_combo_ids == expected_combo_ids
    
    def test_filtered_combinations_fallback_to_all_when_no_selections(self):
        """Fall back to all combinations when no active selections exist"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            combo_mappings = pd.DataFrame({
                "combo_id": ["combo_abc123", "combo_def456"],
                "concept_id": ["concept_001", "concept_002"],
                "description_level": ["brief", "brief"], 
                "k_max": [2, 2]
            })
            mappings_path = Path(tmp_dir) / "combo_mappings.csv"
            combo_mappings.to_csv(mappings_path, index=False)
            
            # No experiment directory = no active selections
            asset = FilteredCombinationsAsset(
                combo_mappings_path=str(mappings_path),
                experiments_base_path=str(Path(tmp_dir) / "experiments")
            )
            
            mock_context = Mock()
            mock_context.partition_key = "exp_nonexistent"
            
            filtered_combos = asset.materialize(mock_context)
            
            # Should include all combos when no filtering
            assert len(filtered_combos) == 2
            assert set(filtered_combos["combo_id"]) == {"combo_abc123", "combo_def456"}
    
    def test_filtered_combinations_cross_product_filtering(self):
        """Apply filtering across multiple dimensions (concepts + templates + models)"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Setup test data with multiple models and templates
            combo_mappings = pd.DataFrame({
                "combo_id": ["combo_001", "combo_002", "combo_003"],
                "concept_id": ["concept_001", "concept_002", "concept_003"],
                "description_level": ["brief", "brief", "detailed"],
                "k_max": [2, 2, 3]
            })
            mappings_path = Path(tmp_dir) / "combo_mappings.csv"
            combo_mappings.to_csv(mappings_path, index=False)
            
            # Setup active selections for multiple types
            exp_dir = Path(tmp_dir) / "experiments" / "exp_test"
            exp_dir.mkdir(parents=True)
            
            (exp_dir / "active_concepts.csv").write_text("concept_001\nconcept_002\n")
            (exp_dir / "active_generation_models.csv").write_text("claude_3_sonnet\n")
            (exp_dir / "active_generation_templates.csv").write_text("template_brief\n")
            
            asset = FilteredCombinationsAsset(
                combo_mappings_path=str(mappings_path),
                experiments_base_path=str(Path(tmp_dir) / "experiments")
            )
            
            mock_context = Mock()
            mock_context.partition_key = "exp_test"
            
            filtered_combos = asset.materialize(mock_context)
            
            # Should exclude combo_003 (concept_003 not active, detailed not matching brief template)
            assert "combo_003" not in filtered_combos["combo_id"].values
            assert len(filtered_combos) == 2

class TestCrossExperimentAnalysis:
    def test_stable_combo_ids_enable_cross_experiment_joins(self):
        """Verify that stable combo IDs work across different experiments"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create sample results from two experiments with same combo IDs
            exp_a_results = pd.DataFrame({
                "combo_id": ["combo_abc123", "combo_def456"],
                "score": [7.5, 8.2],
                "experiment_id": ["exp_2025-08-08_a", "exp_2025-08-08_a"]
            })
            
            exp_b_results = pd.DataFrame({
                "combo_id": ["combo_abc123", "combo_ghi789"], 
                "score": [7.8, 6.9],
                "experiment_id": ["exp_2025-08-08_b", "exp_2025-08-08_b"]
            })
            
            # Concatenate results (this is the key use case)
            combined_results = pd.concat([exp_a_results, exp_b_results])
            
            # Should be able to group by combo_id across experiments
            combo_stats = combined_results.groupby("combo_id")["score"].agg(["mean", "std", "count"])
            
            # combo_abc123 appears in both experiments
            assert combo_stats.loc["combo_abc123", "count"] == 2
            assert combo_stats.loc["combo_abc123", "mean"] == (7.5 + 7.8) / 2
            
            # Other combos appear in only one experiment
            assert combo_stats.loc["combo_def456", "count"] == 1
            assert combo_stats.loc["combo_ghi789", "count"] == 1
```

#### 3.2 Implement Asset Filtering
```python  
# daydreaming_dagster/assets/content_combinations.py
from dagster import asset, StaticPartitionsDefinition
import pandas as pd
from .utilities import get_experiment_id, load_active_selection

class FilteredCombinationsAsset:
    def __init__(self, combo_mappings_path: str, experiments_base_path: str):
        self.combo_mappings_path = combo_mappings_path
        self.experiments_base_path = experiments_base_path
    
    def materialize(self, context) -> pd.DataFrame:
        # Implementation here
        pass

def experiment_partitions_def():
    # Placeholder; wire to experiments registry in implementation
    return StaticPartitionsDefinition([])

@asset(partitions_def=experiment_partitions_def())
def filtered_combinations_csv(context, combo_mappings_csv) -> pd.DataFrame:
    """Generate filtered combinations for specific experiment"""
    exp_id = get_experiment_id(context)
    
    # Load active selections
    active_concepts = load_active_selection(exp_id, "concepts")
    active_gen_models = load_active_selection(exp_id, "generation_models") 
    active_gen_templates = load_active_selection(exp_id, "generation_templates")
    
    # Apply filtering to combo mappings
    filtered_df = combo_mappings_csv
    
    if active_concepts:
        filtered_df = filtered_df[filtered_df["concept_id"].isin(active_concepts)]
    
    # Additional filtering logic here...
    
    return filtered_df
```

### Step 4: Update Generation/Evaluation Assets
1. **Update output metadata**
   - Write under per-experiment subdirs (e.g., `data/3_generation/{experiment_id}/`, `data/4_evaluation/{experiment_id}/`)
   - Include `experiment_id` in MaterializeResult metadata and CSV columns

2. **Update data loading**
   - Read from `filtered_combinations.csv` when in experiment mode
   - Fall back to global combinations for backward compatibility

### Step 5: Experiment Workflow
1. **Setup new experiment**
   ```bash
   # Create experiment directory structure
   mkdir -p data/experiments/exp_2025-08-08_a
   
   # Create active selection files (one ID per line, no header)
   echo "anthropic_claude_3_5_sonnet" > data/experiments/exp_2025-08-08_a/active_generation_models.csv
   echo "concept_001" > data/experiments/exp_2025-08-08_a/active_concepts.csv
   # ... etc for other selections
   
   # Register experiment
   echo "exp_2025-08-08_a,Test Run,Initial experiment setup,2025-08-08T10:00:00,researcher" >> data/experiments/experiments.csv
   ```

2. **Launch experiment**
   ```bash
   uv run dagster asset materialize --tag experiment_id=exp_2025-08-08_a
   ```

3. **Analyze results**
   - Check `data/5_parsing/` for parsed results with experiment_id columns
   - Cross-experiment analysis by concatenating CSV files with matching combo_ids
   - Individual experiment filtering using experiment_id column

## Acceptance Criteria

### Phase 1: Stable Combo ID System
- [ ] **Targeted tests first for pure logic**: colocated unit tests for `generate_combo_id`
- [ ] Stable combo IDs implemented with SHA-256 hash algorithm  
- [ ] ComboIDManager class with append-only behavior
- [ ] Global combo mappings table created at `data/combo_mappings.csv`
- [ ] Combo ID determinism verified across multiple runs
- [ ] Combo ID stability verified across concept universe changes
- [ ] Collision safety documented (lengthen hash or raise)

### Phase 2: Experiment Infrastructure  
- [ ] **Tests where valuable**: registration, directory creation, selection loading
- [ ] Experiment utilities (get_experiment_id, experiment_dir) implemented
- [ ] ExperimentManager with registration and validation
- [ ] Experiment directory structure creation automated
- [ ] Active selection loading with fallback behavior
- [ ] Experiment ID uniqueness validation
- [ ] Partition definition derived from registry

### Phase 3: Asset Filtering System
- [ ] **Tests written first**: `test_asset_filtering.py` with failing tests  
- [ ] FilteredCombinationsAsset respects active selections
- [ ] Cross-product filtering across concepts/templates/models
- [ ] Graceful fallback when no active selections exist
- [ ] Cross-experiment analysis functionality verified

### Phase 4: Integration & Validation
- [ ] All generation/evaluation assets include experiment_id in metadata
- [ ] CSV outputs include experiment_id column for filtering
- [ ] Cross-experiment concatenation and analysis tested end-to-end
- [ ] Backward compatibility maintained for non-experiment runs
- [ ] Performance benchmarks for large combo sets

## TDD Testing Strategy (Pragmatic)

### Test File Organization
```
daydreaming_dagster/assets/
├── utilities.py
└── test_utilities_unit.py          # colocated unit tests for pure functions only

tests/
├── test_combo_id_system_integration.py   # file IO and manager behavior (temp dirs)
├── test_experiment_infrastructure.py     # registry + dirs + selections
├── test_asset_filtering.py               # filtering behavior
├── test_cross_experiment_analysis.py     # integration tests
└── test_experiment_workflow.py           # end-to-end workflow tests
```

### Testing Commands for Each Phase
```bash
# Phase 1: Run colocated unit tests (fast)
uv run pytest daydreaming_dagster/assets/test_utilities_unit.py -v

# Phase 1b: Manager integration tests (file IO)
uv run pytest tests/test_combo_id_system_integration.py -v

# Phase 3: Run filtering tests (should fail initially)
uv run pytest tests/test_asset_filtering.py -v

# Integration: Run all experiment tests
uv run pytest tests/test_*experiment*.py -v

# Full test suite
uv run pytest tests/ -k "experiment or combo" -v
```

### Key Testing Principles
1. **Write failing tests first** - Every feature starts with a failing test
2. **Test behavior, not implementation** - Focus on expected outcomes
3. **Use temporary directories** - Isolate tests from real data files
4. **Mock external dependencies** - No real API calls or file system dependencies
5. **Test edge cases** - Empty files, missing directories, duplicate IDs
6. **Verify cross-experiment functionality** - Key requirement for analysis

## Implementation Priority
1. **Phase 1**: Write combo ID tests → Implement stable combo ID system
2. **Phase 2**: Write infrastructure tests → Implement experiment utilities
3. **Phase 3**: Write filtering tests → Implement asset filtering system
4. **Phase 4**: Integration testing and end-to-end validation

## Benefits
- **Test-Driven Quality**: All features developed with failing tests first
- **Reproducibility**: Stable combo IDs enable reliable cross-experiment analysis
- **Isolation**: Clear separation prevents experiment interference  
- **Scalability**: Append-only global mapping supports growing concept universe
- **Compatibility**: Graceful fallbacks preserve existing functionality
- **Transparency**: File-based CSV approach aligns with project philosophy


