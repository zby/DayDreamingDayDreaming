"""Integration tests for concepts active column functionality."""

import pandas as pd
from pathlib import Path
import tempfile
from unittest.mock import Mock

from daydreaming_dagster.definitions import defs
from daydreaming_dagster.resources.io_managers import CSVIOManager
from daydreaming_dagster.resources.data_paths_config import DataPathsConfig


class TestConceptsActiveColumnIntegration:
    """Essential tests for active column functionality."""
    
    def test_concepts_metadata_csv_with_active_column_exists(self):
        """Test that concepts_metadata.csv contains the active column."""
        csv_path = Path("data/1_raw/concepts/concepts_metadata.csv")
        
        # Must fail if required data file is missing
        assert csv_path.exists(), f"Required concepts metadata CSV not found: {csv_path}"
        
        df = pd.read_csv(csv_path)
        assert "active" in df.columns, "concepts_metadata.csv should contain 'active' column"
        
        # Validate we have both active and inactive concepts
        active_concepts = df[df["active"] == True]
        inactive_concepts = df[df["active"] == False]
        
        assert len(active_concepts) > 0, "Should have at least one active concept"
        assert len(inactive_concepts) > 0, "Should have at least one inactive concept"
    
    def test_enhanced_csv_io_manager_loads_active_concepts_only(self):
        """Test that enhanced CSV I/O manager filters to active concepts only."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_csv = Path(temp_dir) / "test_concepts.csv"
            
            # Test data with mixed active/inactive concepts
            test_data = pd.DataFrame([
                {"concept_id": "active-1", "name": "Active 1", "active": True},
                {"concept_id": "active-2", "name": "Active 2", "active": True},
                {"concept_id": "inactive-1", "name": "Inactive 1", "active": False},
            ])
            test_data.to_csv(temp_csv, index=False)
            
            # Create I/O manager with active filtering
            io_manager = CSVIOManager(
                base_path=Path(temp_dir),
                source_mappings={
                    "concepts_metadata": {
                        "source_file": str(temp_csv),
                        "filters": [{"column": "active", "value": True}]
                    }
                }
            )
            
            # Test filtering
            mock_context = Mock()
            mock_context.asset_key.path = ["concepts_metadata"]
            
            result = io_manager.load_input(mock_context)
            
            assert len(result) == 2, "Should return only active concepts"
            assert all(result["active"] == True), "All returned concepts should be active"
            
            returned_ids = set(result["concept_id"].tolist())
            expected_ids = {"active-1", "active-2"}
            assert returned_ids == expected_ids, "Should return correct active concepts"

    def test_dagster_definitions_have_active_filtering_configured(self):
        """Test that Dagster definitions properly configure active filtering."""
        enhanced_io_manager = defs.resources["enhanced_csv_io_manager"]
        
        assert isinstance(enhanced_io_manager, CSVIOManager)
        
        # Check source mappings configuration
        source_mappings = enhanced_io_manager.source_mappings
        concepts_mapping = source_mappings["concepts_metadata"]
        
        assert "filters" in concepts_mapping, "Should have filters configured"
        
        filters = concepts_mapping["filters"]
        active_filter = next((f for f in filters if f.get("column") == "active"), None)
        
        assert active_filter is not None, "Should have active column filter"
        assert active_filter.get("value") == True, "Should filter for active=True"


class TestBackwardCompatibility:
    """Test that existing functionality still works."""
    
    def test_existing_model_filtering_still_works(self):
        """Test that existing generation/evaluation model filtering works."""
        models_csv_path = Path("data/1_raw/llm_models.csv")
        assert models_csv_path.exists(), f"Required models CSV not found: {models_csv_path}"
        
        models_df = pd.read_csv(models_csv_path)
        
        # Verify we have models with the expected columns
        generation_models = models_df[models_df["for_generation"] == True]
        evaluation_models = models_df[models_df["for_evaluation"] == True]
        
        assert len(generation_models) > 0, "Should have generation models"
        assert len(evaluation_models) > 0, "Should have evaluation models"
        
        # Verify definitions still configure model filtering correctly
        enhanced_io_manager = defs.resources["enhanced_csv_io_manager"]
        source_mappings = enhanced_io_manager.source_mappings
        
        gen_filters = source_mappings["generation_models"]["filters"]
        eval_filters = source_mappings["evaluation_models"]["filters"]
        
        gen_filter = next(f for f in gen_filters if f["column"] == "for_generation")
        eval_filter = next(f for f in eval_filters if f["column"] == "for_evaluation")
        
        assert gen_filter["value"] == True
        assert eval_filter["value"] == True