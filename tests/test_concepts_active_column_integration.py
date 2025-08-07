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
    
    def test_concepts_asset_filters_for_active_concepts_only(self):
        """Test that concepts asset filters for active concepts only."""
        # This test now verifies that filtering happens in the concepts asset itself
        # rather than in the I/O manager, which aligns with our new architecture
        
        # Test data with mixed active/inactive concepts
        test_metadata = pd.DataFrame([
            {"concept_id": "active-1", "name": "Active 1", "active": True},
            {"concept_id": "active-2", "name": "Active 2", "active": True},
            {"concept_id": "inactive-1", "name": "Inactive 1", "active": False},
        ])
        
        # Import the actual concepts asset function and test its filtering logic
        from daydreaming_dagster.assets.raw_data import concepts
        from daydreaming_dagster.resources.experiment_config import ExperimentConfig
        from dagster import build_asset_context
        
        # Create a proper Dagster context for testing
        config = ExperimentConfig(k_max=2, description_level="paragraph")
        
        # Create temporary directory structure for description files
        with tempfile.TemporaryDirectory() as temp_dir:
            desc_dir = Path(temp_dir) / "descriptions-paragraph"
            desc_dir.mkdir(parents=True)
            desc_dir.joinpath("active-1.txt").write_text("Active 1 description")
            desc_dir.joinpath("active-2.txt").write_text("Active 2 description")
            desc_dir.joinpath("inactive-1.txt").write_text("Inactive 1 description")
            
            # Patch the data path to point to our test directory
            from unittest.mock import patch
            with patch('daydreaming_dagster.assets.raw_data.Path') as mock_path:
                def path_side_effect(arg):
                    if arg == "data/1_raw/concepts/descriptions-paragraph":
                        return desc_dir
                    return Path(arg)
                mock_path.side_effect = path_side_effect
                
                # Create proper asset context
                context = build_asset_context()
                
                # Call the concepts asset function directly
                result_concepts = concepts(context, test_metadata, config)
                
                # Verify only active concepts are returned
                assert len(result_concepts) == 2, "Should return only active concepts"
                concept_ids = {concept.concept_id for concept in result_concepts}
                expected_ids = {"active-1", "active-2"}
                assert concept_ids == expected_ids, "Should return correct active concepts"

    def test_dagster_definitions_use_simplified_architecture(self):
        """Test that Dagster definitions use the new simplified architecture."""
        # Verify we have the simplified CSV I/O manager instead of enhanced one
        csv_io_manager = defs.resources["csv_io_manager"]
        
        assert isinstance(csv_io_manager, CSVIOManager)
        
        # Verify we no longer have the enhanced_csv_io_manager  
        assert "enhanced_csv_io_manager" not in defs.resources, "Should not have enhanced_csv_io_manager"
        
        # Verify we have the new merged assets
        asset_names = {asset.key.to_user_string() for asset in defs.assets}
        
        assert "llm_models" in asset_names, "Should have merged llm_models asset"
        assert "generation_templates" in asset_names, "Should have generation_templates asset"
        assert "evaluation_templates" in asset_names, "Should have evaluation_templates asset"
        assert "generation_models" not in asset_names, "Should not have separate generation_models asset"
        assert "evaluation_models" not in asset_names, "Should not have separate evaluation_models asset"


class TestBackwardCompatibility:
    """Test that existing functionality still works."""
    
    def test_model_data_structure_still_works(self):
        """Test that model data structure is still compatible."""
        models_csv_path = Path("data/1_raw/llm_models.csv")
        assert models_csv_path.exists(), f"Required models CSV not found: {models_csv_path}"
        
        models_df = pd.read_csv(models_csv_path)
        
        # Verify we have models with the expected columns
        generation_models = models_df[models_df["for_generation"] == True]
        evaluation_models = models_df[models_df["for_evaluation"] == True]
        
        assert len(generation_models) > 0, "Should have generation models"
        assert len(evaluation_models) > 0, "Should have evaluation models"
        
        # Verify the llm_models asset loads all models and can be filtered by core assets
        from daydreaming_dagster.assets.raw_data import llm_models
        from daydreaming_dagster.resources.data_paths_config import DataPathsConfig
        from dagster import build_asset_context
        
        # Create proper asset context with resources
        context = build_asset_context(resources={"data_paths_config": DataPathsConfig()})
        
        # Load all models using the new asset
        all_models = llm_models(context)
        
        # Verify filtering still works (now done in core assets)
        generation_filtered = all_models[all_models["for_generation"] == True]
        evaluation_filtered = all_models[all_models["for_evaluation"] == True]
        
        assert len(generation_filtered) > 0, "Should be able to filter for generation models"
        assert len(evaluation_filtered) > 0, "Should be able to filter for evaluation models"
        assert len(all_models) >= len(generation_filtered), "All models should include generation models"
        assert len(all_models) >= len(evaluation_filtered), "All models should include evaluation models"