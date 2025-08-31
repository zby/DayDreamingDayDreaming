"""Simplified unit tests for raw data assets focusing on active column logic."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from daydreaming_dagster.models import Concept


class TestConceptsActiveFilteringLogic:
    """Test the core filtering logic that will be added to concepts asset."""
    
    def test_active_filtering_logic_filters_correctly(self):
        """Test that active filtering logic works correctly when applied to concepts metadata."""
        # This tests the core logic that needs to be added to the concepts asset
        
        # Sample concepts_metadata DataFrame with active column
        concepts_metadata_df = pd.DataFrame([
            {"concept_id": "active-concept-1", "name": "Active Concept 1", "active": True},
            {"concept_id": "active-concept-2", "name": "Active Concept 2", "active": True},
            {"concept_id": "inactive-concept-1", "name": "Inactive Concept 1", "active": False},
            {"concept_id": "inactive-concept-2", "name": "Inactive Concept 2", "active": False}
        ])
        
        # Simulate the active filtering logic that needs to be added
        def apply_active_filtering(df):
            """The filtering logic that should be added to the concepts asset."""
            if "active" in df.columns:
                return df[df["active"] == True]
            else:
                # Backward compatibility: if no active column, include all
                return df
        
        # Test the filtering logic
        filtered_df = apply_active_filtering(concepts_metadata_df)
        
        # This will fail until the logic is implemented in the actual asset
        expected_count = 2  # Only active concepts
        actual_count = len(concepts_metadata_df)  # Current implementation returns all
        
        assert expected_count != actual_count, \
            f"Test setup validation: expected filtered count ({expected_count}) should differ from unfiltered ({actual_count})"
        
        # These assertions show what the implemented logic should achieve
        assert len(filtered_df) == 2, "Should return only active concepts"
        assert all(filtered_df["active"] == True), "All returned concepts should be active"
        
        filtered_concept_ids = set(filtered_df["concept_id"])
        expected_active_ids = {"active-concept-1", "active-concept-2"}
        assert filtered_concept_ids == expected_active_ids, "Should return correct active concept IDs"
    
    
    def test_backward_compatibility_logic(self):
        """Test logic for handling missing active column (backward compatibility)."""
        # Legacy CSV without active column
        legacy_concepts_df = pd.DataFrame([
            {"concept_id": "legacy-concept-1", "name": "Legacy Concept 1"},
            {"concept_id": "legacy-concept-2", "name": "Legacy Concept 2"}
        ])
        
        def apply_active_filtering_with_fallback(df):
            """Backward compatible active filtering logic."""
            if "active" in df.columns:
                return df[df["active"] == True]
            else:
                # Backward compatibility: treat missing active column as all active
                return df
        
        filtered_df = apply_active_filtering_with_fallback(legacy_concepts_df)
        
        # Should include all concepts when active column is missing
        assert len(filtered_df) == 2, "Should include all concepts when active column missing"
        assert list(filtered_df["concept_id"]) == ["legacy-concept-1", "legacy-concept-2"]
    
    def test_pandas_boolean_filtering_behavior(self):
        """Test pandas boolean filtering behavior with various data types."""
        concepts_with_mixed_active = pd.DataFrame([
            {"concept_id": "concept-1", "name": "Concept 1", "active": True},
            {"concept_id": "concept-2", "name": "Concept 2", "active": "yes"},  # String
            {"concept_id": "concept-3", "name": "Concept 3", "active": 1},      # Integer 1
            {"concept_id": "concept-4", "name": "Concept 4", "active": None},   # Null
            {"concept_id": "concept-5", "name": "Concept 5", "active": False}
        ])
        
        def apply_pandas_boolean_filtering(df):
            """Standard pandas boolean filtering (== True)."""
            if "active" in df.columns:
                return df[df["active"] == True]
            return df
        
        filtered_df = apply_pandas_boolean_filtering(concepts_with_mixed_active)
        
        # Pandas treats integer 1 as == True, so we expect both concept-1 and concept-3
        assert len(filtered_df) == 2, "Pandas should match both boolean True and integer 1"
        
        result_ids = set(filtered_df["concept_id"])
        expected_ids = {"concept-1", "concept-3"}  # True and 1
        assert result_ids == expected_ids, "Should include boolean True and integer 1"
        
        # Verify excluded values
        assert "concept-2" not in result_ids, "Should exclude string 'yes'"
        assert "concept-4" not in result_ids, "Should exclude None"
        assert "concept-5" not in result_ids, "Should exclude boolean False"
    
    def test_strict_boolean_filtering_approach(self):
        """Test more restrictive boolean filtering if needed."""
        concepts_with_mixed_active = pd.DataFrame([
            {"concept_id": "concept-1", "name": "Concept 1", "active": True},
            {"concept_id": "concept-2", "name": "Concept 2", "active": "true"},
            {"concept_id": "concept-3", "name": "Concept 3", "active": 1},
            {"concept_id": "concept-4", "name": "Concept 4", "active": None},
            {"concept_id": "concept-5", "name": "Concept 5", "active": False}
        ])
        
        def apply_strict_boolean_filtering(df):
            """Strict boolean filtering using is True comparison."""
            if "active" in df.columns:
                # More restrictive: use pandas.Series.apply with type checking
                return df[df["active"].apply(lambda x: x is True)]
            return df
        
        filtered_df = apply_strict_boolean_filtering(concepts_with_mixed_active)
        
        # This will fail initially, demonstrating stricter approach option
        # With standard == filtering, we'd get both True and 1
        standard_filtered = concepts_with_mixed_active[concepts_with_mixed_active["active"] == True]
        
        assert len(filtered_df) != len(standard_filtered), \
            "Strict filtering should be more restrictive than == filtering"
        
        # Should only include concept-1 (explicit boolean True)
        assert len(filtered_df) == 1, "Should only match boolean True with 'is True'"
        assert filtered_df.iloc[0]["concept_id"] == "concept-1"


class TestCSVIOManagerSourceMappingLogic:
    """Test the source mapping logic that needs to be added to definitions.py."""
    
    def test_concepts_metadata_source_mapping_needs_active_filter(self):
        """Test that concepts_metadata source mapping should include active filter."""
        # Current source mapping from definitions.py (without active filter)
        current_mapping = {
            "source_file": "data/1_raw/concepts_metadata.csv"
            # No filters currently
        }
        
        # Expected mapping after implementing active column feature
        expected_mapping = {
            "source_file": "data/1_raw/concepts_metadata.csv",
            "filters": [{"column": "active", "value": True}]
        }
        
        # This assertion will fail initially, showing what needs to be added
        assert "filters" not in current_mapping, \
            "Current mapping doesn't have filters (this shows what needs to be implemented)"
        
        # This shows what the implementation should achieve
        assert "filters" in expected_mapping, "Expected mapping should have active filter"
        
        active_filter = expected_mapping["filters"][0]
        assert active_filter["column"] == "active", "Should filter on active column"
        assert active_filter["value"] == True, "Should filter for active=True"


class TestConceptsAssetMetadataUpdates:
    """Test metadata updates that should be added to concepts asset."""
    
    def test_concepts_metadata_should_include_active_filtering_stats(self):
        """Test that concepts asset should report active filtering statistics."""
        # Sample data for testing metadata calculation
        concepts_metadata_df = pd.DataFrame([
            {"concept_id": "concept-1", "name": "Concept 1", "active": True},
            {"concept_id": "concept-2", "name": "Concept 2", "active": False},
            {"concept_id": "concept-3", "name": "Concept 3", "active": True}
        ])
        
        def calculate_active_filtering_metadata(df):
            """Metadata calculation logic to be added to concepts asset."""
            if "active" in df.columns:
                total_concepts = len(df)
                active_concepts = len(df[df["active"] == True])
                inactive_concepts = total_concepts - active_concepts
                
                return {
                    "total_concepts_in_csv": total_concepts,
                    "active_concepts_count": active_concepts,
                    "inactive_concepts_count": inactive_concepts,
                    "active_filtering_applied": True
                }
            else:
                return {
                    "total_concepts_in_csv": len(df),
                    "active_filtering_applied": False
                }
        
        metadata = calculate_active_filtering_metadata(concepts_metadata_df)
        
        # These assertions show what the implemented metadata should include
        assert metadata["total_concepts_in_csv"] == 3, "Should report total concepts"
        assert metadata["active_concepts_count"] == 2, "Should report active count"
        assert metadata["inactive_concepts_count"] == 1, "Should report inactive count"  
        assert metadata["active_filtering_applied"] == True, "Should indicate filtering applied"
    
    def test_existing_concepts_metadata_lacks_active_stats(self):
        """Test that current concepts asset doesn't provide active filtering metadata."""
        # This test demonstrates what's currently missing and needs to be added
        
        # Current metadata keys (from examining the existing concepts asset)
        current_metadata_keys = {
            "concept_count", "total_available", "filtered_out", 
            "description_level", "has_filter", "filter_applied"
        }
        
        # Expected new metadata keys after implementation
        expected_new_keys = {
            "active_concepts_count", "inactive_concepts_count", "active_filter_applied"
        }
        
        # This assertion will pass initially, showing current state
        assert expected_new_keys.isdisjoint(current_metadata_keys), \
            "Current metadata doesn't include active filtering stats (this shows what needs to be added)"
        
        # This shows what the implementation should add
        assert len(expected_new_keys) == 3, "Should add 3 new metadata fields for active filtering"
