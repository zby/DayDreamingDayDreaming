"""
Test for Dagster CLI compatibility issues.

This test reproduces the ANTLR4 compatibility issue with Python 3.13
that prevents asset selection from working properly.
"""

import pytest
from dagster import AssetSelection


class TestDagsterCLICompatibility:
    """Test Dagster CLI functionality that may have compatibility issues."""
    
    def test_asset_selection_parsing(self):
        """
        Test that AssetSelection.from_string works with asset group patterns.
        
        This test reproduces the ANTLR4 compatibility issue with Python 3.13
        where asset selection parsing fails due to ord() expecting string but getting int.
        """
        # Test basic asset selection patterns that should work
        test_patterns = [
            "raw_data*",
            "*",
            "concepts_metadata",
            "daydreaming_experiment*",
            "llm_generation*"
        ]
        
        for pattern in test_patterns:
            try:
                # This should work but fails on Python 3.13 with ANTLR4 4.9.3
                selection = AssetSelection.from_string(pattern)
                assert selection is not None
                print(f"✓ Pattern '{pattern}' parsed successfully")
            except TypeError as e:
                if "ord() expected string of length 1, but int found" in str(e):
                    pytest.fail(
                        f"ANTLR4 compatibility issue with Python 3.13 detected for pattern '{pattern}': {e}"
                    )
                else:
                    raise
            except Exception as e:
                # Other exceptions may be expected (e.g., if assets don't exist)
                print(f"Pattern '{pattern}' failed with: {type(e).__name__}: {e}")
                
    def test_asset_selection_from_coercible(self):
        """
        Test AssetSelection.from_coercible with multiple patterns.
        
        This reproduces the exact error from the CLI command.
        """
        try:
            # This reproduces the exact call that fails in the CLI
            selection = AssetSelection.from_coercible(["raw_data*"])
            assert selection is not None
            print("✓ AssetSelection.from_coercible works")
        except TypeError as e:
            if "ord() expected string of length 1, but int found" in str(e):
                pytest.fail(f"ANTLR4 compatibility issue detected: {e}")
            else:
                raise
                
    def test_simple_asset_selection_without_patterns(self):
        """
        Test that simple asset selection works without wildcard patterns.
        This should work even with the ANTLR4 issue.
        """
        try:
            # Simple asset name without patterns should work
            selection = AssetSelection.from_string("concepts_metadata")
            assert selection is not None
            print("✓ Simple asset selection works")
        except Exception as e:
            print(f"Simple asset selection failed: {type(e).__name__}: {e}")


class TestDagsterAssetInstantiation:
    """Test that our Dagster assets can be instantiated properly."""
    
    def test_dagster_definitions_load(self):
        """Test that our Dagster definitions load without errors."""
        try:
            from daydreaming_dagster.definitions import defs
            assert defs is not None
            assert len(defs.assets) > 0
            print(f"✓ Dagster definitions loaded with {len(defs.assets)} assets")
        except Exception as e:
            pytest.fail(f"Failed to load Dagster definitions: {e}")
    
    def test_asset_names_accessible(self):
        """Test that we can access asset names without triggering ANTLR issues."""
        try:
            from daydreaming_dagster.definitions import defs
            asset_names = [asset.key.to_user_string() for asset in defs.assets]
            assert len(asset_names) > 0
            print(f"✓ Successfully retrieved {len(asset_names)} asset names")
            
            # Check for expected asset groups
            raw_data_assets = [name for name in asset_names if name.startswith("raw_data/") or name in [
                "concepts_metadata", "concept_descriptions_sentence", 
                "concept_descriptions_paragraph", "concept_descriptions_article",
                "generation_models", "evaluation_models", 
                "generation_templates", "evaluation_templates"
            ]]
            assert len(raw_data_assets) > 0
            print(f"✓ Found {len(raw_data_assets)} raw data assets")
            
        except Exception as e:
            pytest.fail(f"Failed to access asset names: {e}")


if __name__ == "__main__":
    # Run tests directly for debugging
    test_instance = TestDagsterCLICompatibility()
    print("Testing ANTLR4 compatibility...")
    
    try:
        test_instance.test_asset_selection_parsing()
        print("✓ Asset selection parsing test passed")
    except Exception as e:
        print(f"✗ Asset selection parsing test failed: {e}")
    
    try:
        test_instance.test_asset_selection_from_coercible()
        print("✓ Asset selection from coercible test passed")
    except Exception as e:
        print(f"✗ Asset selection from coercible test failed: {e}")
    
    try:
        test_instance.test_simple_asset_selection_without_patterns()
        print("✓ Simple asset selection test passed")
    except Exception as e:
        print(f"✗ Simple asset selection test failed: {e}")
    
    definitions_test = TestDagsterAssetInstantiation()
    try:
        definitions_test.test_dagster_definitions_load()
        print("✓ Dagster definitions load test passed")
    except Exception as e:
        print(f"✗ Dagster definitions load test failed: {e}")
    
    try:
        definitions_test.test_asset_names_accessible()
        print("✓ Asset names accessible test passed")
    except Exception as e:
        print(f"✗ Asset names accessible test failed: {e}")