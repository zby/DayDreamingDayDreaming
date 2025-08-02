"""
Tests for Dagster resource dependency issues.

This test suite validates that all assets properly declare their required resources
to prevent runtime errors when accessing resources through context.resources.
"""

import pytest
import inspect
import ast
from dagster import build_asset_context
from daydreaming_dagster import defs


class ResourceAccessVisitor(ast.NodeVisitor):
    """AST visitor to find context.resources.resource_name accesses."""
    
    def __init__(self):
        self.resource_accesses = []
    
    def visit_Attribute(self, node):
        # Look for patterns like context.resources.resource_name
        if (isinstance(node.value, ast.Attribute) and 
            isinstance(node.value.value, ast.Name) and
            node.value.value.id == 'context' and
            node.value.attr == 'resources'):
            self.resource_accesses.append(node.attr)
        self.generic_visit(node)


def get_resource_accesses_from_function(func):
    """Parse function source code to find resource accesses."""
    try:
        source = inspect.getsource(func)
        tree = ast.parse(source)
        visitor = ResourceAccessVisitor()
        visitor.visit(tree)
        return visitor.resource_accesses
    except (OSError, TypeError):
        # Some functions may not have accessible source
        return []


class TestResourceDependencies:
    """Test that all assets properly declare their resource dependencies."""
    
    def test_all_assets_declare_required_resources(self):
        """
        Test that every asset that accesses context.resources.X has X in required_resource_keys.
        
        This test prevents the runtime error:
        DagsterUnknownResourceError: Unknown resource `X`. Specify `X` as a required resource.
        """
        failures = []
        
        for asset_def in defs.assets:
            # Get the asset's compute function
            compute_fn = asset_def.op.compute_fn
            
            # Find all resource accesses in the function
            resource_accesses = get_resource_accesses_from_function(compute_fn)
            
            # Get declared required resources
            required_resources = asset_def.required_resource_keys or set()
            
            # Check if all accessed resources are declared
            for resource_name in resource_accesses:
                if resource_name not in required_resources:
                    failures.append({
                        'asset': asset_def.key.to_user_string(),
                        'function': compute_fn.__name__,
                        'missing_resource': resource_name,
                        'accessed_resources': resource_accesses,
                        'declared_resources': list(required_resources)
                    })
        
        if failures:
            error_msg = "Assets are accessing resources without declaring them as required:\n"
            for failure in failures:
                error_msg += (
                    f"  Asset '{failure['asset']}' accesses resource '{failure['missing_resource']}' "
                    f"but doesn't declare it in required_resource_keys.\n"
                    f"    Function: {failure['function']}\n"
                    f"    Accessed resources: {failure['accessed_resources']}\n"
                    f"    Declared resources: {failure['declared_resources']}\n\n"
                )
            
            pytest.fail(error_msg)
    
    def test_experiment_config_resource_access(self):
        """
        Test specific case that caused the original error.
        Assets accessing experiment_config must declare it as required.
        """
        # Find assets that should use experiment_config
        experiment_config_assets = []
        
        for asset_def in defs.assets:
            compute_fn = asset_def.op.compute_fn
            resource_accesses = get_resource_accesses_from_function(compute_fn)
            
            if 'experiment_config' in resource_accesses:
                experiment_config_assets.append({
                    'asset': asset_def.key.to_user_string(),
                    'required_resources': asset_def.required_resource_keys or set()
                })
        
        # Verify all these assets declare experiment_config as required
        for asset_info in experiment_config_assets:
            assert 'experiment_config' in asset_info['required_resources'], (
                f"Asset '{asset_info['asset']}' accesses experiment_config but doesn't "
                f"declare it in required_resource_keys: {asset_info['required_resources']}"
            )
    
    def test_openrouter_client_resource_access(self):
        """
        Test that assets accessing openrouter_client declare it as required.
        """
        openrouter_assets = []
        
        for asset_def in defs.assets:
            compute_fn = asset_def.op.compute_fn
            resource_accesses = get_resource_accesses_from_function(compute_fn)
            
            if 'openrouter_client' in resource_accesses:
                openrouter_assets.append({
                    'asset': asset_def.key.to_user_string(),
                    'required_resources': asset_def.required_resource_keys or set()
                })
        
        # Verify all these assets declare openrouter_client as required
        for asset_info in openrouter_assets:
            assert 'openrouter_client' in asset_info['required_resources'], (
                f"Asset '{asset_info['asset']}' accesses openrouter_client but doesn't "
                f"declare it in required_resource_keys: {asset_info['required_resources']}"
            )
    
    def test_asset_context_with_missing_resource_fails(self):
        """
        Integration test: verify that missing resources cause proper failures.
        """
        from daydreaming_dagster.assets.core import concepts_and_content
        
        # Create context without required resources
        context_without_resources = build_asset_context()
        
        # Mock the required asset dependencies
        import pandas as pd
        mock_concepts_metadata = pd.DataFrame([{"concept_id": "test", "name": "Test"}])
        mock_descriptions = {"test": "test description"}
        
        # This should fail because experiment_config is not provided
        with pytest.raises(Exception) as exc_info:
            concepts_and_content(
                context_without_resources,
                mock_concepts_metadata,
                mock_descriptions,
                mock_descriptions,
                mock_descriptions
            )
        
        # Should be a resource-related error
        assert "experiment_config" in str(exc_info.value)
    
    def test_asset_context_with_resources_succeeds(self):
        """
        Integration test: verify that providing required resources allows assets to work.
        """
        from daydreaming_dagster.assets.core import concepts_and_content
        from daydreaming_dagster.resources.api_client import ExperimentConfig
        
        # Create context with required resources
        context_with_resources = build_asset_context(
            resources={"experiment_config": ExperimentConfig()}
        )
        
        # Mock the required asset dependencies
        import pandas as pd
        mock_concepts_metadata = pd.DataFrame([{"concept_id": "test", "name": "Test"}])
        mock_descriptions = {"test": "test description"}
        
        # This should succeed
        result = concepts_and_content(
            context_with_resources,
            mock_concepts_metadata,
            mock_descriptions,
            mock_descriptions,
            mock_descriptions
        )
        
        # Verify the result structure
        assert isinstance(result, tuple)
        assert len(result) == 2
        concepts_list, concept_contents_dict = result
        assert isinstance(concepts_list, list)
        assert isinstance(concept_contents_dict, dict)


class TestResourceAvailability:
    """Test that all required resources are properly configured in definitions."""
    
    def test_all_required_resources_exist_in_definitions(self):
        """
        Test that every resource referenced by assets exists in the definitions.
        """
        # Collect all required resources from all assets
        all_required_resources = set()
        for asset_def in defs.assets:
            if asset_def.required_resource_keys:
                all_required_resources.update(asset_def.required_resource_keys)
        
        # Get available resources from definitions
        available_resources = set(defs.resources.keys())
        
        # Check that all required resources are available
        missing_resources = all_required_resources - available_resources
        
        assert not missing_resources, (
            f"Assets require resources that are not defined: {missing_resources}\n"
            f"Required resources: {sorted(all_required_resources)}\n"
            f"Available resources: {sorted(available_resources)}"
        )
    
    def test_critical_resources_are_configured(self):
        """Test that critical resources for the experiment are properly configured."""
        required_critical_resources = {
            'experiment_config',
            'openrouter_client',
            'csv_io_manager',
            'generation_response_io_manager',
            'evaluation_response_io_manager'
        }
        
        available_resources = set(defs.resources.keys())
        
        missing_critical = required_critical_resources - available_resources
        
        assert not missing_critical, (
            f"Critical resources are missing from definitions: {missing_critical}"
        )


if __name__ == "__main__":
    # Run tests directly for debugging
    test_instance = TestResourceDependencies()
    
    print("Testing resource dependency declarations...")
    try:
        test_instance.test_all_assets_declare_required_resources()
        print("✓ All assets properly declare required resources")
    except Exception as e:
        print(f"✗ Resource dependency test failed: {e}")
    
    try:
        test_instance.test_experiment_config_resource_access()
        print("✓ experiment_config resource properly declared")
    except Exception as e:
        print(f"✗ experiment_config test failed: {e}")
    
    try:
        test_instance.test_openrouter_client_resource_access()
        print("✓ openrouter_client resource properly declared")
    except Exception as e:
        print(f"✗ openrouter_client test failed: {e}")
    
    resource_test = TestResourceAvailability()
    try:
        resource_test.test_all_required_resources_exist_in_definitions()
        print("✓ All required resources exist in definitions")
    except Exception as e:
        print(f"✗ Resource availability test failed: {e}")
    
    try:
        resource_test.test_critical_resources_are_configured()
        print("✓ Critical resources are configured")
    except Exception as e:
        print(f"✗ Critical resources test failed: {e}")