"""Integration tests for enhanced CSV parsing error handling in raw_data assets.

Tests CSV parsing failures with enhanced error messages that include:
- Original pandas error message
- Problematic line number
- CSV content around the problematic line
- ">>>" marker pointing to the specific problematic line
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch
import os

from dagster import materialize, DagsterInstance, Failure

pytestmark = [pytest.mark.integration]

class TestCSVErrorHandling:
    """Tests for enhanced CSV parsing error handling in raw_data assets."""

    def test_evaluation_templates_malformed_csv_error_handling(self):
        """Test that evaluation_templates asset provides enhanced error message for malformed CSV."""
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home"
            temp_data_dir = Path(temp_root) / "data"
            
            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            
            # Create a malformed evaluation_templates.csv with unquoted commas in description
            raw_data_path = temp_data_dir / "1_raw"
            malformed_csv_content = '''template_id,template_name,description,active
creativity-metrics,Creativity Metrics,Evaluates novel creative thinking patterns,true
novel-elements-coverage-v1,Novel Elements Coverage,Checks coverage of novel elements, mechanism specificity, originality, and leakage (0–9 score),false
scientific-rigor,Scientific Rigor,Assesses methodological soundness,true'''
            
            malformed_csv_path = raw_data_path / "evaluation_templates.csv"
            malformed_csv_path.write_text(malformed_csv_content)
            
            # Create the evaluation_templates directory (even though we won't use it due to CSV error)
            (raw_data_path / "evaluation_templates").mkdir()
            
            with patch.dict(os.environ, {'DAGSTER_HOME': str(temp_dagster_home)}):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                from daydreaming_dagster.assets.raw_data import evaluation_templates
                
                resources = {
                    "data_root": str(temp_data_dir),
                }
                
                # This should fail due to malformed CSV and include enhanced context
                with pytest.raises(Failure) as exc_info:
                    materialize(
                        [evaluation_templates],
                        resources=resources,
                        instance=instance
                    )
                
                failure_message = str(exc_info.value)
                
                # Verify the enhanced error message contains all required components:
                
                # 1. Original pandas error message (should contain something about parsing)
                assert "Error" in failure_message or "parsing" in failure_message.lower(), \
                    f"Should contain original pandas error information. Got: {failure_message}"
                
                # 2. Line number information (line 3 is the problematic one)
                assert "line 3" in failure_message.lower() or "line: 3" in failure_message, \
                    f"Should contain problematic line number (3). Got: {failure_message}"
                
                # 3. CSV content around the problematic line
                assert "novel-elements-coverage-v1" in failure_message, \
                    f"Should show content around problematic line. Got: {failure_message}"
                
                # 4. The ">>>" marker pointing to the specific problematic line
                assert ">>>" in failure_message, \
                    f"Should contain '>>>' marker pointing to problematic line. Got: {failure_message}"
                
                # 5. The problematic line content should be shown after the marker
                assert "Checks coverage of novel elements, mechanism specificity" in failure_message, \
                    f"Should show the actual problematic line content. Got: {failure_message}"
                
                # 6. Should mention the file path for context
                assert "evaluation_templates.csv" in failure_message, \
                    f"Should mention the file being parsed. Got: {failure_message}"
                
                print("✅ Enhanced CSV error handling produced detailed context for evaluation_templates.csv")

    def test_evaluation_templates_successful_parsing_with_quoted_commas(self):
        """Test that evaluation_templates asset works correctly with properly quoted CSV."""
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home"
            temp_data_dir = Path(temp_root) / "data"
            
            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            
            # Create a properly formatted evaluation_templates.csv with quoted commas
            raw_data_path = temp_data_dir / "1_raw"
            valid_csv_content = '''template_id,template_name,description,active
creativity-metrics,Creativity Metrics,Evaluates novel creative thinking patterns,true
novel-elements-coverage-v1,Novel Elements Coverage,"Checks coverage of novel elements, mechanism specificity, originality, and leakage (0–9 score)",false
scientific-rigor,Scientific Rigor,Assesses methodological soundness,true'''
            
            valid_csv_path = raw_data_path / "evaluation_templates.csv"
            valid_csv_path.write_text(valid_csv_content)
            
            # Create the evaluation_templates directory and template files
            templates_dir = raw_data_path / "evaluation_templates"
            templates_dir.mkdir()
            templates_dir.joinpath("creativity-metrics.txt").write_text("Template content for creativity metrics")
            templates_dir.joinpath("novel-elements-coverage-v1.txt").write_text("Template content for novel elements")
            templates_dir.joinpath("scientific-rigor.txt").write_text("Template content for scientific rigor")
            
            with patch.dict(os.environ, {'DAGSTER_HOME': str(temp_dagster_home)}):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                from daydreaming_dagster.assets.raw_data import evaluation_templates
                
                resources = {
                    "data_root": str(temp_data_dir),
                }
                
                # This should succeed with properly quoted CSV
                result = materialize(
                    [evaluation_templates],
                    resources=resources,
                    instance=instance
                )
                
                assert result.success, "Should successfully parse properly quoted CSV"
                
                # Verify the result contains expected data
                materializations = result.asset_materializations_for_node("evaluation_templates")
                assert len(materializations) == 1, "Should have one materialization"
                
                # Check metadata
                metadata = materializations[0].metadata
                total_templates = metadata["total_templates"].value
                assert total_templates == 3, f"Should load 3 templates, got {total_templates}"
                
                print("✅ Valid CSV parsing works correctly")

    def test_concepts_metadata_csv_error_handling_integration(self):
        """Test enhanced error handling for concepts metadata CSV parsing.
        
        Similar test pattern for concepts asset to ensure consistency.
        """
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home"
            temp_data_dir = Path(temp_root) / "data"
            
            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            
            # Create a malformed concepts metadata CSV
            concepts_dir = temp_data_dir / "1_raw" / "concepts"
            concepts_dir.mkdir()
            
            malformed_csv_content = '''concept_id,name,active
test-concept-1,Test Concept 1,true
malformed-concept,Test Concept with, unquoted commas in name,true
test-concept-3,Test Concept 3,false'''
            
            malformed_csv_path = concepts_dir / "concepts_metadata.csv"
            malformed_csv_path.write_text(malformed_csv_content)
            
            with patch.dict(os.environ, {'DAGSTER_HOME': str(temp_dagster_home)}):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                from daydreaming_dagster.assets.raw_data import concepts
                
                resources = {
                    "data_root": str(temp_data_dir),
                }
                
                # This should fail due to malformed CSV, but currently won't have enhanced error handling
                with pytest.raises((Failure, Exception)) as exc_info:
                    materialize(
                        [concepts],
                        resources=resources,
                        instance=instance
                    )
                
                error_message = str(exc_info.value)
                
                # For now, we just verify that parsing fails - enhanced error handling will be implemented later
                assert len(error_message) > 0, "Should produce some error message"
                
                print("✅ Test confirmed that concepts CSV parsing fails on malformed data")
                print(f"Current error (to be enhanced): {error_message[:100]}...")
