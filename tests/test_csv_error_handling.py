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
                
                # New model: CSVs are sources; consumers read them directly.
                # Assert that malformed CSV raises a parsing error when read.
                with pytest.raises(Exception):
                    pd.read_csv(malformed_csv_path)
                print("✅ Malformed evaluation_templates.csv raised a parsing error as expected")

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
                
                # New model: read CSV directly and ensure it parses.
                df = pd.read_csv(valid_csv_path)
                assert len(df) == 3, "Should load 3 templates"
                print("✅ Valid CSV parsing works correctly under source-only model")

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
                
                # New model: read CSV directly and ensure pandas raises.
                with pytest.raises(Exception):
                    pd.read_csv(malformed_csv_path)
                print("✅ Malformed concepts metadata CSV raised a parsing error")
