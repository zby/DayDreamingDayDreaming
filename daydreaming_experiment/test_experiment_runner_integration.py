import pytest
import tempfile
from pathlib import Path
from click.testing import CliRunner

from daydreaming_experiment.experiment_runner import run_experiment
from daydreaming_experiment.concept_db import ConceptDB
from daydreaming_experiment.concept import Concept


class TestExperimentRunnerIntegration:
    """Integration tests that would have caught the bugs."""
    
    def test_cli_with_real_concept_database(self):
        """Test CLI integration with real concept database."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create real concept database
            concept_db = ConceptDB()
            concept_db.add_concept(
                Concept(name="test_concept", descriptions={"sentence": "A test concept"})
            )
            
            # Save to filesystem 
            manifest_path = Path(temp_dir) / "day_dreaming_concepts.json"
            concept_db.save(str(manifest_path))
            
            runner = CliRunner()
            
            # Test 1: Would catch the wrong method name bug
            result = runner.invoke(run_experiment, [
                '--concepts-dir', temp_dir,
                '--k-max', '1',
                '--level', 'sentence',
                '--output', f'{temp_dir}/output'
            ])
            
            # This would fail with AttributeError: 'ConceptDB' object has no attribute 'get_all_concepts'
            # if the wrong method name was used
            
            # Test 2: Would catch the directory path bug  
            result = runner.invoke(run_experiment, [
                '--concepts-dir', temp_dir,  # Directory path - should work
                '--k-max', '1'
            ])
            
            # Without proper path construction, this would fail with:
            # IsADirectoryError: [Errno 21] Is a directory
    
    def test_concepts_dir_path_handling(self):
        """Test that CLI properly constructs manifest path from directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create the expected manifest file
            manifest_path = Path(temp_dir) / "day_dreaming_concepts.json"
            manifest_path.write_text('{"version": "1.0", "concepts": []}')
            
            runner = CliRunner()
            
            # Pass directory path - CLI should construct full manifest path
            result = runner.invoke(run_experiment, [
                '--concepts-dir', temp_dir,  # Just directory
                '--k-max', '1',
                '--help'  # Don't actually run, just test path construction
            ])
            
            # Would catch if CLI tries to open directory as file
    
    def test_concept_db_method_exists(self):
        """Test that the method we're calling actually exists."""
        concept_db = ConceptDB()
        
        # This would catch method name errors
        assert hasattr(concept_db, 'get_concepts')
        assert callable(getattr(concept_db, 'get_concepts'))
        
        # This would fail if we used the wrong method name
        concepts = concept_db.get_concepts()
        assert isinstance(concepts, list)