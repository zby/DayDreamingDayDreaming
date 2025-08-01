"""Integration tests for pipeline node functions.

These tests use real data files and are designed to fail if data is missing,
following the project's integration testing guidelines.
"""

import pytest
import pandas as pd
from pathlib import Path

from daydreaming_experiment.pipelines.daydreaming.nodes import create_task_list, generate_prompts


class TestCreateTaskList:
    """Integration tests for the create_task_list node function."""
    
    def test_create_task_list_with_real_concepts(self):
        """Test create_task_list with real concept database - fails if data missing."""
        concepts_file = "data/01_raw/concepts/day_dreaming_concepts.json"
        
        # This should fail with clear error if file doesn't exist
        tasks_df, task_concepts_df, concept_contents = create_task_list(concepts_file, k_max=2)
        
        # Verify tasks_df structure
        assert isinstance(tasks_df, pd.DataFrame)
        assert not tasks_df.empty
        assert 'run_id' in tasks_df.columns
        
        # Verify required columns exist in tasks_df
        required_columns = ['template', 'generator_model', 'evaluator_model', 'k_max']
        for col in required_columns:
            assert col in tasks_df.columns, f"Missing required column: {col}"
        
        # Verify task_concepts_df structure
        assert isinstance(task_concepts_df, pd.DataFrame)
        assert not task_concepts_df.empty
        required_tc_columns = ['run_id', 'concept_id', 'concept_name', 'concept_order']
        for col in required_tc_columns:
            assert col in task_concepts_df.columns, f"Missing required task_concepts column: {col}"
        
        # Verify concept_contents structure
        assert isinstance(concept_contents, dict)
        assert len(concept_contents) > 0
        for concept_id, content in concept_contents.items():
            assert isinstance(concept_id, str), f"Concept ID should be string: {concept_id}"
            assert isinstance(content, str), f"Concept content should be string: {content}"
            assert len(content.strip()) > 0, f"Empty concept content for {concept_id}"
        
        # Verify run_id format
        for run_id in tasks_df['run_id']:
            assert run_id.startswith('run_'), f"Invalid run_id format: {run_id}"
            assert len(run_id) == 9, f"Invalid run_id length: {run_id}"  # 'run_' + 5 digits
        
        # Verify task-concept relationships
        for run_id in tasks_df['run_id']:
            task_concepts = task_concepts_df[task_concepts_df['run_id'] == run_id]
            assert len(task_concepts) == 2, f"Expected 2 concepts for k_max=2, got {len(task_concepts)}"
            
            # Verify concept ordering
            concept_orders = sorted(task_concepts['concept_order'].tolist())
            assert concept_orders == [1, 2], f"Invalid concept ordering: {concept_orders}"
        
        # Verify no duplicate combinations
        concept_pairs = set()
        for run_id in tasks_df['run_id']:
            task_concepts = task_concepts_df[task_concepts_df['run_id'] == run_id].sort_values('concept_order')
            concepts = task_concepts['concept_id'].tolist()
            pair = tuple(sorted(concepts))
            assert pair not in concept_pairs, f"Duplicate concept pair: {pair}"
            concept_pairs.add(pair)
            
            # Verify concepts are different
            assert len(set(concepts)) == len(concepts), f"Duplicate concepts in task: {concepts}"
    
    def test_create_task_list_with_k_max_3(self):
        """Test create_task_list with k_max=3 generates 3-concept combinations."""
        concepts_file = "data/01_raw/concepts/day_dreaming_concepts.json"
        
        tasks_df, task_concepts_df, concept_contents = create_task_list(concepts_file, k_max=3)
        
        # Verify basic structure
        assert isinstance(tasks_df, pd.DataFrame)
        assert not tasks_df.empty
        assert isinstance(task_concepts_df, pd.DataFrame)
        assert not task_concepts_df.empty
        
        # Verify each task has exactly 3 concepts
        for run_id in tasks_df['run_id']:
            task_concepts = task_concepts_df[task_concepts_df['run_id'] == run_id]
            assert len(task_concepts) == 3, f"Expected 3 concepts for k_max=3, got {len(task_concepts)}"
            
            # Verify concept ordering for k_max=3
            concept_orders = sorted(task_concepts['concept_order'].tolist())
            assert concept_orders == [1, 2, 3], f"Invalid concept ordering: {concept_orders}"
            
            # Verify all three concepts are different
            concepts = task_concepts['concept_id'].tolist()
            assert len(set(concepts)) == 3, f"Duplicate concepts in task: {concepts}"
    
    def test_create_task_list_with_k_max_1(self):
        """Test create_task_list with k_max=1 generates single-concept tasks."""
        concepts_file = "data/01_raw/concepts/day_dreaming_concepts.json"
        
        tasks_df, task_concepts_df, concept_contents = create_task_list(concepts_file, k_max=1)
        
        # Verify basic structure
        assert isinstance(tasks_df, pd.DataFrame)
        assert not tasks_df.empty
        assert isinstance(task_concepts_df, pd.DataFrame)
        assert not task_concepts_df.empty
        
        # Verify each task has exactly 1 concept
        for run_id in tasks_df['run_id']:
            task_concepts = task_concepts_df[task_concepts_df['run_id'] == run_id]
            assert len(task_concepts) == 1, f"Expected 1 concept for k_max=1, got {len(task_concepts)}"
            
            # Verify concept ordering for k_max=1
            concept_orders = task_concepts['concept_order'].tolist()
            assert concept_orders == [1], f"Invalid concept ordering: {concept_orders}"
        
        # Verify all concept IDs are unique (no duplicate tasks)
        all_concepts = task_concepts_df['concept_id'].tolist()
        assert len(all_concepts) == len(set(all_concepts)), "Duplicate concept tasks found"
    
    def test_create_task_list_file_not_found(self):
        """Test that create_task_list fails clearly when concept file is missing."""
        nonexistent_file = "data/01_raw/concepts/nonexistent_concepts.json"
        
        # Should raise FileNotFoundError or similar when file doesn't exist
        with pytest.raises((FileNotFoundError, OSError)):
            create_task_list(nonexistent_file, k_max=2)
    
    def test_create_task_list_default_parameters(self):
        """Test create_task_list with default parameters."""
        # Should use default file path and k_max=2
        tasks_df, task_concepts_df, concept_contents = create_task_list()
        
        # Verify basic structure
        assert isinstance(tasks_df, pd.DataFrame)
        assert not tasks_df.empty
        assert isinstance(task_concepts_df, pd.DataFrame)
        assert not task_concepts_df.empty
        
        # Should have k_max=2 by default
        for run_id in tasks_df['run_id']:
            task_concepts = task_concepts_df[task_concepts_df['run_id'] == run_id]
            assert len(task_concepts) == 2, f"Expected 2 concepts for default k_max=2, got {len(task_concepts)}"
    
    def test_create_task_list_output_structure(self):
        """Test the detailed structure of create_task_list output."""
        concepts_file = "data/01_raw/concepts/day_dreaming_concepts.json"
        tasks_df, task_concepts_df, concept_contents = create_task_list(concepts_file, k_max=2)
        
        # Verify default values in tasks_df
        for _, row in tasks_df.iterrows():
            assert row['template'] == '00_systematic_analytical'
            assert row['generator_model'] == 'deepseek/deepseek-r1:free'
            assert row['evaluator_model'] == 'meta-llama/llama-4-maverick:free'
            assert row['k_max'] == 2
        
        # Verify DataFrame has run_id as column
        assert 'run_id' in tasks_df.columns
        assert all(isinstance(run_id, str) for run_id in tasks_df['run_id'])
        
        # Verify no missing values in critical columns
        critical_task_columns = ['template', 'generator_model', 'evaluator_model', 'k_max']
        for col in critical_task_columns:
            assert not tasks_df[col].isna().any(), f"Found NaN values in tasks column: {col}"
        
        critical_tc_columns = ['run_id', 'concept_id', 'concept_name', 'concept_order']
        for col in critical_tc_columns:
            assert not task_concepts_df[col].isna().any(), f"Found NaN values in task_concepts column: {col}"
        
        # Verify all run_ids in task_concepts_df exist in tasks_df
        tc_run_ids = set(task_concepts_df['run_id'].unique())
        tasks_run_ids = set(tasks_df['run_id'])
        assert tc_run_ids == tasks_run_ids, "Mismatch between task and task_concepts run_ids"


class TestGeneratePrompts:
    """Integration tests for the generate_prompts node function."""
    
    def test_generate_prompts_with_real_data(self):
        """Test generate_prompts with real concept database and templates - fails if data missing."""
        # First create some tasks using real data
        concepts_file = "data/01_raw/concepts/day_dreaming_concepts.json"
        tasks_df, task_concepts_df, concept_contents = create_task_list(concepts_file, k_max=2)
        
        # Limit to just a few tasks for testing
        test_tasks = tasks_df.head(3)
        test_task_concepts = task_concepts_df[task_concepts_df['run_id'].isin(test_tasks['run_id'])]
        
        # Mock generation templates (simulate partitioned dataset loaders)
        def mock_template_loader():
            return """Below are several concepts to work with:

{% for concept in concepts %}
**{{ concept.name }}**: {{ concept.content }}
{% endfor %}

Please systematically explore how these concepts might be combined."""
        
        generation_templates = {
            "00_systematic_analytical": mock_template_loader
        }
        
        # Test the function
        prompts = generate_prompts(test_tasks, test_task_concepts, generation_templates, concept_contents)
        
        # Verify basic structure
        assert isinstance(prompts, dict)
        assert len(prompts) == len(test_tasks)
        
        # Verify all task run_ids have prompts
        for run_id in test_tasks['run_id']:
            assert run_id in prompts, f"Missing prompt for task {run_id}"
            assert isinstance(prompts[run_id], str), f"Prompt for {run_id} is not a string"
            assert len(prompts[run_id].strip()) > 0, f"Empty prompt for {run_id}"
        
        # Verify prompts contain concept names and descriptions
        for run_id, prompt in prompts.items():
            # Get concepts for this task
            task_concepts = test_task_concepts[test_task_concepts['run_id'] == run_id]
            concept_names = task_concepts['concept_name'].tolist()
            
            # Verify each concept name appears in the prompt
            for concept_name in concept_names:
                assert concept_name in prompt, f"Concept name '{concept_name}' not found in prompt for {run_id}"
                
        # Verify prompts are not identical (different concept combinations should yield different prompts)
        unique_prompts = set(prompts.values())
        assert len(unique_prompts) == len(prompts), "Found duplicate prompts for different tasks"
    
    def test_generate_prompts_with_real_templates(self):
        """Test generate_prompts with actual template files from partitioned dataset."""
        # Create minimal test data
        concepts_file = "data/01_raw/concepts/day_dreaming_concepts.json"
        tasks_df, task_concepts_df, concept_contents = create_task_list(concepts_file, k_max=2)
        
        # Use just one task for this test
        test_tasks = tasks_df.head(1)
        test_task_concepts = task_concepts_df[task_concepts_df['run_id'].isin(test_tasks['run_id'])]
        
        # Create real template loaders that read from actual files
        template_files = {
            "00_systematic_analytical": "data/01_raw/generation_templates/00_systematic_analytical.txt"
        }
        
        generation_templates = {}
        for template_name, template_path in template_files.items():
            def make_loader(path):
                def loader():
                    with open(path, 'r', encoding='utf-8') as f:
                        return f.read()
                return loader
            generation_templates[template_name] = make_loader(template_path)
        
        # Test the function
        prompts = generate_prompts(test_tasks, test_task_concepts, generation_templates, concept_contents)
        
        # Verify prompt was generated
        assert len(prompts) == 1
        run_id = list(prompts.keys())[0]
        prompt = prompts[run_id]
        
        # Verify prompt contains expected template content
        assert "systematically explore" in prompt.lower(), "Template content not found in prompt"
        assert "concepts might be combined" in prompt.lower(), "Template content not found in prompt"
        
        # Verify concept descriptions are included (not just names)
        task_concepts = test_task_concepts[test_task_concepts['run_id'] == run_id]
        concept_names = task_concepts['concept_name'].tolist()
        for concept_name in concept_names:
            assert concept_name in prompt, f"Concept name '{concept_name}' not found in prompt"
            # Should contain more than just the name (i.e., description content)
            concept_mentions = prompt.count(concept_name)
            assert concept_mentions >= 1, f"Concept '{concept_name}' should appear in prompt"
    
    def test_generate_prompts_missing_concept_content(self):
        """Test that generate_prompts fails when concept content is missing."""
        # Create minimal test data 
        import pandas as pd
        tasks_df = pd.DataFrame({
            'run_id': ['run_001'],
            'template': ['00_systematic_analytical'],
            'generator_model': ['test-model'],
            'evaluator_model': ['test-eval'],
            'k_max': [2]
        })
        
        task_concepts_df = pd.DataFrame({
            'run_id': ['run_001', 'run_001'],
            'concept_id': ['concept_a', 'concept_b'],
            'concept_name': ['Concept A', 'Concept B'],
            'concept_order': [1, 2]
        })
        
        generation_templates = {
            "00_systematic_analytical": lambda: "Test template with {% for concept in concepts %}{{ concept.content }}{% endfor %}"
        }
        
        # Missing concept_contents (empty dict)
        concept_contents = {}
        
        # Should raise KeyError when concept content is missing
        with pytest.raises(KeyError):
            generate_prompts(tasks_df, task_concepts_df, generation_templates, concept_contents)
    
    def test_generate_prompts_partial_concept_content(self):
        """Test that generate_prompts fails when some concept content is missing."""
        # Create test data with partial concept content
        import pandas as pd
        tasks_df = pd.DataFrame({
            'run_id': ['run_001'],
            'template': ['00_systematic_analytical'],
            'generator_model': ['test-model'],
            'evaluator_model': ['test-eval'],
            'k_max': [2]
        })
        
        task_concepts_df = pd.DataFrame({
            'run_id': ['run_001', 'run_001'],
            'concept_id': ['concept_a', 'concept_b'],
            'concept_name': ['Concept A', 'Concept B'],
            'concept_order': [1, 2]
        })
        
        generation_templates = {
            "00_systematic_analytical": lambda: "Test template with {% for concept in concepts %}{{ concept.content }}{% endfor %}"
        }
        
        # Only one concept has content, missing the second one
        concept_contents = {'concept_a': 'Content for concept A'}
        
        # Should raise KeyError when concept_b content is missing
        with pytest.raises(KeyError):
            generate_prompts(tasks_df, task_concepts_df, generation_templates, concept_contents)
    
    def test_generate_prompts_template_rendering_variables(self):
        """Test that templates receive correct variables (concepts with content)."""
        # Create minimal test data
        concepts_file = "data/01_raw/concepts/day_dreaming_concepts.json"
        tasks_df, task_concepts_df, concept_contents = create_task_list(concepts_file, k_max=2)
        
        # Use just one task
        test_tasks = tasks_df.head(1)
        test_task_concepts = task_concepts_df[task_concepts_df['run_id'].isin(test_tasks['run_id'])]
        
        # Template that uses the new concept structure
        def template_with_variables():
            return """Concept count: {{ concepts|length }}
{% for concept in concepts %}
- {{ concept.name }}: {{ concept.content }}
{% endfor %}"""
        
        generation_templates = {
            "00_systematic_analytical": template_with_variables
        }
        
        # Test the function
        prompts = generate_prompts(test_tasks, test_task_concepts, generation_templates, concept_contents)
        
        prompt = list(prompts.values())[0]
        
        # Verify template variables were rendered
        assert "Concept count: 2" in prompt, "Concepts list not available to template"
        
        # Verify concept.content was rendered successfully
        # (should contain actual description text, not just concept IDs)
        lines = prompt.split('\n')
        concept_lines = [line for line in lines if line.startswith('- ')]
        assert len(concept_lines) == 2, "Expected 2 concept description lines"
        
        for line in concept_lines:
            # Each line should have format "- concept_name: description..."
            assert ':' in line, f"Concept line missing description: {line}"
            description_part = line.split(':', 1)[1].strip()
            assert len(description_part) > 0, f"Empty description in line: {line}"