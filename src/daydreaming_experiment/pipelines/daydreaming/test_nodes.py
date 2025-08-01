"""Integration tests for pipeline node functions.

These tests use real data files and are designed to fail if data is missing,
following the project's integration testing guidelines.
"""

import pytest
import pandas as pd
from pathlib import Path

from daydreaming_experiment.pipelines.daydreaming.nodes import (
    load_and_prepare_concepts,
    generate_concept_combinations, 
    create_all_tasks,
    create_task_list,
    generate_prompts
)


def create_test_model_data():
    """Create test model DataFrames for testing."""
    generation_models = pd.DataFrame({
        'model_name': ['deepseek/deepseek-r1:free', 'google/gemma-3-27b-it:free'],
        'model_short': ['deepseek-r1', 'gemma-3-27b'],
        'specialization': ['reasoning', 'instruction_following'],
        'active': [True, False]
    })
    
    evaluation_models = pd.DataFrame({
        'model_name': ['deepseek/deepseek-r1:free', 'qwen/qwq-32b:free'],
        'model_short': ['deepseek-r1', 'qwq-32b'],
        'evaluation_strength': ['logical_reasoning', 'question_answering'],
        'active': [True, False]
    })
    
    generation_templates = {
        '00_systematic_analytical': lambda: 'Test template {% for concept in concepts %}{{ concept.name }}: {{ concept.content }}{% endfor %}'
    }
    
    evaluation_templates = {
        'creativity_metrics': lambda: 'Evaluate: {{ response }}'
    }
    
    return generation_models, evaluation_models, generation_templates, evaluation_templates


def create_test_concept_data():
    """Create test concept data for unit testing."""
    # Create test concepts metadata
    concepts_metadata = pd.DataFrame({
        'concept_id': ['test-concept-1', 'test-concept-2', 'test-concept-3', 'test-concept-4', 'test-concept-5', 'test-concept-6'],
        'name': ['Test Concept 1', 'Test Concept 2', 'Test Concept 3', 'Test Concept 4', 'Test Concept 5', 'Test Concept 6']
    })
    
    # Create test concept descriptions (mock callables like partitioned datasets)
    concept_descriptions_sentence = {}
    concept_descriptions_paragraph = {}
    concept_descriptions_article = {}
    
    for i in range(1, 7):
        concept_id = f'test-concept-{i}'
        concept_descriptions_sentence[concept_id] = lambda i=i: f'Sentence description for test concept {i}'
        concept_descriptions_paragraph[concept_id] = lambda i=i: f'Paragraph description for test concept {i}. This is more detailed content.'
        concept_descriptions_article[concept_id] = lambda i=i: f'Article description for test concept {i}. This is the most detailed content available.'
    
    return concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article


class TestLoadAndPrepareConcepts:
    """Unit tests for the load_and_prepare_concepts node function."""
    
    def test_load_and_prepare_concepts_basic(self):
        """Test load_and_prepare_concepts with basic concept data."""
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = create_test_concept_data()
        parameters = {'description_level': 'paragraph'}
        
        concepts_list, concept_contents = load_and_prepare_concepts(
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        # Verify concepts_list structure
        assert isinstance(concepts_list, list)
        assert len(concepts_list) == 6  # We created 6 test concepts
        for concept in concepts_list:
            assert isinstance(concept, dict)
            assert 'concept_id' in concept
            assert 'name' in concept
            assert concept['concept_id'].startswith('test-concept-')
            assert concept['name'].startswith('Test Concept')
        
        # Verify concept_contents structure
        assert isinstance(concept_contents, dict)
        assert len(concept_contents) == 6
        for concept_id, content in concept_contents.items():
            assert isinstance(concept_id, str)
            assert isinstance(content, str)
            assert len(content.strip()) > 0
            assert 'Paragraph description' in content  # Should use paragraph level
    
    def test_load_and_prepare_concepts_fallback_strategy(self):
        """Test fallback strategy when preferred level is not available."""
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = create_test_concept_data()
        
        # Test with missing paragraph level (should fall back to other levels)
        partial_paragraph = {'test-concept-1': lambda: 'Only concept 1 has paragraph'}
        parameters = {'description_level': 'paragraph'}
        
        concepts_list, concept_contents = load_and_prepare_concepts(
            concepts_metadata, concept_descriptions_sentence, partial_paragraph, concept_descriptions_article, parameters
        )
        
        # Should still get all concepts with fallback content
        assert len(concept_contents) == 6
        assert 'Only concept 1 has paragraph' in concept_contents['test-concept-1']
        # Other concepts should fall back to sentence level
        assert 'Sentence description' in concept_contents['test-concept-2']
    
    def test_load_and_prepare_concepts_different_levels(self):
        """Test different description levels."""
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = create_test_concept_data()
        
        # Test sentence level
        parameters = {'description_level': 'sentence'}
        concepts_list, concept_contents = load_and_prepare_concepts(
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        for content in concept_contents.values():
            assert 'Sentence description' in content
        
        # Test article level
        parameters = {'description_level': 'article'}
        concepts_list, concept_contents = load_and_prepare_concepts(
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        for content in concept_contents.values():
            assert 'Article description' in content


class TestGenerateConceptCombinations:
    """Unit tests for the generate_concept_combinations node function."""
    
    def test_generate_concept_combinations_k_max_2(self):
        """Test generate_concept_combinations with k_max=2."""
        concepts_list = [
            {'concept_id': 'test-concept-1', 'name': 'Test Concept 1'},
            {'concept_id': 'test-concept-2', 'name': 'Test Concept 2'},
            {'concept_id': 'test-concept-3', 'name': 'Test Concept 3'}
        ]
        parameters = {'k_max': 2}
        
        concept_combinations_df, concept_combo_relationships_df = generate_concept_combinations(
            concepts_list, parameters
        )
        
        # Should have 3 choose 2 = 3 combinations
        assert len(concept_combinations_df) == 3
        assert len(concept_combo_relationships_df) == 6  # 3 combinations * 2 concepts each
        
        # Verify structure
        required_combo_columns = ['combo_id', 'description', 'num_concepts', 'created_date']
        for col in required_combo_columns:
            assert col in concept_combinations_df.columns
        
        required_rel_columns = ['combo_id', 'concept_id', 'position']
        for col in required_rel_columns:
            assert col in concept_combo_relationships_df.columns
        
        # Verify each combo has exactly 2 concepts
        for combo_id in concept_combinations_df['combo_id']:
            combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
            assert len(combo_concepts) == 2
            positions = sorted(combo_concepts['position'].tolist())
            assert positions == [1, 2]
    
    def test_generate_concept_combinations_k_max_3(self):
        """Test generate_concept_combinations with k_max=3."""
        concepts_list = [
            {'concept_id': f'test-concept-{i}', 'name': f'Test Concept {i}'}
            for i in range(1, 7)  # 6 concepts
        ]
        parameters = {'k_max': 3}
        
        concept_combinations_df, concept_combo_relationships_df = generate_concept_combinations(
            concepts_list, parameters
        )
        
        # Should have 6 choose 3 = 20 combinations
        assert len(concept_combinations_df) == 20
        assert len(concept_combo_relationships_df) == 60  # 20 combinations * 3 concepts each
        
        # Verify each combo has exactly 3 concepts
        for combo_id in concept_combinations_df['combo_id']:
            combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
            assert len(combo_concepts) == 3
            positions = sorted(combo_concepts['position'].tolist())
            assert positions == [1, 2, 3]
    
    def test_generate_concept_combinations_description_format(self):
        """Test that combination descriptions are properly formatted."""
        concepts_list = [
            {'concept_id': 'test-concept-1', 'name': 'Alpha'},
            {'concept_id': 'test-concept-2', 'name': 'Beta'}
        ]
        parameters = {'k_max': 2}
        
        concept_combinations_df, _ = generate_concept_combinations(concepts_list, parameters)
        
        # Should have one combination with description "Alpha + Beta"
        assert len(concept_combinations_df) == 1
        description = concept_combinations_df.iloc[0]['description']
        assert description == 'Alpha + Beta'


class TestCreateAllTasks:
    """Unit tests for the create_all_tasks node function."""
    
    def test_create_all_tasks_basic(self):
        """Test create_all_tasks with basic model and combo data."""
        generation_models, evaluation_models, _, _ = create_test_model_data()
        
        # Create test concept combinations
        concept_combinations_df = pd.DataFrame([
            {'combo_id': 'combo_001', 'description': 'Test + Combo', 'num_concepts': 2, 'created_date': '2024-01-01'},
            {'combo_id': 'combo_002', 'description': 'Another + Test', 'num_concepts': 2, 'created_date': '2024-01-01'}
        ])
        
        parameters = {
            'current_gen_template': '00_systematic_analytical',
            'current_eval_template': 'creativity_metrics'
        }
        
        generation_tasks_df, evaluation_tasks_df = create_all_tasks(
            concept_combinations_df, generation_models, evaluation_models, parameters
        )
        
        # Should have tasks for active models only
        # 2 combos * 1 active gen model = 2 generation tasks
        # 2 gen tasks * 1 active eval model = 2 evaluation tasks
        assert len(generation_tasks_df) == 2
        assert len(evaluation_tasks_df) == 2
        
        # Verify structure
        required_gen_columns = ['generation_task_id', 'combo_id', 'generation_template', 'generation_model', 'generation_model_short']
        for col in required_gen_columns:
            assert col in generation_tasks_df.columns
        
        required_eval_columns = ['evaluation_task_id', 'generation_task_id', 'evaluation_template', 'evaluation_model', 'evaluation_model_short']
        for col in required_eval_columns:
            assert col in evaluation_tasks_df.columns
        
        # Verify only active models are used
        active_gen_models = generation_models[generation_models['active']]['model_name'].tolist()
        active_eval_models = evaluation_models[evaluation_models['active']]['model_name'].tolist()
        
        for model in generation_tasks_df['generation_model'].unique():
            assert model in active_gen_models
        
        for model in evaluation_tasks_df['evaluation_model'].unique():
            assert model in active_eval_models
    
    def test_create_all_tasks_no_active_generation_models(self):
        """Test that create_all_tasks fails when no generation models are active."""
        generation_models, evaluation_models, _, _ = create_test_model_data()
        generation_models['active'] = False  # Make all generation models inactive
        
        concept_combinations_df = pd.DataFrame([
            {'combo_id': 'combo_001', 'description': 'Test + Combo', 'num_concepts': 2, 'created_date': '2024-01-01'}
        ])
        
        parameters = {
            'current_gen_template': '00_systematic_analytical',
            'current_eval_template': 'creativity_metrics'
        }
        
        with pytest.raises(ValueError, match="No active generation models found"):
            create_all_tasks(concept_combinations_df, generation_models, evaluation_models, parameters)
    
    def test_create_all_tasks_no_active_evaluation_models(self):
        """Test that create_all_tasks fails when no evaluation models are active."""
        generation_models, evaluation_models, _, _ = create_test_model_data()
        evaluation_models['active'] = False  # Make all evaluation models inactive
        
        concept_combinations_df = pd.DataFrame([
            {'combo_id': 'combo_001', 'description': 'Test + Combo', 'num_concepts': 2, 'created_date': '2024-01-01'}
        ])
        
        parameters = {
            'current_gen_template': '00_systematic_analytical',
            'current_eval_template': 'creativity_metrics'
        }
        
        with pytest.raises(ValueError, match="No active evaluation models found"):
            create_all_tasks(concept_combinations_df, generation_models, evaluation_models, parameters)


class TestCreateTaskList:
    """Integration tests for the create_task_list node function."""
    
    def test_create_task_list_with_real_concepts(self):
        """Test create_task_list with test concept data."""
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = create_test_concept_data()
        parameters = {'k_max': 2, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
        
        # Test with concept datasets
        (concept_combinations_df, concept_combo_relationships_df, 
         generation_tasks_df, evaluation_tasks_df, concept_contents) = create_task_list(
            generation_models, evaluation_models, generation_templates, evaluation_templates,
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        # Verify concept_combinations_df structure
        assert isinstance(concept_combinations_df, pd.DataFrame)
        assert not concept_combinations_df.empty
        required_combinations_columns = ['combo_id', 'description', 'num_concepts', 'created_date']
        for col in required_combinations_columns:
            assert col in concept_combinations_df.columns, f"Missing required column: {col}"
        
        # Verify concept_combo_relationships_df structure
        assert isinstance(concept_combo_relationships_df, pd.DataFrame)
        assert not concept_combo_relationships_df.empty
        required_relationships_columns = ['combo_id', 'concept_id', 'position']
        for col in required_relationships_columns:
            assert col in concept_combo_relationships_df.columns, f"Missing required column: {col}"
        
        # Verify generation_tasks_df structure
        assert isinstance(generation_tasks_df, pd.DataFrame)
        assert not generation_tasks_df.empty
        required_gen_columns = ['generation_task_id', 'combo_id', 'generation_template', 'generation_model', 'generation_model_short']
        for col in required_gen_columns:
            assert col in generation_tasks_df.columns, f"Missing required column: {col}"
        
        # Verify evaluation_tasks_df structure
        assert isinstance(evaluation_tasks_df, pd.DataFrame)
        assert not evaluation_tasks_df.empty
        required_eval_columns = ['evaluation_task_id', 'generation_task_id', 'evaluation_template', 'evaluation_model', 'evaluation_model_short']
        for col in required_eval_columns:
            assert col in evaluation_tasks_df.columns, f"Missing required column: {col}"
        
        # Verify concept_contents structure
        assert isinstance(concept_contents, dict)
        assert len(concept_contents) > 0
        for concept_id, content in concept_contents.items():
            assert isinstance(concept_id, str), f"Concept ID should be string: {concept_id}"
            assert isinstance(content, str), f"Concept content should be string: {content}"
            assert len(content.strip()) > 0, f"Empty concept content for {concept_id}"
        
        # Verify combo_id format
        for combo_id in concept_combinations_df['combo_id']:
            assert combo_id.startswith('combo_'), f"Invalid combo_id format: {combo_id}"
        
        # Verify task-concept relationships for k_max=2
        for combo_id in concept_combinations_df['combo_id']:
            combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
            assert len(combo_concepts) == 2, f"Expected 2 concepts for k_max=2, got {len(combo_concepts)}"
            
            # Verify concept positioning
            positions = sorted(combo_concepts['position'].tolist())
            assert positions == [1, 2], f"Invalid concept positioning: {positions}"
            
            # Verify concepts are different
            concepts = combo_concepts['concept_id'].tolist()
            assert len(set(concepts)) == len(concepts), f"Duplicate concepts in combo: {concepts}"
    
    def test_create_task_list_with_k_max_3(self):
        """Test create_task_list with k_max=3 generates 3-concept combinations."""
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = create_test_concept_data()
        parameters = {'k_max': 3, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
        
        (concept_combinations_df, concept_combo_relationships_df, 
         generation_tasks_df, evaluation_tasks_df, concept_contents) = create_task_list(
            generation_models, evaluation_models, generation_templates, evaluation_templates,
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        # Verify basic structure
        assert isinstance(concept_combinations_df, pd.DataFrame)
        assert not concept_combinations_df.empty
        assert isinstance(concept_combo_relationships_df, pd.DataFrame)
        assert not concept_combo_relationships_df.empty
        
        # Verify each combo has exactly 3 concepts
        for combo_id in concept_combinations_df['combo_id']:
            combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
            assert len(combo_concepts) == 3, f"Expected 3 concepts for k_max=3, got {len(combo_concepts)}"
            
            # Verify concept positioning for k_max=3
            positions = sorted(combo_concepts['position'].tolist())
            assert positions == [1, 2, 3], f"Invalid concept positioning: {positions}"
            
            # Verify all three concepts are different
            concepts = combo_concepts['concept_id'].tolist()
            assert len(set(concepts)) == 3, f"Duplicate concepts in combo: {concepts}"
    
    def test_create_task_list_active_model_selection(self):
        """Test that create_task_list only uses active models."""
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = create_test_concept_data()
        parameters = {'k_max': 2, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
        
        (concept_combinations_df, concept_combo_relationships_df, 
         generation_tasks_df, evaluation_tasks_df, concept_contents) = create_task_list(
            generation_models, evaluation_models, generation_templates, evaluation_templates,
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        # Should only have tasks for active models (deepseek-r1 for both gen and eval)
        active_gen_models = generation_models[generation_models['active']]['model_name'].tolist()
        active_eval_models = evaluation_models[evaluation_models['active']]['model_name'].tolist()
        
        # Verify generation tasks only use active models
        gen_models_used = generation_tasks_df['generation_model'].unique()
        for model in gen_models_used:
            assert model in active_gen_models, f"Inactive generation model used: {model}"
        
        # Verify evaluation tasks only use active models
        eval_models_used = evaluation_tasks_df['evaluation_model'].unique()
        for model in eval_models_used:
            assert model in active_eval_models, f"Inactive evaluation model used: {model}"
    
    def test_create_task_list_no_active_models_error(self):
        """Test that create_task_list fails when no models are active."""
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        
        # Make all models inactive
        generation_models['active'] = False
        evaluation_models['active'] = False
        
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = create_test_concept_data()
        parameters = {'k_max': 2, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
        
        # Should raise ValueError when no active models found
        with pytest.raises(ValueError, match="No active generation models found"):
            create_task_list(generation_models, evaluation_models, generation_templates, evaluation_templates,
                           concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters)


class TestGeneratePrompts:
    """Integration tests for the generate_prompts node function."""
    
    def test_generate_prompts_with_real_data(self):
        """Test generate_prompts with test concept data and templates."""
        # First create some tasks using test data
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = create_test_concept_data()
        parameters = {'k_max': 2, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
        
        (concept_combinations_df, concept_combo_relationships_df, 
         generation_tasks_df, evaluation_tasks_df, concept_contents) = create_task_list(
            generation_models, evaluation_models, generation_templates, evaluation_templates,
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        # Limit to just a few tasks for testing
        test_gen_tasks = generation_tasks_df.head(3)
        
        # Test the function
        prompts = generate_prompts(
            test_gen_tasks, concept_combinations_df, concept_combo_relationships_df,
            concept_contents, generation_templates, concepts_metadata
        )
        
        # Verify basic structure
        assert isinstance(prompts, dict)
        assert len(prompts) == len(test_gen_tasks)
        
        # Verify all task generation_task_ids have prompts
        for generation_task_id in test_gen_tasks['generation_task_id']:
            assert generation_task_id in prompts, f"Missing prompt for task {generation_task_id}"
            assert isinstance(prompts[generation_task_id], str), f"Prompt for {generation_task_id} is not a string"
            assert len(prompts[generation_task_id].strip()) > 0, f"Empty prompt for {generation_task_id}"
        
        # Verify prompts contain concept content
        for generation_task_id, prompt in prompts.items():
            # Get combo for this task
            task_row = test_gen_tasks[test_gen_tasks['generation_task_id'] == generation_task_id].iloc[0]
            combo_id = task_row['combo_id']
            
            # Get concepts for this combo
            combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
            
            # Verify each concept content appears in the prompt (not just names)
            for _, concept_row in combo_concepts.iterrows():
                concept_id = concept_row['concept_id']
                concept_content = concept_contents[concept_id]
                # Content should appear in prompt (templates render content, not just names)
                assert concept_content in prompt, f"Concept content for '{concept_id}' not found in prompt"
        
        # Verify prompts are not identical (different concept combinations should yield different prompts)
        unique_prompts = set(prompts.values())
        assert len(unique_prompts) == len(prompts), "Found duplicate prompts for different tasks"