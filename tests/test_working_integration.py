"""
Integration tests that work with the current Kedro architecture.
These tests verify integration with real concept databases and pipeline nodes.
"""

import pytest
import pandas as pd
from pathlib import Path

# ConceptDB and Concept classes have been replaced with Kedro filesystem approach
from daydreaming_experiment.pipelines.daydreaming.nodes import create_task_list, generate_prompts


def create_test_model_data():
    """Create test model DataFrames for integration testing."""
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


def load_concepts_from_filesystem():
    """Load concepts using the new filesystem structure."""
    # Load metadata
    concepts_metadata = pd.read_csv(CONCEPTS_METADATA_PATH)
    
    # Load descriptions from filesystem
    concept_descriptions_sentence = {}
    concept_descriptions_paragraph = {}
    concept_descriptions_article = {}
    
    for _, row in concepts_metadata.iterrows():
        concept_id = row['concept_id']
        
        # Load sentence descriptions
        sentence_path = CONCEPTS_DESCRIPTIONS_PATH / "sentence" / f"{concept_id}.txt"
        if sentence_path.exists():
            with open(sentence_path, 'r', encoding='utf-8') as f:
                concept_descriptions_sentence[concept_id] = lambda path=sentence_path: path.read_text(encoding='utf-8')
        
        # Load paragraph descriptions
        paragraph_path = CONCEPTS_DESCRIPTIONS_PATH / "paragraph" / f"{concept_id}.txt"
        if paragraph_path.exists():
            with open(paragraph_path, 'r', encoding='utf-8') as f:
                concept_descriptions_paragraph[concept_id] = lambda path=paragraph_path: path.read_text(encoding='utf-8')
        
        # Load article descriptions (if available)
        article_path = CONCEPTS_DESCRIPTIONS_PATH / "article" / f"{concept_id}.txt"
        if article_path.exists():
            with open(article_path, 'r', encoding='utf-8') as f:
                concept_descriptions_article[concept_id] = lambda path=article_path: path.read_text(encoding='utf-8')
    
    return concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article

CONCEPTS_METADATA_PATH = Path(__file__).parent.parent / "data" / "01_raw" / "concepts" / "concepts_metadata.csv"
CONCEPTS_DESCRIPTIONS_PATH = Path(__file__).parent.parent / "data" / "01_raw" / "concepts" / "descriptions"

GEN_TEMPLATES_PATH = Path(__file__).parent.parent / "data" / "01_raw" / "generation_templates"

EVAL_TEMPLATES_PATH = Path(__file__).parent.parent / "data" / "01_raw" / "evaluation_templates"

class TestPipelineIntegration:
    """Integration tests using real concept data and pipeline nodes."""

    @pytest.fixture
    def concepts_data(self):
        """Load real concepts from the filesystem structure."""
        return load_concepts_from_filesystem()

    def test_loads_real_concepts(self, concepts_data):
        """Verify the concept filesystem structure loads real data correctly."""
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = concepts_data
        
        assert len(concepts_metadata) == 6

        expected_names = {
            "Dearth of AI-driven Discoveries",
            "Default Mode Network",
            "Human Creativity and Insight",
            "Combinatorial Search",
            "Generator-Verifier Gap",
            "Economic Innovation Models",
        }

        actual_names = set(concepts_metadata['name'])
        assert actual_names == expected_names

        # Verify all concepts have required content levels
        for _, row in concepts_metadata.iterrows():
            concept_id = row['concept_id']
            concept_name = row['name']
            
            assert (
                concept_id in concept_descriptions_sentence
            ), f"Concept '{concept_name}' missing sentence description"
            assert (
                concept_id in concept_descriptions_paragraph  
            ), f"Concept '{concept_name}' missing paragraph description"

    def test_create_task_list_integration(self):
        """Test create_task_list node with real concept data."""
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = load_concepts_from_filesystem()
        
        # Test different k_max values
        for k_max in [1, 2, 3]:
            parameters = {'k_max': k_max, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
            
            (concept_combinations_df, concept_combo_relationships_df, 
             generation_tasks_df, evaluation_tasks_df, concept_contents) = create_task_list(
                generation_models, evaluation_models, generation_templates, evaluation_templates,
                concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
            )
            
            # Verify structure
            assert isinstance(concept_combinations_df, pd.DataFrame)
            assert isinstance(concept_combo_relationships_df, pd.DataFrame)
            assert isinstance(generation_tasks_df, pd.DataFrame)
            assert isinstance(evaluation_tasks_df, pd.DataFrame)
            assert isinstance(concept_contents, dict)
            assert not concept_combinations_df.empty
            assert not concept_combo_relationships_df.empty
            assert not generation_tasks_df.empty
            assert not evaluation_tasks_df.empty
            assert len(concept_contents) > 0
            
            # Verify each combo has correct number of concepts
            for combo_id in concept_combinations_df['combo_id']:
                combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
                assert len(combo_concepts) == k_max

    def test_prompt_generation_integration(self):
        """Test prompt generation with real data and templates."""
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = load_concepts_from_filesystem()
        parameters = {'k_max': 2, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
        
        # Create tasks using real data
        (concept_combinations_df, concept_combo_relationships_df, 
         generation_tasks_df, evaluation_tasks_df, concept_contents) = create_task_list(
            generation_models, evaluation_models, generation_templates, evaluation_templates,
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        # Take a small sample for testing
        sample_gen_tasks = generation_tasks_df.head(3)
        
        # Create mock generation templates (simulate data catalog behavior)
        def make_template_loader(template_content):
            def loader():
                return template_content
            return loader
        
        generation_templates_real = {
            "00_systematic_analytical": make_template_loader("""Below are several concepts to work with:

{% for concept in concepts %}
**{{ concept.name }}**: {{ concept.content }}
{% endfor %}

Please systematically explore how these concepts might be combined.""")
        }
        
        # Test prompt generation
        prompts = generate_prompts(
            sample_gen_tasks, concept_combinations_df, concept_combo_relationships_df,
            concept_contents, generation_templates_real, concepts_metadata
        )
        
        # Verify results
        assert isinstance(prompts, dict)
        assert len(prompts) == len(sample_gen_tasks)
        
        # Verify prompt content
        for generation_task_id, prompt in prompts.items():
            assert isinstance(prompt, str)
            assert len(prompt) > 100
            assert "concepts" in prompt.lower()
            
            # Get combo_id for this generation task
            gen_task = sample_gen_tasks[sample_gen_tasks['generation_task_id'] == generation_task_id].iloc[0]
            combo_id = gen_task['combo_id']
            
            # Get concepts for this combo
            combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
            
            # Load concept names and verify they appear in prompt  
            concept_name_lookup = dict(zip(concepts_metadata['concept_id'], concepts_metadata['name']))
            
            for _, concept_row in combo_concepts.iterrows():
                concept_id = concept_row['concept_id']
                concept_name = concept_name_lookup[concept_id]
                assert concept_name in prompt

    def test_end_to_end_pipeline_sample(self):
        """Test complete pipeline workflow with real data."""
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = load_concepts_from_filesystem()
        parameters = {'k_max': 2, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
        
        # Step 1: Create tasks using new architecture
        (concept_combinations_df, concept_combo_relationships_df, 
         generation_tasks_df, evaluation_tasks_df, concept_contents) = create_task_list(
            generation_models, evaluation_models, generation_templates, evaluation_templates,
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        # Take small sample of generation tasks
        sample_gen_tasks = generation_tasks_df.head(2)
        
        # Step 2: Generate prompts
        prompts = generate_prompts(
            sample_gen_tasks, concept_combinations_df, concept_combo_relationships_df,
            concept_contents, generation_templates, concepts_metadata
        )
        
        # Verify complete workflow
        assert len(prompts) == 2
        for generation_task_id, prompt in prompts.items():
            assert isinstance(prompt, str)
            assert len(prompt) > 50
            
            # Get combo_id for this generation task
            gen_task = sample_gen_tasks[sample_gen_tasks['generation_task_id'] == generation_task_id].iloc[0]
            combo_id = gen_task['combo_id']
            
            # Verify concepts are properly integrated
            combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
            assert len(combo_concepts) == 2  # k_max=2
            
            # Load concept names for verification
            concept_name_lookup = dict(zip(concepts_metadata['concept_id'], concepts_metadata['name']))
            
            for _, concept_row in combo_concepts.iterrows():
                concept_id = concept_row['concept_id']
                concept_name = concept_name_lookup[concept_id]
                assert concept_name in prompt


class TestEvaluationTemplatesIntegration:
    """Integration tests with actual evaluation template files."""

    def test_evaluation_templates_exist(self):
        """Test that evaluation template files exist in the expected location."""
        eval_templates_dir = EVAL_TEMPLATES_PATH
        assert eval_templates_dir.exists(), f"Evaluation templates directory must exist: {eval_templates_dir}"
        
        # Check for expected template files
        expected_templates = [
            "creativity_metrics.txt",
            "iterative_loops.txt", 
            "scientific_rigor.txt",
        ]
        
        for template_file in expected_templates:
            template_path = eval_templates_dir / template_file
            assert template_path.exists(), f"Expected template file missing: {template_path}"
            
            # Verify template files have content
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                assert len(content) > 0, f"Template file is empty: {template_path}"
                assert "REASONING:" in content, f"Template missing REASONING section: {template_path}"
                assert "SCORE:" in content, f"Template missing SCORE section: {template_path}"

    def test_evaluation_template_structure(self):
        """Test that evaluation templates have the correct structure for pipeline use."""
        eval_templates_dir = EVAL_TEMPLATES_PATH
        
        test_response = "This response discusses novel AI architectures with iterative learning."
        
        from jinja2 import Environment
        
        # Test each template file can be loaded and rendered with Jinja2
        for template_file in eval_templates_dir.glob("*.txt"):
            with open(template_file, 'r', encoding='utf-8') as f:
                template_content = f.read().strip()
                
            # Templates should be valid Jinja2 and render with response
            try:
                env = Environment()
                template = env.from_string(template_content)
                formatted_prompt = template.render(response=test_response)
                
                # Verify the response was inserted
                assert test_response in formatted_prompt
                
                # Verify template structure is preserved
                assert "REASONING:" in formatted_prompt
                assert "SCORE:" in formatted_prompt
                
            except Exception as e:
                pytest.fail(f"Template {template_file.name} failed to render: {e}")


class TestGenerationTemplatesIntegration:
    """Integration tests with actual generation template files."""

    def test_generation_templates_exist(self):
        """Test that generation template files exist in the expected location."""
        gen_templates_dir = GEN_TEMPLATES_PATH
        assert gen_templates_dir.exists(), f"Generation templates directory must exist: {gen_templates_dir}"
        
        # Check for expected template files  
        expected_templates = [
            "00_systematic_analytical.txt",
            "01_creative_synthesis.txt",
            "02_problem_solving.txt",
            "03_research_discovery.txt",
            "04_application_implementation.txt",
        ]
        
        for template_file in expected_templates:
            template_path = gen_templates_dir / template_file
            assert template_path.exists(), f"Expected template file missing: {template_path}"
            
            # Verify template files have content
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                assert len(content) > 0, f"Template file is empty: {template_path}"

    def test_generation_template_rendering(self):
        """Test that generation templates can be rendered with test data."""
        gen_templates_dir = GEN_TEMPLATES_PATH
        
        # Create test concept data
        test_concepts = [
            {
                'concept_id': 'test-concept-1',
                'name': 'Test Concept 1',
                'content': 'This is test content for concept 1.'
            },
            {
                'concept_id': 'test-concept-2', 
                'name': 'Test Concept 2',
                'content': 'This is test content for concept 2.'
            }
        ]
        
        from jinja2 import Environment
        
        # Test each template file can be rendered
        for template_file in gen_templates_dir.glob("*.txt"):
            with open(template_file, 'r', encoding='utf-8') as f:
                template_content = f.read().strip()
                
            # Templates should be valid Jinja2 and render with concepts
            try:
                env = Environment()
                template = env.from_string(template_content)
                rendered = template.render(concepts=test_concepts)
                
                # Verify rendering was successful
                assert isinstance(rendered, str)
                assert len(rendered) > 50  # Should be substantial content
                
                # Verify concept names appear in rendered output
                for concept in test_concepts:
                    assert concept['name'] in rendered, f"Concept name '{concept['name']}' not found in rendered template {template_file.name}"
                    
            except Exception as e:
                pytest.fail(f"Template {template_file.name} failed to render: {e}")


class TestDataDirectoryStructure:
    """Tests that verify the expected data directory structure exists."""

    def test_data_directories_exist(self):
        """Test that expected data directories exist."""
        data_dir = Path("data/01_raw")

        # These directories must exist for the system to work properly
        expected_dirs = ["concepts", "generation_templates", "evaluation_templates"]

        assert data_dir.exists(), "data/01_raw/ directory must exist"

        for dir_name in expected_dirs:
            dir_path = data_dir / dir_name
            assert dir_path.exists(), f"Required data directory must exist: {dir_path}"

    def test_required_files_exist(self):
        """Test that required data files exist."""
        required_files = [CONCEPTS_METADATA_PATH]

        for file_path in required_files:
            path = Path(file_path)
            assert path.exists(), f"Required data file must exist: {file_path}"

        # Basic validation that concept metadata is readable
        concepts_metadata = pd.read_csv(CONCEPTS_METADATA_PATH)
        assert (
            len(concepts_metadata) > 0
        ), "Concept metadata must contain concepts"


class TestWorkflowIntegrationWithRealData:
    """Integration tests that combine multiple components with real data."""

    def test_pipeline_nodes_with_real_concepts_and_templates(self):
        """Test complete pipeline workflow using real concepts and templates."""
        generation_models, evaluation_models, generation_templates, evaluation_templates = create_test_model_data()
        concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article = load_concepts_from_filesystem()
        parameters = {'k_max': 2, 'description_level': 'paragraph', 'current_gen_template': '00_systematic_analytical', 'current_eval_template': 'creativity_metrics'}
        
        # Step 1: Create tasks with real concept data
        (concept_combinations_df, concept_combo_relationships_df, 
         generation_tasks_df, evaluation_tasks_df, concept_contents) = create_task_list(
            generation_models, evaluation_models, generation_templates, evaluation_templates,
            concepts_metadata, concept_descriptions_sentence, concept_descriptions_paragraph, concept_descriptions_article, parameters
        )
        
        # Verify we have sufficient data
        assert len(generation_tasks_df) > 0, "Should generate tasks with real concept data"
        assert len(concept_contents) >= 2, "Need at least 2 concepts for combination testing"
        
        # Take a sample for testing
        sample_gen_tasks = generation_tasks_df.head(5)
        
        # Step 2: Create template loaders that use real template files
        def create_real_template_loader(template_path):
            def loader():
                with open(template_path, 'r', encoding='utf-8') as f:
                    return f.read()
            return loader
        
        # Use actual template file
        template_path = GEN_TEMPLATES_PATH / "00_systematic_analytical.txt"
        assert template_path.exists(), f"Template file must exist: {template_path}"
        
        real_generation_templates = {
            "00_systematic_analytical": create_real_template_loader(template_path)
        }
        
        # Step 3: Generate prompts using real templates
        prompts = generate_prompts(
            sample_gen_tasks, concept_combinations_df, concept_combo_relationships_df,
            concept_contents, real_generation_templates, concepts_metadata
        )
        
        # Verify results
        assert len(prompts) == len(sample_gen_tasks)
        
        # Load concept names for verification
        concept_name_lookup = dict(zip(concepts_metadata['concept_id'], concepts_metadata['name']))
        
        for generation_task_id, prompt in prompts.items():
            # Should generate meaningful prompts
            assert isinstance(prompt, str)
            assert len(prompt) > 100, "Prompts should be substantial"
            
            # Get combo_id for this generation task
            gen_task = sample_gen_tasks[sample_gen_tasks['generation_task_id'] == generation_task_id].iloc[0]
            combo_id = gen_task['combo_id']
            
            # Should contain concept names from the combo
            combo_concepts = concept_combo_relationships_df[concept_combo_relationships_df['combo_id'] == combo_id]
            for _, concept_row in combo_concepts.iterrows():
                concept_id = concept_row['concept_id']
                concept_name = concept_name_lookup[concept_id]
                assert concept_name in prompt, f"Concept '{concept_name}' should appear in prompt"
                
            # Should contain template-specific content
            assert "systematically" in prompt.lower(), "Should contain template-specific language"
