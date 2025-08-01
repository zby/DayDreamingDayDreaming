# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python experiment that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a focused combinatorial testing approach. The system tests k_max-sized concept combinations to elicit the Day-Dreaming idea from offline LLMs using automated LLM-based evaluation.

## Core Modules and File Structure

### Core Modules and Classes

1. **concept.py** - Core `Concept` dataclass with concept_id, name, and hierarchical descriptions (sentence, paragraph, article)
2. **concept_db.py** - `ConceptDB` registry for batch retrieval and combination iteration with ID-based lookups
3. **model_client.py** - Simple LLM interface for content generation and evaluation

### Kedro Pipeline Architecture

The project uses Kedro for data pipeline orchestration, with node functions in:
- **nodes.py** - Pure pipeline node functions for task generation, prompt creation, LLM querying, and evaluation

### File Organization

```
src/daydreaming_experiment/
├── utils/
│   ├── concept.py              # Core Concept dataclass with concept_id
│   ├── concept_db.py           # ConceptDB registry with ID-based access
│   └── model_client.py         # Simple LLM interface
├── pipelines/daydreaming/
│   ├── nodes.py                # Pipeline node functions
│   ├── pipeline.py             # Kedro pipeline definition
│   └── test_nodes.py           # Unit tests for pipeline nodes
└── legacy/                     # Legacy CLI runners (archived)

data/
├── 01_raw/                     # External inputs only
│   ├── concepts/
│   │   ├── day_dreaming_concepts.json  # Concept database with IDs and names
│   │   └── articles/           # External article files
│   ├── generation_templates/   # Jinja2 prompt templates
│   │   ├── 00_systematic_analytical.txt
│   │   ├── 01_creative_synthesis.txt
│   │   ├── 02_problem_solving.txt
│   │   ├── 03_research_discovery.txt
│   │   └── 04_application_implementation.txt
│   ├── evaluation_templates/   # Evaluation prompt templates
│   ├── generation_models.csv   # Available generation models with active selection
│   └── evaluation_models.csv   # Available evaluation models with active selection
├── 02_tasks/                   # Generated task definitions (hierarchical structure)
│   ├── concept_combinations.csv        # Concept combination definitions (combo_id, description, num_concepts)
│   ├── concept_combo_relationships.csv # Concept-combo relationships (combo_id, concept_id, position)
│   ├── generation_tasks.csv            # Generation tasks (generation_task_id, combo_id, template, model)
│   ├── evaluation_tasks.csv            # Evaluation tasks (evaluation_task_id, generation_task_id, template, model)
│   └── concept_contents/               # Individual concept content files (by concept_id)
├── 03_generation/              # LLM generation results
│   ├── generation_prompts/     # Prompts sent to generator LLM
│   └── generation_responses/   # Raw generator responses
├── 04_evaluation/              # LLM evaluation results
│   ├── evaluation_prompts/     # Prompts sent to evaluator LLM
│   └── evaluation_responses/   # Raw evaluator responses
├── 05_parsing/                 # Parsed evaluation scores
│   └── parsed_scores.csv       # Extracted scores with metadata
├── 06_summary/                 # Final aggregated results
│   └── final_results_summary.csv
└── 07_reporting/               # Error logs and reporting
    ├── generation_failures.csv
    ├── evaluation_failures.csv
    └── parsing_failures.csv

conf/base/
├── catalog.yml                 # Kedro data catalog configuration
└── parameters.yml              # Pipeline parameters

tests/                          # Integration tests with real data
└── test_working_integration.py # Real data integration tests
```

# Package Management

This project uses **uv** for fast Python package management and dependency resolution.

## Common Development Commands

### Installation
```bash
# Install the project and all dependencies
uv sync

# Install development dependencies
uv sync --dev
```

### Running Tests
```bash
# Run all tests
uv run pytest

# Run only unit tests (fast, isolated)
uv run pytest daydreaming_experiment/

# Run only integration tests (data-dependent)
uv run pytest tests/

# Run with coverage report
uv run pytest --cov=daydreaming_experiment
```

### Code Formatting
```bash
# Format code with black
uv run black .

# Check for style issues with flake8
uv run flake8
```

## Testing Structure

### Overview
This project follows a clear separation between unit tests and integration tests to ensure fast, reliable testing with proper isolation.

### Test Categories and Placement

#### Unit Tests (Fast, Isolated)
- **Location**: Colocated with the modules they test (e.g., `test_concept.py` next to `concept.py`)
- **Purpose**: Test individual functions/classes in isolation
- **Data Access**: **MUST NOT** access files in the `data/` directory
- **Dependencies**: Use mocking for external dependencies (APIs, file systems, etc.)
- **Performance**: Should run quickly (<1 second per test)
- **Examples**: `daydreaming_experiment/utils/test_concept.py`, `daydreaming_experiment/pipelines/daydreaming/test_nodes.py`

#### Integration Tests (Component Interaction)
- **Location**: `tests/` directory only
- **Purpose**: Test component interactions and workflows with real data
- **Data Access**: **CAN** read from `data/` directory when testing data-dependent functionality
- **Data Requirements**: **MUST FAIL** if required data files are missing (no graceful skipping)
- **API Restrictions**: **MUST NOT** make real API calls (use proper mocking)
- **Examples**: `tests/test_working_integration.py`

### Running Tests

```bash
# Run only unit tests (fast, no data dependencies)
uv run pytest daydreaming_experiment/

# Run only integration tests (may require data files)
uv run pytest tests/

# Run all tests
uv run pytest

# Run tests with coverage report
uv run pytest --cov=daydreaming_experiment

# Run tests in parallel for speed
uv run pytest -n auto

# Run specific test file
uv run pytest daydreaming_experiment/utils/test_concept.py

# Run specific test method
uv run pytest daydreaming_experiment/pipelines/daydreaming/test_nodes.py::TestCreateTaskList::test_create_task_list_with_real_concepts
```

### Testing Best Practices

#### For Unit Tests:
- Use dependency injection to make components testable
- Mock external dependencies with `unittest.mock` or `pytest-mock`
- Test edge cases and error conditions
- Keep tests focused on a single behavior
- Use descriptive test names that explain the expected behavior

```python
# Good unit test example
def test_concept_get_description_with_fallback():
    """Should return paragraph when sentence is missing but paragraph exists."""
    concept = Concept(
        name="test",
        descriptions={"paragraph": "Test paragraph", "article": "Test article"}
    )
    
    # Should fall back to paragraph level
    assert concept.get_description("sentence", strict=False) == "Test paragraph"
```

#### For Integration Tests:
- **Fail fast** if required data files are missing (do not skip)
- Test realistic workflows end-to-end
- Use real data files and validate their expected structure
- Mock API calls even in integration tests

```python
# Good integration test example
def test_concept_database_loading():
    """Test loading real concept database - fails if data missing."""
    # Should fail with clear error if file doesn't exist
    concept_db = ConceptDB.load("data/concepts/day_dreaming_concepts.json")
    concepts = concept_db.get_concepts()
    assert len(concepts) > 0
    
    # Verify structure without depending on specific content
    for concept in concepts:
        assert hasattr(concept, 'name')
        assert hasattr(concept, 'descriptions')
```

#### Naming Conventions:
- Test files: `test_*.py`
- Test classes: `TestClassName`
- Test methods: `test_method_name_condition_expected_result`
- Use underscores for readability in test names

#### Mock Usage:
- Mock external services (APIs, file systems when not testing I/O)
- Use `@patch` decorator for clean mocking
- Inject mocks through dependency injection when possible
- Verify mock calls when testing interaction behavior

### Test Framework Configuration
- **Framework**: pytest (configured in `pytest.ini`)
- **Coverage**: pytest-cov for coverage reporting
- **Mocking**: unittest.mock (built-in) and pytest-mock
- **Fixtures**: Create reusable test data in `conftest.py` files
- **Parallel Execution**: pytest-xdist for faster test runs

## Design Preferences

**Dependency Injection**: This project uses dependency injection patterns for better testability and modularity. Dependencies should be passed as constructor parameters rather than created internally.

Examples:
- `SimpleModelClient` can be injected for testing without requiring real API calls
- `ConceptDB` can be provided pre-loaded for experiment runs
- Tests can easily inject mock dependencies without complex patching

**Jinja2-based Templates**: Prompt templates are stored as separate text files in `data/01_raw/generation_templates/` using Jinja2 templating syntax for powerful and flexible prompt generation. Templates are automatically loaded as partitioned datasets and must reference the `concepts` variable.

Template System Features:
- **Clean concept access**: `{% for concept in concepts %}**{{ concept.name }}**: {{ concept.content }}{% endfor %}`
- **Dictionary-based objects**: Templates receive concept dictionaries with `name`, `concept_id`, and `content` keys
- **Human-readable names**: Templates display friendly names like "Default Mode Network" instead of IDs
- **Rich formatting**: Bold concept names, structured layouts, conditional logic
- **Extensible**: Easy to add new templates with complex logic and formatting

Template Structure:
Each template receives a `concepts` list where each concept is a dictionary with:
- `concept.name` - Human-readable name (e.g., "Dearth of AI-driven Discoveries")
- `concept.concept_id` - File-safe identifier (e.g., "dearth-ai-discoveries")  
- `concept.content` - Paragraph-level description content

Template Types:
- `00_systematic_analytical.txt` - Structured step-by-step analysis
- `01_creative_synthesis.txt` - Open-ended imaginative exploration  
- `02_problem_solving.txt` - Focus on solving challenges
- `03_research_discovery.txt` - Academic research orientation
- `04_application_implementation.txt` - Practical real-world focus

## Dependencies

Main dependencies include:
- Standard library modules for core functionality
- jinja2 for advanced template processing
- pytest for testing
- black and flake8 for code formatting
- LLM API clients (we can start with openrouter)

## Development Guidelines

- Never add defensive coding when not explicitly asked for it, the code should fail early
- Focus on simplicity

## Coding Guidelines

- **Formatting Changes**: 
  - Formatting changes (e.g., the result of running `black`) should be committed separately from functional changes
  - We should only run black when we have no uncommitted changes

## Search Strategy

The experiment supports multiple search strategies for testing concept combinations. The current active strategy is documented in:

📄 **[Current Search Strategy](data/current_search_strategy.md)**

This modular approach allows for easy strategy experimentation and comparison without frequent documentation updates.

## Usage Workflow

### Kedro Pipeline Execution

```bash
# Run the complete daydreaming pipeline
uv run kedro run

# Run specific pipeline nodes
uv run kedro run --node create_initial_tasks
uv run kedro run --node generate_prompts_node

# Run pipeline with parameters
uv run kedro run --params k_max:3

# Visualize the pipeline
uv run kedro viz
```

### Data Layer Workflow

The pipeline follows a clear data processing workflow:

1. **Raw Data (01_raw/)**: External inputs only
   - Concept database with IDs and human-readable names
   - Jinja2 templates for generation and evaluation

2. **Task Generation (02_tasks/)**: 
   - Generate all k_max-combinations of concepts as concept_combinations
   - Create hierarchical task structure: combinations → generation tasks → evaluation tasks
   - Active model selection from CSV files with `active` column
   - Generate individual concept content files by concept_id

3. **Generation (03_generation/)**:
   - Render prompts using templates and concept combinations
   - Query generator LLM for responses

4. **Evaluation (04_evaluation/)**:
   - Generate evaluation prompts from responses
   - Query evaluator LLM for scores

5. **Parsing & Summary (05_parsing/, 06_summary/)**:
   - Parse evaluation responses to extract scores
   - Generate final aggregated results

### Legacy CLI Support (Archived)
```bash
# Note: Legacy CLI runners are archived but available in src/legacy/
# Use Kedro pipeline for new experiments

# Legacy experiment runner (archived)
uv run python -m daydreaming_experiment.experiment_runner \
    --k-max 4 --level paragraph --generator-model gpt-4

# Legacy evaluation runner (archived)  
uv run python -m daydreaming_experiment.evaluation_runner \
    experiments/experiment_20250728_143022
```