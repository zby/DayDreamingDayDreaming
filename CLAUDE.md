# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python experiment that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a focused combinatorial testing approach. The system tests k_max-sized concept combinations to elicit the Day-Dreaming idea from offline LLMs using automated LLM-based evaluation.

## Core Modules and File Structure

### Core Modules and Classes

1. **concept.py** - Core `Concept` dataclass with three granularity levels (sentence, paragraph, article)
2. **concept_db.py** - `ConceptDB` registry for batch retrieval and combination iteration
3. **prompt_factory.py** - `PromptFactory` for template-based prompt generation from concept combinations
4. **experiment_runner.py** - CLI experiment execution with combinatorial testing and automated evaluation
5. **model_client.py** - Simple LLM interface for content generation and evaluation

### File Organization

```
daydreaming_experiment/
├── concept.py                    # Core Concept dataclass
├── concept_db.py                 # ConceptDB registry and I/O
├── prompt_factory.py             # PromptFactory for template-based generation  
├── experiment_runner.py          # CLI experiment execution
├── model_client.py              # Simple LLM interface
└── results_analysis.py          # Post-experiment analysis tools

data/
├── concepts/                     # Concept database
│   ├── day_dreaming_concepts.json            # Manifest
│   └── articles/               # Article files
├── templates/                    # Prompt templates
│   ├── 00_systematic_analytical.txt
│   ├── 01_creative_synthesis.txt
│   ├── 02_problem_solving.txt
│   ├── 03_research_discovery.txt
│   └── 04_application_implementation.txt
└── experiments/                # Experiment results
    └── experiment_YYYYMMDD_HHMMSS/
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
- **Examples**: `daydreaming_experiment/test_concept.py`, `daydreaming_experiment/test_model_client.py`

#### Integration Tests (Component Interaction)
- **Location**: `tests/` directory only
- **Purpose**: Test component interactions and workflows with real data
- **Data Access**: **CAN** read from `data/` directory when testing data-dependent functionality
- **Data Requirements**: **MUST FAIL** if required data files are missing (no graceful skipping)
- **API Restrictions**: **MUST NOT** make real API calls (use proper mocking)
- **Examples**: `tests/test_integration_data_dependent.py`, `tests/test_integration_prompt_iterator.py`

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
uv run pytest daydreaming_experiment/test_concept.py

# Run specific test method
uv run pytest daydreaming_experiment/test_concept.py::TestConcept::test_concept_creation
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

**Jinja2-based Templates**: Prompt templates are stored as separate text files in `data/templates/` using Jinja2 templating syntax for powerful and flexible prompt generation. Templates are automatically loaded by filename order and must reference the `concepts` variable.

Template System Features:
- **Dynamic concept access**: `{% for concept in concepts %}{{ concept.name }}: {{ concept.__getattribute__(level) }}{% endfor %}`
- **Level-agnostic**: Templates work with any content level (sentence, paragraph, article)
- **Rich formatting**: Bold concept names, structured layouts, conditional logic
- **Extensible**: Easy to add new templates with complex logic and formatting

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

### Run Experiment
```bash
# Run complete experiment with automated evaluation
uv run python -m daydreaming_experiment.experiment_runner \
    --k-max 4 \
    --level paragraph \
    --generator-model gpt-4 \
    --evaluator-model gpt-4 \
    --output experiments/my_experiment

# Limit number of prompts tested
uv run python -m daydreaming_experiment.experiment_runner \
    --k-max 4 \
    --max-prompts 50 \
    --level paragraph \
    --generator-model gpt-4 \
    --evaluator-model gpt-4
```

### Analysis
```bash
# Analyze results
uv run python -m daydreaming_experiment.results_analysis \
    experiments/experiment_20250728_143022
```