# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python experiment that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a simplified combinatorial testing approach. The system finds the minimal set of concepts that can elicit the Day-Dreaming idea from offline LLMs using automated LLM-based evaluation.

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
├── concepts/                     # New concept database
│   ├── day_dreaming_concepts.json            # Manifest
│   └── articles/               # Article files
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

# Run tests for a specific module
uv run pytest tests/
```

### Code Formatting
```bash
# Format code with black
uv run black .

# Check for style issues with flake8
uv run flake8
```

## Testing Structure

- Unittests should be colocated with the modules they are verifying
- Integration tests should be in the tests directory
- All tests use pytest as the testing framework
- Test fixtures are created using `create_test_fixtures.py` for consistent test data

## Design Preferences

**Dependency Injection**: This project uses dependency injection patterns for better testability and modularity. Dependencies should be passed as constructor parameters rather than created internally.

Examples:
- `SimpleModelClient` can be injected for testing without requiring real API calls
- `ConceptDB` can be provided pre-loaded for experiment runs
- Tests can easily inject mock dependencies without complex patching

## Dependencies

Main dependencies include:
- Standard library modules for core functionality
- pytest for testing
- black and flake8 for code formatting
- LLM API clients (we can start with openrouter)

## Development Guidelines

- Never add defensive coding when not explicitly asked for it, the code should fail early
- Focus on simplicity

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
```

### Analysis
```bash
# Analyze results
uv run python -m daydreaming_experiment.results_analysis \
    experiments/experiment_20250728_143022
```
