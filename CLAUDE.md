# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python experiment that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a structured prompt DAG (Directed Acyclic Graph) approach.

## Code Architecture

### Core Modules

1. **config.py** - Configuration management including API keys, model lists, and scoring rubrics
2. **concept_dag.py** - Implementation of the ConceptDAG structure for organizing concept nodes with three levels (sentence, paragraph, article) and their relationships
3. **execution.py** - Core experiment execution logic that interfaces with LLM APIs
4. **storage.py** - Data storage functionality for saving and loading experiment results

### Key Classes

- `ConceptDAG` - Manages the directed acyclic graph of concept nodes with three representation levels
- `ExperimentExecutor` - Executes the experiment across models and prompt nodes
- `DataStorage` - Handles storage and retrieval of experiment data
- `ExperimentResult` - Data class representing a single experiment result

## Package Management

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
uv run pytest daydreaming_experiment/test_concept_dag.py
```

### Code Formatting
```bash
# Format code with black
uv run black .

# Check for style issues with flake8
uv run flake8
```

## Testing Structure

- Unit tests are colocated with the code they test (in the same directory)
- Functional and integration tests are located in the `tests` directory
- All tests use pytest as the testing framework

## Design Preferences

**Dependency Injection**: This project uses dependency injection patterns for better testability and modularity. Dependencies should be passed as constructor parameters rather than created internally.

Examples:
- `ExperimentExecutor` takes an OpenAI client as a parameter instead of creating it internally
- Use factory functions like `create_openai_client()` for production instantiation
- Tests can easily inject mock dependencies without complex patching

## Dependencies

Main dependencies include:
- openrouter-python for LLM API interactions
- pandas and numpy for data handling
- pytest for testing
- black and flake8 for code formatting