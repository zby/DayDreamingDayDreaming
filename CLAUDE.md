# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python experiment that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a structured prompt DAG (Directed Acyclic Graph) approach.

## Code Architecture

### Core Modules

1. **config.py** - Configuration management including API keys, model lists, and scoring rubrics
2. **dag.py** - Implementation of the PromptDAG structure for organizing prompt nodes and their relationships
3. **execution.py** - Core experiment execution logic that interfaces with LLM APIs
4. **storage.py** - Data storage functionality for saving and loading experiment results

### Key Classes

- `PromptDAG` - Manages the directed acyclic graph of prompt nodes
- `ExperimentExecutor` - Executes the experiment across models and prompt nodes
- `DataStorage` - Handles storage and retrieval of experiment data
- `ExperimentResult` - Data class representing a single experiment result

## Common Development Commands

### Installation
```bash
pip install -e .
```

### Running Tests
```bash
# Run all tests
pytest

# Run tests for a specific module
pytest daydreaming_experiment/test_dag.py
```

### Code Formatting
```bash
# Format code with black
black .

# Check for style issues with flake8
flake8
```

## Testing Structure

- Unit tests are colocated with the code they test (in the same directory)
- Functional and integration tests are located in the `tests` directory
- All tests use pytest as the testing framework

## Dependencies

Main dependencies include:
- openrouter-python for LLM API interactions
- pandas and numpy for data handling
- pytest for testing
- black and flake8 for code formatting