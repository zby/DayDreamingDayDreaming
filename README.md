# Daydreaming Experiment

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Overview

This is a Python experiment that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a focused combinatorial testing approach. The system tests k_max-sized concept combinations to elicit the Day-Dreaming idea from offline LLMs using automated LLM-based evaluation.

The project uses Kedro for data pipeline orchestration with a hierarchical task architecture: concept combinations → generation tasks → evaluation tasks.

## Project Architecture

### Core Components
- **ConceptDB**: Registry for batch concept retrieval and combination iteration
- **Model Selection**: CSV-based active model selection for generation and evaluation
- **Template System**: Jinja2-based prompt templates with rich concept formatting
- **Hierarchical Tasks**: concept combinations → generation tasks → evaluation tasks

### Data Structure
```
data/
├── 01_raw/                     # External inputs
│   ├── concepts/               # Concept database and articles
│   ├── generation_templates/   # Jinja2 prompt templates
│   ├── evaluation_templates/   # Evaluation prompt templates
│   ├── generation_models.csv   # Available generation models
│   └── evaluation_models.csv   # Available evaluation models
├── 02_tasks/                   # Generated hierarchical tasks
│   ├── concept_combinations.csv
│   ├── concept_combo_relationships.csv
│   ├── generation_tasks.csv
│   └── evaluation_tasks.csv
├── 03_generation/              # LLM generation results
├── 04_evaluation/              # LLM evaluation results
├── 05_parsing/                 # Parsed scores
└── 06_summary/                 # Final results
```

## Installation

This project uses **uv** for fast Python package management:

```bash
# Install the project and all dependencies
uv sync

# Install development dependencies
uv sync --dev
```

## Running the Pipeline

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

## Testing

The project follows a clear separation between unit tests and integration tests:

```bash
# Run all tests
uv run pytest

# Run only unit tests (fast, isolated - no data dependencies)
uv run pytest daydreaming_experiment/

# Run only integration tests (requires real data files)
uv run pytest tests/

# Run with coverage report
uv run pytest --cov=daydreaming_experiment

# Run tests in parallel for speed
uv run pytest -n auto
```

### Test Architecture
- **Unit Tests**: Colocated with modules (e.g., `src/daydreaming_experiment/pipelines/daydreaming/test_nodes.py`)
- **Integration Tests**: In `tests/` directory, use real data files and must fail if data is missing

## Key Features

- **Active Model Selection**: CSV-based model configuration with `active` column for easy switching
- **Hierarchical Task Structure**: Efficient N:M relationship between concept combinations and model tasks
- **Jinja2 Template System**: Rich formatting with concept names, IDs, and content
- **Real Data Integration Testing**: Tests that fail fast if required data files are missing
- **Dependency Injection**: Testable architecture with easy mocking

## Dependencies

Main dependencies:
- Kedro for pipeline orchestration
- Jinja2 for template processing
- pandas for data manipulation
- pytest for testing
- OpenRouter for LLM API access

## Configuration

### Model Selection
Models are configured in CSV files with an `active` column:
- `data/01_raw/generation_models.csv`
- `data/01_raw/evaluation_models.csv`

Set `active=True` for models you want to use in the pipeline.

### Templates
Prompt templates use Jinja2 syntax and are stored in:
- `data/01_raw/generation_templates/` - For LLM generation prompts
- `data/01_raw/evaluation_templates/` - For evaluation prompts

### Parameters
Key pipeline parameters in `conf/base/parameters.yml`:
- `k_max`: Maximum number of concepts in combinations
- `current_gen_template`: Active generation template
- `current_eval_template`: Active evaluation template

## Development

```bash
# Format code
uv run black .

# Check style
uv run flake8

# Run specific test file
uv run pytest daydreaming_experiment/pipelines/daydreaming/test_nodes.py
```

### Jupyter Support

```bash
# Start Jupyter with Kedro context
uv run kedro jupyter notebook

# Start JupyterLab
uv run kedro jupyter lab

# IPython session
uv run kedro ipython
```

## Documentation

For detailed development guidelines, architecture decisions, and coding standards, see:
- **[CLAUDE.md](CLAUDE.md)** - Comprehensive development guide and project architecture

## Data Engineering Conventions

- Raw data in `data/01_raw/` (external inputs only)
- Generated data follows numbered convention (`02_tasks/`, `03_generation/`, etc.)
- No data or credentials committed to repository
- All configuration in `conf/base/` or `conf/local/`
