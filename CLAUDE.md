# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python experiment that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a focused combinatorial testing approach. The system tests k_max-sized concept combinations to elicit the Day-Dreaming idea from offline LLMs using automated LLM-based evaluation.

## Architecture

### Core Architecture

The project uses **Dagster** for data pipeline orchestration, with a modern asset-based approach:

- **Assets**: Data artifacts with lineage tracking and dependency management
- **Resources**: Configurable services (LLM clients, experiment configuration, I/O managers)
- **Partitions**: Dynamic partitioning for scalable LLM task processing

### Project Structure

- `daydreaming_dagster/` - Main Dagster package with assets, resources, and definitions
- `tests/` - Integration tests (mostly what we have)
- `data/` - Data pipeline stages (1_raw, 2_tasks, 5_parsing, experiments)

# Package Management

This project uses **uv** for fast Python package management and dependency resolution.

## Common Development Commands

### Installation
```bash
uv sync                    # Install project and dependencies
uv sync --dev             # Install with development dependencies
```

### Running the Pipeline
```bash
uv run dagster dev -f daydreaming_dagster/definitions.py    # Start Dagster UI
uv run dagster asset materialize --select "asset_name"     # Materialize specific assets
```

### Testing & Code Quality
```bash
uv run pytest                                 # Run all tests
uv run pytest daydreaming_dagster/           # Unit tests only
uv run pytest tests/                         # Integration tests only
uv run black .                               # Format code
uv run ruff check                            # Check code style
```

## Test-Driven Development (TDD)

This project follows strict TDD practices:

    Always write tests first - Use the test-writer subagent for creating failing tests
    Red-Green-Refactor cycle - Tests must fail before implementation begins
    No implementation without tests - Every feature needs failing tests first

#### Available Subagents

    test-writer: Creates failing tests for new features and bug reproduction. Use when you need comprehensive test coverage before implementation.

#### TDD Workflow

1. Define feature requirements or bug reports
2. Invoke test-writer to create failing tests
3. Verify tests fail for the correct reasons
4. Implement code to make tests pass
5. Refactor while keeping tests green


## Testing Structure

### Test Organization

#### Unit Tests (Fast, Isolated)
- **Location**: Colocated with the modules they test
- **Purpose**: Test individual functions/classes in isolation
- **Data Access**: **MUST NOT** access files in the `data/` directory
- **Dependencies**: Use mocking for external dependencies (APIs, file systems, etc.)
- **Performance**: Should run quickly (<1 second per test)

#### Integration Tests (Component Interaction)
- **Location**: `tests/` directory only
- **Purpose**: Test component interactions and workflows with real data
- **Data Access**: **CAN** read from `data/` directory when testing data-dependent functionality
- **Data Requirements**: **MUST FAIL** if required data files are missing (no graceful skipping)
- **API Restrictions**: **MUST NOT** make real API calls (use proper mocking)


## Design Principles

**Dependency Injection**: Dependencies are passed as constructor parameters rather than created internally for better testability and modularity.

**Selective Loading**: The pipeline supports filtering concepts and templates for faster development and focused experiments using `concept_ids_filter` and `template_names_filter` parameters.

**Jinja2 Templates**: Prompt templates are stored as text files using Jinja2 syntax. Templates receive a `concepts` list with `name`, `concept_id`, and `content` keys.

**Memory-Efficient Processing**: Assets process data sequentially to handle large datasets without memory constraints. Files are read one at a time rather than loading all into memory.

**Proper Dagster I/O Architecture**: All file access uses configured I/O managers rather than hardcoded paths, ensuring test isolation and production flexibility.

**Robust Parser Architecture**: The evaluation response parser handles multiple LLM response formats:
- Multi-line score detection (searches last 3 non-empty lines)
- Markdown formatting support (`**SCORE: 7**`)  
- Multiple score formats (standard numeric and three-digit averages)
- Automatic strategy selection based on evaluation template

## Development Guidelines

- Code should fail early rather than using defensive programming
- Focus on simplicity and clarity
- Commit formatting changes separately from functional changes
- Only run `black` when there are no uncommitted changes

## File Management Notes

- Don't edit data/1_raw/generation_templates/gwern_original.txt - it is copied from the essay and we don't optimize it