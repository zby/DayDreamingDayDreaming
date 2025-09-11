# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python experiment testing whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept through combinatorial testing. The system uses Dagster for data pipeline orchestration with asset-based architecture.

## Development Commands

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
.venv/bin/pytest                              # Run all tests (preferred)
.venv/bin/pytest daydreaming_dagster/         # Unit tests only
.venv/bin/pytest tests/                       # Integration tests only
uv run black .                                # Format code
uv run ruff check                             # Check code style
```

Note: prefer `.venv/bin/pytest` instead of `uv run pytest` — uv may attempt to write to a global cache outside the sandboxed filesystem, which can fail under restricted execution.

## Key Guidelines

- **See AGENTS.md for comprehensive development guidelines** - it contains up-to-date testing practices, commit guidelines, and coding standards
- Use dependency injection for testability
- Fail early with clear error messages rather than defensive programming  
- Commit formatting changes separately from functional changes
- Don't edit `data/1_raw/templates/draft/gwern_original.txt` - it's copied from the essay
