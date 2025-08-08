# Test Placement Policy

This document outlines the test placement policy for the DayDreaming Dagster Pipeline project.

## Test Organization

### Unit Tests (Fast, Isolated)
**Location**: Colocated with the modules they test (e.g., `daydreaming_dagster/assets/test_*.py`)

**Purpose**: Test individual functions/classes in isolation

**Data Access**: 
- **MUST NOT** access files in the `data/` directory
- Use mocking for external dependencies (APIs, file systems, etc.)

**Performance**: Should run quickly (<1 second per test)

**Examples**:
- `daydreaming_dagster/assets/test_llm_generation.py` - Tests prompt template logic
- `daydreaming_dagster/utils/test_eval_response_parser.py` - Tests evaluation response parsing
- `daydreaming_dagster/models/test_content_combination.py` - Tests ContentCombination class

### Integration Tests (Component Interaction)
**Location**: `tests/` directory only

**Purpose**: Test component interactions and workflows with real data

**Data Access**: 
- **CAN** read from `data/` directory when testing data-dependent functionality
- **MUST FAIL** if required data files are missing (no graceful skipping)

**API Restrictions**: **MUST NOT** make real API calls (use proper mocking)

**Examples**:
- `tests/test_pipeline_integration.py` - Tests full pipeline materialization with live data
- `tests/test_concepts_active_column_integration.py` - Tests concept filtering integration

## Running Tests

```bash
# Run all tests
uv run pytest

# Run only unit tests (fast, isolated)
uv run pytest daydreaming_dagster/

# Run only integration tests (data-dependent)  
uv run pytest tests/

# Run with coverage
uv run pytest --cov=daydreaming_dagster
```

## Guidelines

### When to Write Unit Tests
- Testing pure functions and class methods
- Testing business logic without external dependencies
- Testing error handling and edge cases
- When you need fast feedback during development

### When to Write Integration Tests  
- Testing asset materialization workflows
- Testing data pipeline end-to-end functionality
- Testing component interactions with real data
- Testing configuration and resource management

### Test Data Management
- **Unit tests**: Use minimal, programmatically created test data
- **Integration tests**: Can use live data from `data/` directory but should limit scope for performance
- **Test isolation**: Integration tests use temporary directories to avoid affecting production data

### Mocking Guidelines
- Mock external APIs in both unit and integration tests
- Mock file system access in unit tests only
- Use dependency injection to make mocking easier
- Prefer real data over mocks in integration tests when testing data processing logic

## Current Test Structure

This project already follows these guidelines:

- ✅ Unit tests are colocated with source code in `daydreaming_dagster/`
- ✅ Integration tests are in `tests/` directory
- ✅ Integration tests use real data but create isolated environments
- ✅ No tests make real API calls (all use mocking)
- ✅ Clear separation between fast unit tests and comprehensive integration tests