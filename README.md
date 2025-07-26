# Daydreaming Experiment

This project tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a structured prompt DAG approach.

## Installation

This project uses [uv](https://docs.astral.sh/uv/) for fast Python package management.

```bash
# Clone the repository
git clone <repository-url>
cd DayDreamingDayDreaming

# Install the package and dependencies
uv sync

# Install with development dependencies (includes testing and formatting tools)
uv sync --dev
```

## Usage

```python
# TODO: Add usage examples
```

## Tests

Tests are organized in the following way:
- Unit tests are colocated with the code they test (in the same directory)
- Functional and integration tests are located in the `tests` directory
- All tests use pytest as the testing framework

To run tests:
```bash
# Run all tests
uv run pytest

# Run tests for a specific module
uv run pytest daydreaming_experiment/test_concept_dag.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.