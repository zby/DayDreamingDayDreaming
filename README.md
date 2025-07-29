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

### Running an Experiment

The main experiment runner uses combinatorial testing to find minimal concept sets that can elicit the Day-Dreaming idea from LLMs:

```bash
# Run complete experiment with automated evaluation
uv run python -m daydreaming_experiment.experiment_runner \
    --k-max 4 \
    --level paragraph \
    --generator-model gpt-4 \
    --evaluator-model gpt-4 \
    --output experiments/my_experiment
```

#### Parameters

- `--k-max`: Maximum number of concepts to combine (default: 4)
- `--level`: Content granularity level (`sentence`, `paragraph`, or `article`)
- `--generator-model`: LLM model for content generation
- `--evaluator-model`: LLM model for automated evaluation
- `--output`: Output directory for experiment results

### Analyzing Results

After running an experiment, analyze the results:

```bash
# Analyze experiment results
uv run python -m daydreaming_experiment.results_analysis \
    experiments/experiment_20250728_143022
```

## Testing

This project uses a clear testing strategy with separation between unit tests and integration tests.

### Test Organization

#### Unit Tests (Fast & Isolated)
- **Location**: Colocated with source code (e.g., `test_concept.py` next to `concept.py`)
- **Purpose**: Test individual components in isolation
- **Restrictions**: Cannot access `data/` directory files
- **Dependencies**: Use mocking for external services
- **Speed**: Fast execution (<1s per test)

#### Integration Tests (Data-Dependent)
- **Location**: `tests/` directory
- **Purpose**: Test component interactions with real data
- **Data Access**: Can read from `data/` directory
- **Data Requirements**: Must fail if required data files are missing
- **Restrictions**: No real API calls (use mocking)

### Running Tests

```bash
# Run all tests
uv run pytest

# Run only unit tests (fast, no data dependencies)
uv run pytest daydreaming_experiment/

# Run only integration tests (may require data files)
uv run pytest tests/

# Run tests with coverage report
uv run pytest --cov=daydreaming_experiment

# Run tests in parallel for speed
uv run pytest -n auto

# Run specific test file
uv run pytest daydreaming_experiment/test_concept.py

# Run with verbose output
uv run pytest -v
```

### Test Examples

**Unit Test** (isolated, mocked dependencies):
```python
def test_concept_get_description_fallback():
    """Unit test with no external dependencies."""
    concept = Concept(name="test", descriptions={"paragraph": "Test content"})
    assert concept.get_description("sentence", strict=False) == "Test content"
```

**Integration Test** (real data, fails if missing):
```python
def test_load_real_concept_database():
    """Integration test that requires real data files."""
    # Will fail with clear error if file doesn't exist
    db = ConceptDB.load("data/concepts/day_dreaming_concepts.json")
    assert len(db.get_concepts()) > 0
    
    # Validate expected structure
    for concept in db.get_concepts():
        assert concept.name
        assert concept.descriptions
```

### Development Workflow
1. Write unit tests for new functionality (fast feedback)
2. Add integration tests for data-dependent features
3. Run unit tests frequently during development
4. Run full test suite before committing changes

## License

This project is licensed under the MIT License - see the LICENSE file for details.