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