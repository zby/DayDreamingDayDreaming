# DayDreaming Dagster Pipeline

This is a Dagster-based data pipeline that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a focused combinatorial testing approach.

## Project Overview

The system tests k_max-sized concept combinations to elicit the Day-Dreaming idea from offline LLMs using automated LLM-based evaluation. The pipeline orchestrates:

- **Concept combination generation** from a curated database
- **LLM prompt generation** using Jinja2 templates  
- **Response generation** from multiple LLM models
- **Automated evaluation** of responses for daydreaming-like insights
- **Results aggregation** and analysis

## Architecture

### Dagster Pipeline Structure

```
daydreaming_dagster/
├── assets/
│   ├── raw_data.py              # Raw data loading (concepts, templates, models)
│   ├── core.py                  # Core processing (combinations, tasks)
│   ├── partitions.py            # Partition management
│   └── llm_prompts_responses.py # LLM interaction assets
├── resources/
│   ├── llm_client.py            # LLM API client resource
│   ├── experiment_config.py     # Configuration resource
│   └── io_managers.py           # Custom I/O managers
└── definitions.py               # Single definitions file
```

### Data Flow

```
Raw Data (01_raw/) 
    ↓
Task Generation (02_tasks/)
    ↓  
LLM Generation (03_generation/)
    ↓
LLM Evaluation (04_evaluation/)
    ↓
Results Processing (05_parsing/ & 06_summary/)
```

## Quick Start

### Prerequisites

- Python 3.9+
- UV package manager
- OpenRouter API key (or other LLM API)

### Installation

```bash
# Clone and install
git clone <repository>
cd DayDreamingDayDreaming
uv sync

# Set up environment variables
cp .env.example .env
# Edit .env with your API keys
```

### Running the Pipeline

#### Option 1: Dagster UI (Recommended)
```bash
# Start the Dagster development server
uv run dagster dev -f daydreaming_dagster/definitions.py

# Open browser to http://localhost:3000
# Use the UI to materialize assets interactively
```

#### Option 2: Command Line (3-Step Process)

**Important**: The pipeline requires a specific sequence due to Dagster's dynamic partitioning system.

```bash
# Step 1: Generate setup assets and task CSV files
uv run dagster asset materialize --select "concepts,concepts_metadata,generation_models,evaluation_models,generation_templates,evaluation_templates,content_combinations,generation_tasks,evaluation_tasks" -f daydreaming_dagster/definitions.py

# Step 2: Create dynamic partitions (REQUIRED before LLM assets)
# This reads the CSV files and registers each task as a partition in Dagster
uv run dagster asset materialize --select "task_definitions" -f daydreaming_dagster/definitions.py

# Step 3: Generate LLM responses for specific partitions
# Now you can run individual partitions or use the UI to run all
uv run dagster asset materialize --select "generation_prompt,generation_response" --partition "combo_001_02_problem_solving_deepseek/deepseek-r1:free" -f daydreaming_dagster/definitions.py

# Step 4: Process results (after sufficient generation/evaluation data)
uv run dagster asset materialize --select "parsed_scores,final_results" -f daydreaming_dagster/definitions.py
```

**Why task_definitions is needed**: Dagster uses dynamic partitioning where partitions are created at runtime based on the actual tasks generated. The `task_definitions` asset reads the CSV files and calls `context.instance.add_dynamic_partitions()` to register each task ID as a partition that the LLM assets can use.

### Running All LLM Partitions

After completing Steps 1-2 above, you'll have ~150 LLM partitions available. To run them all:

**Recommended**: Use the Dagster UI
```bash
uv run dagster dev -f daydreaming_dagster/definitions.py
# Go to http://localhost:3000, find the partitioned assets, click "Materialize all partitions"
```

**Alternative**: CLI loop (sequential, slower)
```bash
# Get all partition names and run them one by one
cut -d',' -f1 data/02_tasks/generation_tasks.csv | tail -n +2 | while read partition; do
  echo "Running partition: $partition"
  uv run dagster asset materialize --select "generation_prompt,generation_response" --partition "$partition" -f daydreaming_dagster/definitions.py
done
```

## Development

### Testing

```bash
# Run all tests
uv run pytest

# Run only unit tests (fast, isolated)
uv run pytest daydreaming_dagster/

# Run only integration tests (data-dependent)
uv run pytest daydreaming_dagster_tests/

# Run with coverage
uv run pytest --cov=daydreaming_dagster
```

### Code Formatting

```bash
# Format code
uv run black .

# Check style
uv run ruff check
```

## Configuration

### Environment Variables

- `OPENROUTER_API_KEY`: OpenRouter API key for LLM access
- `DAGSTER_HOME`: Dagster metadata storage (defaults to `dagster_home/`)

### Pipeline Parameters

Configure in `daydreaming_dagster/resources/experiment_config.py`:

- `k_max`: Maximum concept combination size
- `description_level`: Description level ("sentence", "paragraph", "article")
- `concept_ids_filter`: Optional list of concept IDs to load (None = all)
- `template_names_filter`: Optional list of template names to load (None = all)
- Model selection in `data/01_raw/generation_models.csv` and `data/01_raw/evaluation_models.csv`

### Selective Loading for Development

For faster development and testing, use selective loading:

```python
# Example: Load only 3 concepts and 2 templates for faster testing
config = ExperimentConfig(
    k_max=2,
    concept_ids_filter=["dearth-ai-discoveries", "default-mode-network", "human-creativity-insight"],
    template_names_filter=["00_systematic_analytical", "02_problem_solving"]
)

# Result: 3 combinations × 2 templates × 2 models = 12 tasks (vs 150+ for full dataset)
```

## Data Structure

### Input Data
- **Concepts**: `data/01_raw/concepts/day_dreaming_concepts.json`
- **Templates**: `data/01_raw/generation_templates/` (Jinja2 templates)
- **Models**: `data/01_raw/*_models.csv` (available LLM models)

### Output Data
- **Tasks**: `data/02_tasks/` (generated combinations and tasks)
- **Generation**: `data/03_generation/` (LLM prompts and responses)
- **Evaluation**: `data/04_evaluation/` (evaluation prompts and scores)
- **Results**: `data/05_parsing/` and `data/06_summary/` (processed results)

## Key Features

- **Partitioned Assets**: Efficient processing of large task sets
- **Template System**: Flexible Jinja2-based prompt generation
- **Multi-Model Support**: Test across different LLM providers
- **Automated Evaluation**: LLM-based scoring of creativity and insight
- **Human-Readable Outputs**: CSV and text files for easy debugging
- **Selective Loading**: Optional filtering for faster development and testing
- **Performance Optimization**: Scale from full experiments to focused tests

## Architecture Details

### Assets

- **Raw Data Assets**: Load concepts, templates, and model configurations
- **Core Processing Assets**: Generate concept combinations and task definitions
- **Partitioned LLM Assets**: Handle individual generation/evaluation tasks
- **Results Assets**: Parse scores and create final summaries

### Resources

- **LLMClientResource**: Configurable API client for different LLM providers
- **ExperimentConfig**: Centralized parameter management with selective loading support
- **Custom I/O Managers**: Human-readable file formats (CSV, text)

### Partitioning Strategy

The pipeline uses dynamic partitioning:
- **Generation tasks**: Partitioned by `{combo_id}_{template}_{model}`
- **Evaluation tasks**: Partitioned by `{generation_task_id}_{eval_template}_{eval_model}`

This enables:
- Parallel processing of independent tasks
- Easy restart of failed partitions
- Incremental pipeline execution

## Migration from Kedro

This project was migrated from Kedro to Dagster for improved:
- Asset lineage and dependency tracking
- Partitioned asset processing
- Web UI for pipeline monitoring
- Integration with data catalogs and metadata

Legacy Kedro pipeline code is available in `src/legacy/` for reference.

## Contributing

1. Follow the testing structure (unit tests in `daydreaming_dagster/`, integration tests in `daydreaming_dagster_tests/`)
2. Use Black for code formatting
3. Add type hints where appropriate
4. Update tests for new functionality

## License

[Add your license information here]
