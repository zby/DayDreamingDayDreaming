# DayDreaming Dagster Pipeline

This is a Dagster-based data pipeline that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a focused combinatorial testing approach.

## Project Overview

The system tests k_max-sized concept combinations to elicit the Day-Dreaming idea from offline LLMs using automated LLM-based evaluation.

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
# Option A: Use asset groups (recommended - simpler)
uv run dagster asset materialize --select "group:raw_data,group:llm_tasks" -f daydreaming_dagster/definitions.py

# Option B: Use dependency resolution (alternative)
uv run dagster asset materialize --select "+generation_tasks,+evaluation_tasks" -f daydreaming_dagster/definitions.py

# Option C: Explicit list with all dependencies (if you prefer explicit control)
uv run dagster asset materialize --select "concepts,concepts_metadata,generation_models,evaluation_models,generation_templates_metadata,generation_templates,evaluation_templates_metadata,evaluation_templates,content_combinations,generation_tasks,evaluation_tasks" -f daydreaming_dagster/definitions.py

# Dynamic partitions are automatically created when needed

# Step 2: Generate LLM responses for specific partitions
# Now you can run individual partitions or use the UI to run all
# Tip: get a partition key from the first column of generation_tasks.csv
# Example:
# PART=$(cut -d',' -f1 data/2_tasks/generation_tasks.csv | sed -n '2p')
# uv run dagster asset materialize --select "generation_prompt,generation_response" --partition "$PART" -f daydreaming_dagster/definitions.py
uv run dagster asset materialize --select "generation_prompt,generation_response" --partition "<a_generation_task_id_from_csv>" -f daydreaming_dagster/definitions.py

# Step 3: Process results (after sufficient generation/evaluation data)
uv run dagster asset materialize --select "parsed_scores,final_results" -f daydreaming_dagster/definitions.py
```

**Asset Group Breakdown**:
- `group:raw_data`: concepts, models, templates, and their metadata (loads from `data/1_raw/`)
- `group:llm_tasks`: content_combinations, generation_tasks, evaluation_tasks (creates `data/2_tasks/`)
- `group:llm_generation`: generation_prompt, generation_response (creates `data/3_generation/`)
- `group:llm_evaluation`: evaluation_prompt, evaluation_response (creates `data/4_evaluation/`)
- `group:results_processing`: parsed_scores, analysis, and final_results (creates `data/5_parsing/`, `data/6_summary/`)

**Why the specific asset dependencies matter**: 
- `generation_templates` depends on `generation_templates_metadata` 
- `evaluation_templates` depends on `evaluation_templates_metadata`
- These metadata assets load CSV files that specify which templates are active
- Using asset groups or dependency resolution (`+`) automatically includes these dependencies

**Dynamic Partitioning**: Partitions are automatically created from the task CSV files when LLM assets are materialized. Partition keys are based on `generation_task_id`, which includes a stable `combo_id` prefix.

### Running All LLM Partitions

After completing Step 1 above, you'll have ~150 LLM partitions available. To run them all:

**Recommended**: Use the Dagster UI
```bash
uv run dagster dev -f daydreaming_dagster/definitions.py
# Go to http://localhost:3000, find the partitioned assets, click "Materialize all partitions"
```

**Alternative**: CLI loop (sequential, slower)
```bash
# Get all partition names and run them one by one
cut -d',' -f1 data/2_tasks/generation_tasks.csv | tail -n +2 | while read partition; do
  echo "Running partition: $partition"
  uv run dagster asset materialize --select "generation_prompt,generation_response" --partition "$partition" -f daydreaming_dagster/definitions.py
done
```

### Automatic Results Tracking **NEW**

The pipeline now includes automatic cross-experiment tracking:

- **Auto-materializing assets**: New responses are automatically tracked in comprehensive CSV tables
- **Cross-experiment analysis**: Compare results across different experiments and template versions
- **Bulk migration tools**: Scripts to populate tracking tables from existing data

**Initial setup** (populate tables from existing data):
```bash
# Build comprehensive tracking tables from existing responses
./scripts/rebuild_generation_results.sh
python scripts/build_evaluation_results_table.py
```

**Ongoing automatic tracking**: No manual intervention needed - new responses are automatically added to tracking tables when generated.

## Development

### Testing

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

## Configuration

### Environment Variables

- `OPENROUTER_API_KEY`: OpenRouter API key for LLM access
- `DAGSTER_HOME`: Dagster metadata storage (defaults to `dagster_home/`)

### Pipeline Parameters

Configure in `daydreaming_dagster/resources/experiment_config.py`:

- `k_max`: Maximum concept combination size
- `description_level`: Description level ("sentence", "paragraph", "article")
- `template_names_filter`: Optional list of template names to load (None = all)
- Model selection in `data/1_raw/generation_models.csv` and `data/1_raw/evaluation_models.csv`

### Selective Loading for Development

For faster development and testing, use selective loading:

```python
# Example: Load only selective templates for faster testing
config = ExperimentConfig(
    k_max=2,
    template_names_filter=["00_systematic_analytical", "02_problem_solving"]
)

# Concepts are filtered by the 'active' column in concepts_metadata.csv
```

## Data Structure

### Input Data
- **Concepts**: `data/1_raw/concepts/day_dreaming_concepts.json`
- **Templates**: `data/1_raw/generation_templates/` (Jinja2 templates)
- **Models**: `data/1_raw/*_models.csv` (available LLM models)

### Output Data
- **Tasks**: `data/2_tasks/` (generated combinations and tasks)
- **Results**: `data/5_parsing/` (processed results)
- **Global Mapping**: `data/combo_mappings.csv` (append-only mapping of stable combo IDs to their concept components)

## Key Features

- **Partitioned Processing**: Efficient processing of large task sets with automatic recovery
- **Template System**: Flexible Jinja2-based prompt generation
- **Multi-Model Support**: Test across different LLM providers
- **Selective Loading**: Optional filtering for faster development and testing
- **Robust Parser**: Automatic detection and parsing of various LLM evaluation response formats
- **Scalable Processing**: Memory-efficient sequential processing handles large datasets
- **Isolated Testing**: Complete test environment separation from production data

## Troubleshooting

### Common Issues

1. **Missing Asset Dependencies**:
   ```
   FileNotFoundError: [Errno 2] No such file or directory: '.../storage/generation_templates_metadata'
   ```
   **Solution**: Template assets need their metadata dependencies. Use one of these approaches:
   ```bash
   # Recommended: Use asset groups
   uv run dagster asset materialize --select "group:raw_data,group:llm_tasks" -f daydreaming_dagster/definitions.py
   
   # Alternative: Use dependency resolution
   uv run dagster asset materialize --select "+generation_tasks,+evaluation_tasks" -f daydreaming_dagster/definitions.py
   ```

2. **Missing DAGSTER_HOME**:
   ```
   DagsterHomeNotSetError: The environment variable $DAGSTER_HOME is not set
   ```
   **Solution**: Always set DAGSTER_HOME before running any commands:
   ```bash
   export DAGSTER_HOME=./dagster_home
   mkdir -p $DAGSTER_HOME
   ```

3. **Partition Not Found Error**:
   ```
  DagsterUnknownPartitionError: Could not find a partition with key `combo_v1_<hex>_...` (or legacy `combo_001_...`)
   ```
  **Solution**: Make sure task CSV files exist by running step 1 first. Use `cut -d',' -f1 data/2_tasks/generation_tasks.csv | sed -n '2p'` to retrieve a valid partition key.

4. **Partitioned Asset Group Error**:
   ```
   CheckError: Asset has partitions, but no '--partition' option was provided
   ```
   **Solution**: Cannot use group selection with partitioned assets. Use individual partition specification or the Dagster UI.

5. **Missing API Key**:
   ```bash
   export OPENROUTER_API_KEY="your_api_key_here"
   ```

6. **Evaluation Response Formats**:
   The system automatically detects and parses various LLM evaluation response formats:
   - **Score location**: Searches the last 3 non-empty lines for score patterns
   - **Formatting support**: Handles markdown formatting like `**SCORE: 7**`
   - **Score formats**: Standard numeric (e.g., 8.5) and three-digit averages (e.g., 456 → 5.0)
   - **Strategy detection**: Automatically selects parsing strategy based on evaluation template

## Documentation

For detailed architecture and implementation information, see:

- [Architecture Overview](docs/architecture.md) - Detailed system design and components
- [LLM Concurrency Guide](docs/llm_concurrency_guide.md) - LLM API optimization patterns
- [CLAUDE.md](CLAUDE.md) - Development guidelines and project instructions

## Stable Combo IDs

The pipeline uses stable, versioned combo IDs for concept combinations:

- Format: `combo_v1_<12-hex>` (e.g., `combo_v1_1f3a9c2d7b2c`)
- Mapping: `data/combo_mappings.csv` stores an append-only mapping with columns `combo_id, version, concept_id, description_level, k_max, created_at`
- Backward compatibility: Older assets/tests that reference `combo_XXX` continue to work; both formats are accepted in tests/examples where applicable.


## Contributing

1. Follow the testing structure (unit tests in `daydreaming_dagster/`, integration tests in `tests/`)
2. Use Black for code formatting
3. Add type hints where appropriate
4. Update tests for new functionality

## License

[Add your license information here]
