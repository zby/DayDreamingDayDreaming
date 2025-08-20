# DayDreaming Dagster Pipeline

This is a Dagster-based data pipeline that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a focused combinatorial testing approach.

## Project Overview

The system tests k_max-sized concept combinations to elicit the Day-Dreaming idea from offline LLMs using automated LLM-based evaluation. The pipeline features a **two-phase generation architecture** that separates creative brainstorming from structured essay composition for improved quality and consistency.

### Two-Phase Generation System 🚀

The pipeline uses an innovative two-phase approach to LLM generation:

**Phase 1 - Links Generation**: LLMs brainstorm conceptual connections and combinations between input concepts, producing 6-12 specific bullet points describing how concepts could be integrated.

**Phase 2 - Essay Generation**: Using the links from Phase 1 as inspiration, LLMs compose comprehensive essays (1500-3000 words) that develop the most promising conceptual combinations into detailed analyses.

**Key Benefits**:
- **Higher Quality**: Separates creative ideation from structured writing
- **Robust Validation**: Phase 2 fails if Phase 1 produces insufficient links (< 3)
- **Better Essays**: Phase 2 has rich context from Phase 1 brainstorming
- **Backward Compatible**: Existing evaluation and analysis assets work unchanged

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
uv run dagster asset materialize --select "group:raw_data,group:task_definitions" -f daydreaming_dagster/definitions.py

# Option B: Use dependency resolution (alternative)
uv run dagster asset materialize --select "+link_generation_tasks,+essay_generation_tasks,+evaluation_tasks" -f daydreaming_dagster/definitions.py

# Option C: Explicit list with all dependencies (if you prefer explicit control)
uv run dagster asset materialize --select "concepts,llm_models,link_templates,essay_templates,evaluation_templates,content_combinations,link_generation_tasks,essay_generation_tasks,evaluation_tasks" -f daydreaming_dagster/definitions.py

# Dynamic partitions are automatically created when needed

# Step 2: Generate LLM responses using two-phase generation
# Now you can run individual partitions or use the UI to run all
# Tip: get a link partition key from the first column of link_generation_tasks.csv
# Example (Phase 1):
# LINK=$(cut -d',' -f1 data/2_tasks/link_generation_tasks.csv | sed -n '2p')
# uv run dagster asset materialize --select "group:generation_links" --partition "$LINK" -f daydreaming_dagster/definitions.py

# Then get an essay partition key for that link from essay_generation_tasks.csv
# Example (Phase 2):
# ESSAY=$(awk -F, -v link="$LINK" 'NR==1{for(i=1;i<=NF;i++)h[$i]=i} NR>1 && $h["link_task_id"]==link {print $h["essay_task_id"]; exit}' data/2_tasks/essay_generation_tasks.csv)
# uv run dagster asset materialize --select "group:generation_essays" --partition "$ESSAY" -f daydreaming_dagster/definitions.py

# Step 3: Process results (after sufficient generation/evaluation data)
uv run dagster asset materialize --select "parsed_scores,final_results" -f daydreaming_dagster/definitions.py
```

**Asset Group Breakdown**:
- `group:raw_data`: concepts, models, templates, and their metadata (loads from `data/1_raw/`)
- `group:task_definitions`: content_combinations, link_generation_tasks, essay_generation_tasks, evaluation_tasks (creates `data/2_tasks/`)
- `group:generation_links`: links_prompt, links_response (creates `data/3_generation/links_*`)
- `group:generation_essays`: essay_prompt, essay_response (creates `data/3_generation/essay_*`)
- `group:evaluation`: evaluation_prompt, evaluation_response (creates `data/4_evaluation/`)
- `group:results_processing`: parsed_scores, analysis, and final_results (creates `data/5_parsing/`, `data/6_summary/`)

**Why the specific asset dependencies matter**: 
- `link_templates` and `essay_templates` load their CSVs and template files and determine activeness
- `evaluation_templates` loads evaluation CSV + files
- Using asset groups or dependency resolution (`+`) automatically includes these dependencies

**Dynamic Partitioning**: Partitions are automatically created from the task CSV files when LLM assets are materialized. Partition keys are based on `link_task_id` (links) and `essay_task_id` (essays), which include a stable `combo_id` prefix.

### Running All LLM Partitions

After completing Step 1 above, you'll have ~150 LLM partitions available. To run them all:

**Recommended**: Use the Dagster UI
```bash
uv run dagster dev -f daydreaming_dagster/definitions.py
# Go to http://localhost:3000, find the partitioned assets, click "Materialize all partitions"
```

**Alternative**: CLI loop (sequential, slower)
```bash
# Get all partition names and run them one by one (two-phase generation)
# Phase 1: run all link partitions
cut -d',' -f1 data/2_tasks/link_generation_tasks.csv | tail -n +2 | while read LINK; do
  echo "Running link partition: $LINK"
  uv run dagster asset materialize --select "group:generation_links" --partition "$LINK" -f daydreaming_dagster/definitions.py
done

# Phase 2: run all essay partitions
cut -d',' -f1 data/2_tasks/essay_generation_tasks.csv | tail -n +2 | while read ESSAY; do
  echo "Running essay partition: $ESSAY"
  uv run dagster asset materialize --select "group:generation_essays" --partition "$ESSAY" -f daydreaming_dagster/definitions.py
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
- **Templates**: `data/1_raw/generation_templates/` (Jinja2 templates with two-phase structure)
- **Models**: `data/1_raw/*_models.csv` (available LLM models)

#### Template Structure (Two-Phase)

Templates are organized in a two-phase structure:

```
data/1_raw/generation_templates/
├── links/                    # Phase 1: Concept link generation
│   ├── creative-synthesis-v7.txt
│   ├── systematic-analytical.txt
│   └── ...
└── essay/                    # Phase 2: Essay composition
    ├── creative-synthesis-v7.txt
    ├── systematic-analytical.txt
    └── ...
```

**Phase 1 Templates** (`links/`): 
- Focus on brainstorming conceptual connections
- Output: 6-12 bullet points describing concept combinations
- Template variables: `concepts` (list with name, content)

**Phase 2 Templates** (`essay/`):
- Focus on structured essay composition
- Input: `links_block` (raw output from Phase 1)
- Output: 1500-3000 word essays developing the best combinations
- Built-in validation: Phase 2 fails if Phase 1 produces < 3 usable links

### Output Data
- **Tasks**: `data/2_tasks/` (generated combinations and tasks)
- **Two-Phase Generation**: `data/3_generation/` (links and essays)
  - `links_prompts/`, `links_responses/` (Phase 1 outputs)
  - `essay_prompts/`, `essay_responses/` (Phase 2 outputs)
  - `parsed_generation_responses/` (legacy; two-phase writes directly to `links_*` and `essay_*`)
- **Legacy Generation**: `data/3_generation/` (single-phase outputs, still supported)
  - `generation_prompts/`, `generation_responses/`
- **Results**: `data/5_parsing/` (processed results)
- **Global Mapping**: `data/combo_mappings.csv` (append-only mapping of stable combo IDs to their concept components)

## Key Features

- **🚀 Two-Phase Generation**: Innovative separated brainstorming and essay composition with quality validation
- **Partitioned Processing**: Efficient processing of large task sets with automatic recovery
- **Template System**: Flexible Jinja2-based prompt generation with phase-aware loading
- **Multi-Model Support**: Test across different LLM providers
- **Selective Loading**: Optional filtering for faster development and testing
- **Robust Parser**: Automatic detection and parsing of various LLM evaluation response formats
- **Scalable Processing**: Memory-efficient sequential processing handles large datasets
- **Backward Compatibility**: Legacy single-phase generation still supported
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
   uv run dagster asset materialize --select "group:raw_data,group:task_definitions" -f daydreaming_dagster/definitions.py
   
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
  **Solution**: Make sure task CSV files exist by running step 1 first. Use `cut -d',' -f1 data/2_tasks/link_generation_tasks.csv | sed -n '2p'` to retrieve a valid link partition key.

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

### Style and Commit Policy
- Run style checks locally: `uv run black .` and `uv run ruff check`.
- Style-only changes must be in separate commits. Do not mix formatting/lint fixes with functional changes. Commit sequence: logic first, then a follow-up commit like `style: format with Black`.

### CI & Style Checks (Reminder)
- If CI is enabled for this repo, it may run Black/Ruff checks. If a style failure occurs, push a separate, style-only commit to fix formatting rather than amending functional commits.

## License

[Add your license information here]
