# DayDreaming Dagster Implementation

This directory contains the complete Dagster implementation of the DayDreaming experiment, migrated from Kedro.

## Architecture Overview

The implementation follows the plan in `plans/migration_to_dagster.md` and includes:

### Assets Structure
- **raw_data.py**: Load external data files (concepts, models, templates)
- **core.py**: Basic task setup assets (concepts, combinations, tasks)
- **partitions.py**: Dynamic partition definitions for scalable LLM processing
- **llm_prompts_responses.py**: Partitioned LLM prompt and response assets

### Resources
- **api_client.py**: OpenRouter API client and experiment configuration
- **io_managers.py**: Custom I/O managers for file-based storage

### Key Features

1. **Dynamic Partitions**: Each LLM prompt/response is a separate partition for granular caching
2. **Interruption Recovery**: Failed or interrupted runs automatically resume from where they left off
3. **File-Based Storage**: Maintains existing CSV and text file structure for debugging
4. **Complete Asset Graph**: 20 assets covering the full experiment pipeline
5. **Group-Based Organization**: Assets organized by logical groups for easy selection

## Usage

### Setup
```bash
# REQUIRED: Set persistent Dagster instance for partition storage
export DAGSTER_HOME=/tmp/dagster_home
mkdir -p $DAGSTER_HOME
touch $DAGSTER_HOME/dagster.yaml

# Set API key for LLM generation
export OPENROUTER_API_KEY="your_api_key_here"

# Install dependencies (already in main pyproject.toml)
uv sync
```

### Running the Pipeline

#### Option 1: Interactive UI (Recommended)
```bash
# Launch Dagster UI
dagster dev -f daydreaming_dagster/definitions.py
```
Then visit `http://localhost:3000` to see the asset graph and control your experiment visually.

#### Option 2: Command Line Execution

**Step-by-Step Workflow:**
```bash
# REQUIRED FIRST: Set up Dagster instance for partition persistence
export DAGSTER_HOME=/tmp/dagster_home
mkdir -p $DAGSTER_HOME
touch $DAGSTER_HOME/dagster.yaml

# Step 1: Load raw data and set up experiment tasks (creates partitions)
dagster asset materialize --select "+group:raw_data,+group:daydreaming_experiment" --module-name daydreaming_dagster

# Step 3: Generate specific LLM responses (requires API key and partition)
# IMPORTANT: Partitioned assets require individual partition specification
dagster asset materialize --select "generation_prompt,generation_response" \
  --partition "combo_001_00_systematic_analytical_deepseek-r1" \
  -f daydreaming_dagster/definitions.py

# Step 4: Evaluate specific responses (requires partition)
dagster asset materialize --select "evaluation_prompt,evaluation_response" \
  --partition "combo_001_00_systematic_analytical_deepseek-r1_creativity_metrics_deepseek-r1" \
  -f daydreaming_dagster/definitions.py

# Step 5: Get final results
dagster asset materialize --select "+group:results_processing" -f daydreaming_dagster/definitions.py
```

**All at Once:**
```bash
# REQUIRED: Set persistent instance first
export DAGSTER_HOME=/tmp/dagster_home && mkdir -p $DAGSTER_HOME && touch $DAGSTER_HOME/dagster.yaml

# Materialize non-partitioned assets (partitioned assets require individual partition specification)
dagster asset materialize --select "+group:raw_data,+group:daydreaming_experiment,+group:results_processing" --module-name daydreaming_dagster
```

### Asset Groups

Assets are organized into logical groups for easy selection:

- **`raw_data`**: External data loading (concepts, models, templates)
- **`daydreaming_experiment`**: Core experiment logic (combinations, tasks)  
- **`llm_generation`**: LLM prompt/response generation (partitioned)
- **`llm_evaluation`**: LLM evaluation (partitioned)
- **`results_processing`**: Final results and parsing

### Partition Management

**Important**: Dynamic partitions require specific workflow due to Dagster's architecture:

```bash
# STEP 0: REQUIRED - Set up persistent Dagster instance
export DAGSTER_HOME=/tmp/dagster_home
mkdir -p $DAGSTER_HOME
touch $DAGSTER_HOME/dagster.yaml

# STEP 1: REQUIRED - Create partitions first
dagster asset materialize --select "task_definitions" --module-name daydreaming_dagster

# STEP 2: Materialize specific partitions (not groups!)
dagster asset materialize --select "generation_response" --partition "combo_001_02_problem_solving_deepseek/deepseek-r1:free" --module-name daydreaming_dagster

# ERROR: Cannot use group selection with partitioned assets
# dagster asset materialize --select "+group:llm_generation"  # ❌ FAILS
# Error: "Asset has partitions, but no '--partition' option was provided"

# View partition status and available partitions in UI
dagster dev --module-name daydreaming_dagster
```

**Why this workflow is required:**
1. **DAGSTER_HOME**: Partitions must persist between CLI commands in a Dagster instance
2. **Dynamic partitions**: Don't exist until `task_definitions` creates them  
3. **CLI limitation**: Cannot materialize entire groups containing partitioned assets
4. **Individual specification**: Each partition must be specified individually for partitioned assets

## Migration Benefits

1. **Perfect Interruption Recovery**: Each LLM call is cached individually
2. **Dynamic Scaling**: Partitions automatically adapt to input data
3. **Enhanced Observability**: Rich UI showing progress, failures, and dependencies
4. **Granular Control**: Retry individual failed responses or run specific subsets
5. **Group-Based Selection**: Intuitive asset organization and selection
6. **ANTLR4 Compatibility**: Fixed Python 3.13 compatibility issues

## Technical Notes

### Python 3.13 Compatibility
This implementation includes a fix for ANTLR4 compatibility with Python 3.13:
- Upgraded `antlr4-python3-runtime` from 4.9.3 to 4.13.2
- All asset selection patterns now work correctly
- Comprehensive test coverage for compatibility issues

### File Structure

The implementation preserves the existing data structure:
```
data/
├── 01_raw/                     # External inputs (unchanged)
├── 02_tasks/
│   ├── concept_combinations_combinations.csv
│   ├── concept_combinations_relationships.csv
│   ├── generation_tasks.csv
│   ├── evaluation_tasks.csv
│   └── concept_contents/       # Individual concept files
├── 03_generation/
│   ├── generation_prompts/     # Individual prompt files
│   └── generation_responses/   # Individual response files
├── 04_evaluation/
│   ├── evaluation_prompts/
│   └── evaluation_responses/
├── 05_parsing/
│   └── parsed_scores.csv
├── 06_summary/
│   └── final_results.csv
└── 07_reporting/               # Error logs
```

## Testing

```bash
# Run Dagster-specific tests
uv run pytest daydreaming_dagster_tests/

# Run all tests (includes compatibility tests)
uv run pytest

# Test specific compatibility features
python daydreaming_dagster_tests/test_dagster_cli_compatibility.py
```

## Troubleshooting

### Common Issues

1. **Missing DAGSTER_HOME (Most Common)**:
   ```
   DagsterHomeNotSetError: The environment variable $DAGSTER_HOME is not set
   ```
   **Solution**: Always set DAGSTER_HOME before running any commands:
   ```bash
   export DAGSTER_HOME=/tmp/dagster_home
   mkdir -p $DAGSTER_HOME
   touch $DAGSTER_HOME/dagster.yaml
   ```

2. **Partition Not Found Error**:
   ```
   DagsterUnknownPartitionError: Could not find a partition with key `combo_001_...`
   ```
   **Solution**: Run `task_definitions` asset first to create the dynamic partitions:
   ```bash
   dagster asset materialize --select "task_definitions" --module-name daydreaming_dagster
   ```

3. **Partitioned Asset Group Error**:
   ```
   CheckError: Asset has partitions, but no '--partition' option was provided
   ```
   **Solution**: Cannot use group selection with partitioned assets. Use individual partition specification:
   ```bash
   # ❌ This fails
   dagster asset materialize --select "+group:llm_generation"
   
   # ✅ This works  
   dagster asset materialize --select "generation_prompt" --partition "combo_001_..."
   ```

4. **ANTLR4 Compatibility Error**:
   ```
   TypeError: ord() expected string of length 1, but int found
   ```
   **Solution**: Ensure `antlr4-python3-runtime>=4.13.2` is installed

5. **Missing API Key**:
   ```bash
   export OPENROUTER_API_KEY="your_api_key_here"
   ```

### Performance Tips

- **Parallel Execution**: Raw data assets run in parallel automatically
- **Incremental Updates**: Only missing partitions are materialized
- **Memory Management**: Large datasets use efficient I/O managers
- **Progress Monitoring**: Use Dagster UI for real-time progress tracking

## Understanding Data Storage (New to Dagster?)

If you're new to Dagster, here's how data storage works in this implementation:

### 🔄 **Temporary Storage (Internal)**
When you run raw data assets, you'll see logs like:
```
Writing file at: /tmp/.tmp_dagster_home_xyz/storage/concepts_metadata using PickledObjectFilesystemIOManager...
```

**This is normal!** These assets just load your existing CSV/text files from `data/01_raw/` and cache them in Dagster's internal storage for efficient access by downstream assets.

### 📁 **Experiment Output Storage (What You Care About)**
Your actual experiment results are saved to the `data/` directory structure you're familiar with:

**After running each asset group, check these locations:**

```bash
# After: +group:daydreaming_experiment
ls -la data/02_tasks/
# You'll see: concept_combinations_combinations.csv, generation_tasks.csv, etc.

# After: +group:llm_generation  
ls -la data/03_generation/
# You'll see: generation_prompts/*.txt, generation_responses/*.txt

# After: +group:llm_evaluation
ls -la data/04_evaluation/
# You'll see: evaluation_prompts/*.txt, evaluation_responses/*.txt

# After: +group:results_processing
ls -la data/05_parsing/ data/06_summary/
# You'll see: parsed_scores.csv, final_results.csv
```

### 🎯 **Storage Summary**

| Asset Type | Storage Location | Purpose |
|------------|------------------|---------|
| **Raw Data** | Dagster's temp storage | Cached loaded files (internal optimization) |
| **Experiment Tasks** | `data/02_tasks/` | Task definitions you can inspect |
| **LLM Prompts** | `data/03_generation/generation_prompts/` | Individual prompt files for debugging |
| **LLM Responses** | `data/03_generation/generation_responses/` | Individual response files (cached!) |
| **Evaluation** | `data/04_evaluation/` | Evaluation prompts and responses |
| **Final Results** | `data/05_parsing/`, `data/06_summary/` | Parsed scores and aggregated results |

### 💡 **Key Benefits of This Approach**

1. **Caching**: If interrupted, only missing LLM responses are generated
2. **Debugging**: Each prompt/response is a separate file you can inspect
3. **Familiar Structure**: Same `data/` layout as your original Kedro pipeline
4. **Efficiency**: Raw data is loaded once and reused by all downstream assets