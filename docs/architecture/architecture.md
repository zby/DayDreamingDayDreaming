# DayDreaming Pipeline Architecture

This document provides a detailed technical overview of the DayDreaming Dagster pipeline architecture, including system design, data flow, and implementation details.

## System Overview

The DayDreaming pipeline is built on **Dagster**, a modern data orchestration platform that provides asset-based dependency management, partitioned processing, and rich observability features.

### Core Architectural Principles

1. **Asset-Based Architecture**: Each data artifact is modeled as a Dagster asset with explicit dependencies
2. **Dynamic Partitioning**: LLM tasks are processed as individual partitions for granular caching and recovery
3. **Dependency Injection**: Resources are injected for testability and configurability
4. **Human-Readable Storage**: All outputs stored as CSV/text files for debugging and analysis
5. **Selective Loading**: Configurable filtering for development and focused experiments

## Architecture Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────────────┐
│   Raw Data      │    │  Task Generation │    │      Two-Phase LLM Generation   │
│   (01_raw/)     │───▶│   (02_tasks/)    │───▶│      (03_generation/)           │
│                 │    │                  │    │                                 │
│ • Concepts      │    │ • Combinations   │    │ Phase 1: Links Generation       │
│ • Templates     │    │ • Generation     │    │ ┌─────────────────────────────┐ │
│   - links/      │    │   Tasks          │    │ │ links_prompt → links_response│ │
│   - essay/      │    │ • Evaluation     │    │ │ (6-12 concept connections)  │ │
│ • Models        │    │   Tasks          │    │ └─────────────┬───────────────┘ │
└─────────────────┘    └─────────────────┘    │               │                 │
                                              │ Phase 2: Essay Generation      │
                              ┌───────────────┤ ┌─────────────▼───────────────┐ │
                              │ Evaluation    │ │ essay_prompt → essay_response│ │
                              │ (04_eval/)    │ │ (using links as inspiration) │ │
                              │               │ └─────────────┬───────────────┘ │
                              │ • Partitioned │ │             │                 │
                              │   by Task ID  │ │ canonical_generation_response │
                              │ • Cached      │ └─────────────┬───────────────┘ │
                              │   Responses   │               │                 │
                              └───────────────┘               │                 │
                                       │                      │                 │
                              ┌─────────────────┐            │                 │
                              │ Results         │◀───────────┼─────────────────┘
                              │ (05_parsing/    │            │
                              │  06_summary/)   │            │
                              │                 │            │
                              │ • Parsed Scores │            │
                              │ • Final Results │◀───────────┘
                              └─────────────────┘
```

## Dagster Implementation Structure

### Assets Organization

```
daydreaming_dagster/
├── assets/                     # Dagster assets (data pipeline components)
│   ├── raw_data.py            # Raw data loading assets (supports selective loading)
│   ├── core.py                # Core processing assets (combinations, tasks)
│   ├── partitions.py          # Partition management assets
│   ├── two_phase_generation.py # Two-phase LLM generation assets (NEW)
│   ├── llm_generation.py      # Legacy single-phase LLM generation assets
│   ├── parsed_generation.py   # Canonical interface for generation responses (NEW)
│   ├── llm_evaluation.py      # LLM evaluation assets
│   ├── results_processing.py  # Score parsing and analysis
│   ├── results_summary.py     # Final aggregated results
│   ├── results_analysis.py    # Statistical analysis assets
│   └── cross_experiment.py    # Cross-experiment tracking
├── utils/                      # Utility modules
│   ├── template_loader.py     # Phase-aware template loading (NEW)
│   ├── eval_response_parser.py # Evaluation response parsing
│   └── generation_response_parser.py # Generation response parsing
├── resources/                  # Dagster resources
│   ├── llm_client.py          # LLM API client resource
│   ├── experiment_config.py   # Experiment configuration resource (with filtering)
│   ├── io_managers.py         # Custom I/O managers for different file types
│   └── cross_experiment_io_manager.py # Cross-experiment I/O manager
├── definitions.py             # Single Dagster definitions file
└── __init__.py                # Package initialization
```

### Asset Groups

Assets are organized into logical groups for easy selection and understanding:

| Group | Assets | Purpose |
|-------|--------|---------|
| **`raw_data`** | concepts, llm_models, link_templates, essay_templates, evaluation_templates | Load external data files |
| **`task_definitions`** | content_combinations, content_combinations_csv, link_generation_tasks, essay_generation_tasks, evaluation_tasks | Generate LLM task definitions (split by phase) |
| **`generation_links`** 🚀 | links_prompt, links_response | Phase 1: link generation (partitioned by link_task_id) |
| **`generation_essays`** 🚀 | essay_prompt, essay_response | Phase 2: essay generation (partitioned by essay_task_id) |
| **`evaluation`** | evaluation_prompt, evaluation_response | LLM evaluation (partitioned by evaluation_task_id) |
| **`results_processing`** | parsed_scores | Parse evaluation scores |
| **`results_summary`** | final_results, perfect_score_paths, generation_scores_pivot, evaluation_model_template_pivot | Final aggregated results |
| **`results_analysis`** | evaluator_agreement_analysis, comprehensive_variance_analysis | Statistical analysis |
| **`cross_experiment`** | filtered_evaluation_results, template_version_comparison_pivot | Cross-experiment analysis |
| **`cross_experiment_tracking`** | generation_results_append, evaluation_results_append | Auto-materializing result tracking |

## Data Flow Architecture

### 1. Raw Data Loading (`raw_data.py`) + Observable Sources (NEW)

**Assets**: `concepts`, `llm_models`, `link_templates`, `essay_templates`, `evaluation_templates`
 plus observable source assets: `raw_concepts_source`, `llm_models_source`, `link_templates_source`, `essay_templates_source`, `evaluation_templates_source`.

**Purpose**: Load and validate external data files from `data/1_raw/`.
Changes are detected via observable source assets that fingerprint the corresponding files/directories. When fingerprints change, Dagster marks downstream raw assets stale and (with eager policy) re-materializes them automatically.

**Key Features**:
- **Selective Loading**: Filter concepts and templates based on configuration
- **Validation**: Ensure required fields and structure
- **Metadata Extraction**: Generate searchable metadata for concepts
- **Template Processing**: Load and validate Jinja2 templates
 - **Auto-Update**: Timestamp/content fingerprinting triggers automatic re-materialization when inputs change

**Implementation Details**:
```python
# Observable source (computes data_version from mtime hash)
@observable_source_asset(name="raw_concepts_source")
def raw_concepts_source(context):
    ...  # fingerprint data/1_raw/concepts/**/*

# Raw asset depends on the observable source and uses eager auto-materialization
@asset(group_name="raw_data", deps=[AssetKey("raw_concepts_source")], auto_materialize_policy=AutoMaterializePolicy.eager())
def concepts(context) -> List[Concept]:
    ...  # load from CSV and description files
```

LLM generation assets (links/essays/evaluation) remain manual to avoid surprise API usage/costs.

### 2. Core Processing (`core.py`)

**Assets**: `content_combinations`, `generation_tasks`, `evaluation_tasks`

**Purpose**: Generate experiment structure and task definitions

**Data Flow**:
1. **Concept Combinations**: Generate k_max-sized combinations from concepts
2. **Generation Tasks**: Cross-product combinations with templates and models
3. **Evaluation Tasks**: Cross-product generation tasks with evaluation templates and models

**Key Features**:
- **Combinatorial Generation**: Efficient k-sized combination generation
- **Task Hierarchies**: Structured task dependencies (generation → evaluation)
- **CSV Output**: Human-readable task definitions for debugging

**Search Strategy**: The experiment currently uses a **focused search strategy** that tests only k_max-sized concept combinations (e.g., if k_max=4, only 4-concept combinations are tested). This approach is based on the insight that richer contextual combinations are more likely to elicit the complex DayDreaming concept from LLMs.

**Strategy Benefits**:
- **Higher success probability**: More concepts provide richer semantic context
- **More efficient**: Avoids testing smaller, less informative combinations  
- **Focused discovery**: Concentrates computational resources on the most promising combinations

**Example**: With 6 concepts and k_max=4:
- **Current strategy**: Tests C(6,4) = 15 combinations
- **Alternative strategies**: Could test C(6,1) + C(6,2) + C(6,3) + C(6,4) = 6+15+20+15 = 56 combinations

### 3. Partition Management (`partitions.py`)

**Assets**: `task_definitions`

**Purpose**: Create dynamic partitions from generated tasks

**Critical Function**: This asset reads the CSV task files and registers each task ID as a dynamic partition in Dagster, enabling partitioned LLM processing.

**Why This Is Required**:
- Dagster's dynamic partitions don't exist until explicitly created
- Each LLM task becomes an independent partition for caching and recovery
- Partitions must be registered before partitioned assets can be materialized

### 4. Two-Phase LLM Generation (`two_phase_generation.py`) 🚀 **NEW**

**Assets**: `links_prompt`, `links_response`, `essay_prompt`, `essay_response`, `canonical_generation_response`

**Purpose**: Execute LLM generation using a two-phase approach for improved quality and consistency

**Architecture Pattern**: Uses "Manual IO with Foreign Keys" pattern for cross-phase data loading

**Phase 1 - Links Generation**:
```python
# links_prompt: Generate brainstorming prompts using links/ templates
# links_response: Generate 6-12 conceptual connection bullet points
```

**Phase 2 - Essay Generation**:
```python
# essay_prompt: Create essay prompts with links_response as inspiration
# essay_response: Generate 1500-3000 word essays based on selected links
# canonical_generation_response: Standardized interface for downstream consumption
```

**Key Features**:
- **Separated Concerns**: Creative brainstorming vs. structured composition
- **Quality Validation**: Phase 2 fails if Phase 1 produces < 3 usable links
- **Cross-Phase Loading**: Essay prompts access links responses using MockLoadContext pattern
- **Rich Error Handling**: Comprehensive metadata and resolution steps
- **Backward Compatibility**: `parsed_generation_responses` provides passthrough interface

**Template Structure**:
```
data/1_raw/generation_templates/
├── links/                    # Phase 1 templates
│   ├── creative-synthesis-v7.txt
│   └── systematic-analytical.txt
└── essay/                    # Phase 2 templates
    ├── creative-synthesis-v7.txt
    └── systematic-analytical.txt
```

Active Link Template (Problem‑First):
- `recursive_construction` (file: `data/1_raw/generation_templates/links/recursive_construction.txt`, selectable via `data/1_raw/link_templates.csv`).
- Approach: C1 is normalized into a one‑line Problem; for each subsequent concept (fixed order, no swaps), the model may lightly operationalize it (Transformation Gate) and attach it to the evolving graph using the existing 1–5 link decision tree. If a coupling yields a new capability, it instantiates an `E::…` mechanism (Node‑Introduction Gate) and records local rewires. Each step includes a micro‑test and an adjacency delta; a final Reflections section suggests better ordering without changing the built graph.


**Data Flow**:
1. `links_prompt` loads `links/template.txt` and renders with concepts
2. `links_response` generates conceptual connections (6-12 bullets)
3. `essay_prompt` loads links response + `essay/template.txt`, validates links (≥3)
4. `essay_response` generates comprehensive essay using links as inspiration
5. `canonical_generation_response` provides standardized output format

### 5. Legacy LLM Processing (`llm_generation.py`)

**Assets**: `generation_prompt`, `generation_response`, `parsed_generation_responses`

**Purpose**: Single-phase LLM generation (maintained for backward compatibility)

**Partitioning Strategy**:
- **Generation**: Partitioned by `{combo_id}_{template}_{model}`
- **Evaluation**: Partitioned by `{essay_task_id}_{eval_template}_{eval_model}`

**Key Features**:
- **Individual File Storage**: Each prompt/response stored as separate files
- **Automatic Caching**: Failed tasks can be rerun without affecting completed ones
- **API Error Handling**: Structured error logging and recovery
- **Rate Limiting**: Built-in throttling for API calls

### 6. Results Processing (`results_processing.py`)

**Assets**: `parsed_scores`, `final_results`

**Purpose**: Parse LLM responses and generate final analysis

**Processing Pipeline**:
1. **Sequential File Processing**: Read evaluation response files one at a time to avoid memory issues
2. **Multi-Format Score Extraction**: Automatically detect and parse various LLM response formats
3. **Strategy Selection**: Choose parsing strategy based on evaluation template type
4. **Metadata Enhancement**: Add task metadata (combo, template, model info) via DataFrame joins
5. **Aggregation**: Calculate summary statistics and perfect score analysis
6. **CSV Output**: Structured results for further analysis

**Parser Capabilities**:
- **Multi-line detection**: Searches last 3 non-empty lines for scores
- **Format support**: Handles markdown (`**SCORE: 7**`), plain text, and various separators
- **Score formats**: Standard numeric (8.5) and three-digit averages (456 → 5.0)
- **Error handling**: Continues processing when individual files fail to parse

### 7. Cross-Experiment Tracking (`cross_experiment.py`)

**Assets**: `link_generation_results_append`, `essay_generation_results_append`, `evaluation_results_append`, `filtered_evaluation_results`, `template_version_comparison_pivot`

**Purpose**: Automatic tracking and cross-experiment analysis of all LLM responses

**Auto-Materializing Tracking**:
- **`link_generation_results_append`**: Automatically appends rows to `link_generation_results.csv` when any `links_response` completes
- **`essay_generation_results_append`**: Automatically appends rows to `essay_generation_results.csv` when any `essay_response` completes
- **`evaluation_results_append`**: Automatically appends rows to `evaluation_results.csv` when any `evaluation_response` completes
- **Thread-safe CSV operations**: Uses file locking to handle concurrent updates
- **Immediate execution**: Uses `AutoMaterializePolicy.eager()` for instant updates

**Tracking Tables**:
- **Link Generation Results**: Metadata for link generation
  - Columns: `link_task_id`, `combo_id`, `link_template_id`, `generation_model`, `generation_status`, `generation_timestamp`, `response_file`, `response_size_bytes`
- **Essay Generation Results**: Metadata for essay generation
  - Columns: `essay_task_id`, `combo_id`, `essay_template_id`, `generation_model`, `generation_status`, `generation_timestamp`, `response_file`, `response_size_bytes`
- **Evaluation Results**: Complete evaluation metadata with essay context
  - Columns: `evaluation_task_id`, `essay_task_id`, `combo_id`, `link_template`, `essay_template`, `generation_model`, `evaluation_template`, `evaluation_model`, `evaluation_status`, `evaluation_timestamp`, `eval_response_file`, `eval_response_size_bytes`

**Cross-Experiment Analysis**:
- **Template comparison**: Compare different template versions across experiments
- **Model performance**: Track model behavior across different concept combinations
- **Filtering capabilities**: Configurable filters for focused analysis

**Implementation Details**:
```python
@asset(
    partitions_def=link_tasks_partitions,
    deps=["links_response"],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    group_name="cross_experiment_tracking"
)
def link_generation_results_append(context, link_generation_tasks):
    # Automatically appends when links_response completes
    append_to_results_csv("data/7_cross_experiment/link_generation_results.csv", new_row)
```

**Bulk Table Generation**:
- **Scripts**: `scripts/build_generation_results_table.py`, `scripts/build_evaluation_results_table.py`
- **Purpose**: Initial migration and table rebuilding from existing response files
- **Thread-safe utility**: `append_to_results_csv()` function with file locking

## Resource Architecture

### 1. LLM Client Resource (`llm_client.py`)

**Purpose**: Configurable LLM API client with provider abstraction

**Features**:
- **Multi-Provider Support**: OpenRouter, direct API endpoints
- **Retry Logic**: Exponential backoff for failed requests
- **Rate Limiting**: Configurable request throttling
- **Error Handling**: Structured error responses and logging

**Implementation Pattern**:
```python
@resource(config_schema={"api_key": str, "base_url": str})
def llm_client_resource(context) -> LLMClientResource:
    return LLMClientResource(
        api_key=context.resource_config["api_key"],
        base_url=context.resource_config["base_url"]
    )
```

### 2. Experiment Configuration (`experiment_config.py`)

**Purpose**: Centralized parameter management with selective loading

**Configuration Options**:
- `k_max`: Maximum concept combination size
- `description_level`: Concept description detail level
- `template_names_filter`: Optional template filtering

**Selective Loading Benefits**:
- **Development Speed**: Test with small data subsets
- **Focused Experiments**: Test specific concept/template combinations
- **Resource Conservation**: Reduce API costs during development

### 3. I/O Managers (`io_managers.py`)

**Purpose**: Custom storage managers for different data types

**Managers**:
- **TextFileIOManager**: Individual text files for prompts/responses
- **CSVIOManager**: Structured data with human-readable format
- **JSONIOManager**: Complex objects with structured format

**Benefits**:
- **Debugging**: Easy inspection of intermediate results
- **Version Control**: Text-based formats for diff tracking
- **Portability**: Standard formats for external tool integration

## Partitioning Architecture

### Dynamic Partitioning Strategy

The pipeline uses Dagster's dynamic partitioning to create fine-grained, recoverable processing:

1. **Task Definition Phase**:
   ```python
   # task_definitions asset creates partitions
   context.instance.add_dynamic_partitions(
       partition_def_name="generation_tasks_partition",
       partition_keys=list(task_ids)
   )
   ```

2. **Partitioned Asset Execution**:
   ```python
   @asset(partitions_def=generation_tasks_partition)
   def generation_response(context, generation_prompt):
       partition_key = context.partition_key
       # Process specific task
   ```

### Benefits of This Architecture

1. **Granular Caching**: Each LLM call cached independently
2. **Fault Tolerance**: Failed partitions don't affect successful ones  
3. **Parallel Processing**: Independent tasks can run concurrently
4. **Incremental Execution**: Only missing partitions are materialized
5. **Progress Tracking**: UI shows per-partition status

### Partition Key Structure

**Generation Tasks**: `{combo_id}_{template}_{model}`
- Stable combo IDs follow `combo_v1_<12-hex>` (e.g., `combo_v1_1f3a9c2d7b2c_02_problem_solving_deepseek`).
- Legacy format `combo_XXX_...` remains supported in older artifacts/tests.

**Evaluation Tasks**: `{essay_task}_{eval_template}_{eval_model}/{eval_model_version}`
- Example with stable ID: `combo_v1_1f3a9c2d7b2c_02_problem_solving_deepseek_creativity_metrics_deepseek/deepseek-r1:free`

## Storage Architecture

### File System Organization

```
data/
├── 01_raw/                    # External inputs only
│   ├── concepts/
│   │   ├── day_dreaming_concepts.json
│   │   └── descriptions/       # Concept descriptions by level
│   ├── generation_templates/   # Jinja2 prompt template files
│   │   ├── links/              # Phase 1 template files
│   │   └── essay/              # Phase 2 template files
│   ├── evaluation_templates/   # Evaluation prompt templates
│   ├── link_templates.csv      # Link-phase template metadata and active selection
│   ├── essay_templates.csv     # Essay-phase template metadata and active selection
│   ├── generation_models.csv   # Available models with active selection
│   └── evaluation_models.csv   # Available evaluation models
├── 02_tasks/                   # Generated task definitions
│   ├── concept_combinations_combinations.csv
│   ├── concept_combinations_relationships.csv
│   ├── generation_tasks.csv
│   ├── evaluation_tasks.csv
│   └── concept_contents/       # Individual concept content files
├── 03_generation/              # LLM generation results
│   ├── generation_prompts/     # Prompts sent to generator LLM
│   └── generation_responses/   # Raw generator responses
├── 04_evaluation/              # LLM evaluation results
│   ├── evaluation_prompts/     # Prompts sent to evaluator LLM
│   └── evaluation_responses/   # Raw evaluator responses
├── 05_parsing/                 # Parsed evaluation scores
│   └── parsed_scores.csv       # Extracted scores with metadata
├── 06_summary/                 # Final aggregated results
│   └── final_results.csv       # Final aggregated results
├── combo_mappings.csv          # Global append-only mapping of stable combo IDs to concepts
└── 07_reporting/               # Error logs and reporting
```

### Storage Design Principles

1. **Human-Readable**: All outputs in CSV/text for easy inspection
2. **Granular**: Individual files for each prompt/response for debugging
3. **Hierarchical**: Clear directory structure reflecting data flow
4. **Cacheable**: File-based storage enables Dagster caching
5. **Portable**: Standard formats for external tool integration

### Overwrite and Retention Policy

- Task CSVs in `data/2_tasks/` are rewritten by their assets on materialization.
- Dynamic partitions for tasks are cleared and recreated when `generation_tasks`/`evaluation_tasks` run.
- Prompts are allowed to overwrite to reflect current templates.
- Responses are write-once by default; existing response files will not be overwritten. Delete files to regenerate or change the partition key.

## Performance and Scalability

### Concurrency Architecture

The pipeline is designed for efficient concurrent processing:

1. **Asset-Level Parallelism**: Independent assets run concurrently
2. **Partition-Level Parallelism**: Independent partitions can be processed simultaneously
3. **Resource Pooling**: Shared LLM client resource across partitions
4. **Rate Limiting**: Configurable throttling to respect API limits

### Scalability Features

1. **Selective Loading**: Scale down for development/testing
2. **Incremental Processing**: Only process missing/failed partitions
3. **Resource Management**: Configurable resource limits and timeouts
4. **Error Isolation**: Failed partitions don't affect successful ones

### Performance Optimization

1. **Template Caching**: Jinja2 templates compiled once and reused
2. **Metadata Precomputation**: Concept metadata calculated once
3. **Sequential Processing**: Files processed one at a time to avoid memory constraints
4. **Efficient Storage**: CSV format for structured data, text for content
5. **Lazy Loading**: Data loaded only when needed by downstream assets
6. **Configurable I/O Paths**: All file access through I/O managers for flexibility

## Error Handling and Recovery

### Error Handling Strategy

1. **Asset-Level**: Each asset handles its specific error conditions
2. **Partition-Level**: Individual partitions can fail without affecting others
3. **Resource-Level**: LLM client handles API errors with retry logic
4. **System-Level**: Dagster provides monitoring and alerting

### Recovery Mechanisms

1. **Automatic Retry**: Failed API calls retried with exponential backoff
2. **Partial Recovery**: Failed partitions can be rerun individually
3. **State Preservation**: Successful partitions remain cached
4. **Error Logging**: Structured error information for debugging

### Monitoring and Observability

1. **Dagster UI**: Real-time asset and partition status
2. **Structured Logging**: Detailed logs for debugging
3. **Asset Lineage**: Visual dependency tracking
4. **Performance Metrics**: Execution time and resource usage tracking

## Evaluation Asset Architecture

### Partition Relationship Challenge

The `evaluation_prompt` asset needs to consume content from the `generation_response` asset, but they use different partition schemes:

- `essay_response`: Partitioned by `essay_task_id`
- `evaluation_prompt`: Partitioned by `evaluation_task_id` 

The relationship between them is established through a foreign key (`essay_task_id`) stored in the evaluation tasks table.

### Solution: Manual IO with Foreign Keys

Rather than using complex multi-dimensional partitions, we use a simpler approach:

1. **Foreign Key Lookup**: `evaluation_prompt` reads its partition's row from `evaluation_tasks` to get the associated `essay_task_id`
2. **Manual IO Loading**: Uses the IO manager directly to load the `essay_response` content by `essay_task_id`
3. **MockLoadContext Pattern**: Creates a temporary context object to interface with the IO manager

### Implementation Pattern

```python
class MockLoadContext:
    def __init__(self, partition_key):
        self.partition_key = partition_key

mock_context = MockLoadContext(essay_task_id)
essay_response = essay_response_io_manager.load_input(mock_context)
```

This pattern allows us to load data from a different partition key than the current asset's partition.

### Why This Approach?

**Advantages:**
- **Simplicity**: Single-dimension partitions are easier to understand and debug
- **Explicit Data Flow**: The FK relationship is visible in the evaluation tasks table
- **Flexible**: Easy to change partition mapping logic without touching partition definitions
- **Testable**: Can easily mock IO managers for testing
- **Performance**: Only loads the specific essay response needed

**Trade-offs:**
- **Manual IO**: Requires explicit IO manager usage instead of Dagster inputs
- **No UI Dependency Edge**: Dagster UI doesn't show the data dependency automatically
- **Error Handling**: Need custom error handling for missing upstream data

## Migration Benefits from Kedro

### Technical Improvements

1. **Asset Lineage**: Visual dependency tracking vs. implicit pipeline dependencies
2. **Partitioned Processing**: Fine-grained caching vs. monolithic pipeline runs
3. **Dynamic Partitioning**: Runtime partition creation vs. static configuration
4. **Web UI**: Rich monitoring vs. command-line only
5. **Python 3.13 Compatibility**: Fixed ANTLR4 issues that affected Kedro

### Operational Benefits

1. **Interruption Recovery**: Resume from exact failure point vs. full restart
2. **Parallel Execution**: Asset-level parallelism vs. sequential pipeline
3. **Selective Materialization**: Run specific assets/partitions vs. full pipeline
4. **Resource Management**: Shared resources across assets vs. per-node isolation

## Future Architecture Considerations

### Potential Enhancements

1. **Multi-Node Processing**: Distribute partitions across multiple machines
2. **Cloud Storage**: Migrate to S3/GCS for larger scale deployments
3. **Database Integration**: Add metadata database for advanced querying
4. **Streaming**: Real-time processing for continuous experiment updates
5. **ML Pipelines**: Integration with ML training and inference pipelines

### Extensibility Points

1. **New LLM Providers**: Add support for additional API providers
2. **Custom Evaluation**: Extend evaluation templates and scoring methods
3. **Data Sources**: Add support for additional concept/template sources
4. **Output Formats**: Support additional output formats (JSON, Parquet, etc.)
5. **Monitoring**: Integration with external monitoring systems
