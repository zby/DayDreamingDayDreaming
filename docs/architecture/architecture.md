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
┌──────────────┐
│ Raw (01_raw) │  concepts, templates, models
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│ Task Definitions (02_tasks)                                   │
│ • generation_tasks = combos × gen_templates × gen_models      │
│ • evaluation_tasks = documents × eval_templates × eval_models │
└───────────────┬───────────────────────────────┬───────────────┘
                │                               │
                ▼                               ▼
┌───────────────────────────────┐        ┌───────────────────────────────┐
│ Generation (03_generation)    │ ─────► │ Evaluation (04_evaluation)    │
│ generation_prompt → response  │        │ evaluation_prompt → response  │
└──────────────┬────────────────┘        └──────────────┬────────────────┘
               │                                        ▲
               └─ documents = generation_responses      │ joined by file_path
                          ∪ curated historical ─────────┘

┌───────────────────────────────┐
│ Parsing & Summary (05, 06)    │  score parsing, aggregation
└───────────────────────────────┘
```

Dimensionality (axes)
- Generation: `combos × generation_templates × generation_models → draft documents`.
- Evaluation: `documents × evaluation_templates × evaluation_models → evaluator outputs`.

## Dagster Implementation Structure

### Assets Organization

```
daydreaming_dagster/
├── assets/                     # Dagster assets (data pipeline components)
│   ├── raw_data.py             # Raw data loading assets
│   ├── partitions.py           # Partition management assets
│   ├── results_processing.py   # Score parsing and analysis
│   ├── results_summary.py      # Final aggregated results
│   ├── results_analysis.py     # Statistical analysis assets
│   ├── cross_experiment.py     # Cross-experiment tracking
│   └── groups/                 # Grouped assets by domain
│       ├── group_task_definitions.py
│       ├── group_generation_draft.py
│       ├── group_generation_essays.py
│       ├── group_evaluation.py
│       ├── group_results_processing.py
│       ├── group_results_summary.py
│       └── group_cross_experiment.py
├── utils/                      # Utility modules
│   ├── template_loader.py      # Phase-aware template loading
│   ├── draft_parsers.py        # Parser registry for Phase‑1 extraction
│   ├── eval_response_parser.py # Evaluation response parsing
│   └── dataframe_helpers.py    # Small helpers for DataFrame lookups
├── resources/                  # Dagster resources
│   ├── llm_client.py           # LLM API client resource
│   ├── experiment_config.py    # Experiment configuration resource
│   └── io_managers.py          # Custom I/O managers
├── definitions.py              # Dagster Definitions (entrypoint)
└── __init__.py                 # Package initialization
```

### Asset Groups

Assets are organized into logical groups for easy selection and understanding:

| Group | Assets | Purpose |
|-------|--------|---------|
| **`raw_data`** | concepts, llm_models, draft/essay/evaluation templates | Load external data files |
| **`task_definitions`** | content_combinations, draft_generation_tasks, essay_generation_tasks, evaluation_tasks | Build partitioned tasks from active CSVs |
| **`generation_draft`** 🚀 | draft_prompt, draft_response | Phase‑1 generation; applies parser (if configured) and saves RAW + parsed outputs |
| **`generation_essays`** | essay_prompt, essay_response | Phase‑2 generation; modes: `llm` (default) and `copy` (parsed draft passthrough) |
| **`evaluation`** | evaluation_prompt, evaluation_response | LLM evaluation (partitioned by evaluation_task_id) |
| **`results_processing`** | parsed_scores | Parse evaluation scores |
| **`results_summary`** | final_results, perfect_score_paths, generation_scores_pivot, evaluation_model_template_pivot | Final aggregated results |
| **`results_analysis`** | evaluator_agreement_analysis, comprehensive_variance_analysis | Statistical analysis |
| **`cross_experiment`** | filtered_evaluation_results, template_version_comparison_pivot | Cross-experiment analysis |

## Data Flow Architecture

### 1. Raw Data Loading (`raw_data.py`)

**Assets**: `concepts`, `llm_models`, `generation_templates`, `evaluation_templates`

**Purpose**: Load and validate external data files from `data/1_raw/`.
Note: Observable source assets were removed for simplicity during development. When inputs change, re‑materialize the raw loader assets to refresh downstream tasks.

**Key Features**:
- **Selective Loading**: Filter concepts and templates based on configuration
- **Validation**: Ensure required fields and structure
- **Metadata Extraction**: Generate searchable metadata for concepts
- **Template Processing**: Load and validate Jinja2 templates
 - **Manual Refresh**: Re‑materialize `group:raw_data` after editing files under `data/1_raw/**` to propagate changes

**Implementation Details**:
```python
# Raw asset uses eager auto-materialization
@asset(group_name="raw_data", automation_condition=AutomationCondition.eager())
def concepts(context) -> List[Concept]:
    ...  # load from CSV and description files
```

LLM generation/evaluation assets remain manual to avoid surprise API usage/costs.

### 2. Core Processing (`core.py`)

**Assets**: `content_combinations`, `generation_tasks`, `evaluation_tasks`

**Purpose**: Generate experiment structure and task definitions

**Data Flow**:
1. **Concept Combinations**: Generate k_max-sized combinations from concepts or consume a `selected_combo_mappings` DataFrame.
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

### 4. Two‑Phase LLM Generation

Two assets groups implement the two‑phase flow:

1) Phase‑1 — Draft Generation (`group_generation_draft.py`)
   - Assets: `draft_prompt`, `draft_response` (partitioned by `gen_id`).
   - Behavior: Calls the LLM, writes prompt/raw/parsed/metadata to `data/gens/draft/<gen_id>/` (parser from `data/1_raw/draft_templates.csv`, identity when missing). On parser failure, the asset fails with a clear error; RAW remains saved in the gens store.

2) Phase‑2 — Essay Generation (`group_generation_essays.py`)
   - Assets: `essay_prompt`, `essay_response` (partitioned by `gen_id`).
   - Modes: `llm` (default; uses parsed draft as input) and `copy` (returns parsed draft verbatim). Essay‑level parser mode is deprecated after parser‑first.
   - Essay templates live under `data/1_raw/generation_templates/essay/` and typically include a placeholder like `{{ links_block }}` / `{{ draft_block }}` to include the Phase‑1 text in prompts.
   - Behavior: Writes prompt/raw/parsed/metadata to `data/gens/essay/<gen_id>/`; loads the parent draft via `parent_gen_id` from `data/gens/draft/<parent>/parsed.txt`.

**Template Structure**:
```
data/1_raw/generation_templates/
├── draft/   # Phase‑1 templates
└── essay/   # Phase‑2 templates
```

### 6. Results Processing (`results_processing.py`)

**Assets**: `parsed_scores`, `final_results`

**Purpose**: Parse LLM responses and generate final analysis

**Processing Pipeline**:
1. **Sequential File Processing**: Read evaluation response files one at a time to avoid memory issues
2. **Multi-Format Score Extraction**: Automatically detect and parse various LLM response formats
3. **Strategy Selection**: Parsing strategy is driven by `data/1_raw/evaluation_templates.csv` column `parsing_strategy` (`in_last_line` or `complex`). If unset, defaults to `in_last_line`.
4. **Metadata Enhancement**: Add task metadata (combo, template, model info) via DataFrame joins
5. **Aggregation**: Calculate summary statistics and perfect score analysis
6. **CSV Output**: Structured results for further analysis

**Parser Capabilities**:
- **Multi-line detection**: Searches last 3 non-empty lines for scores
- **Format support**: Handles markdown (`**SCORE: 7**`), plain text, and various separators
- **Score formats**: Standard numeric (8.5) and three-digit averages (456 → 5.0)
- **Error handling**: Continues processing when individual files fail to parse

### 7. Cross-Experiment Analysis (`cross_experiment.py`)

**Assets**: `filtered_evaluation_results`, `template_version_comparison_pivot`

**Purpose**: Derive cross‑experiment views directly from the docs store and task CSVs (no auto‑appenders).

**Analysis**:
- Template comparison across experiments
- Model performance trends
- Light filtering hooks (expandable)

**Backfills and Tables**:
- Scripts: `scripts/aggregate_scores.py` (evaluation)
- Purpose: One‑off rebuilding from the gens store (no auto-appenders)
- Output: `data/7_cross_experiment/parsed_scores.csv` (canonical)

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
- **GensPromptIOManager**: Persists prompts to `data/gens/<stage>/<gen_id>/prompt.txt`
- **CSVIOManager**: Structured data with human-readable format
- **InMemoryIOManager**: For ephemeral in-process passing in tests/runs

**Benefits**:
- **Debugging**: Easy inspection of intermediate results
- **Version Control**: Text-based formats for diff tracking
- **Portability**: Standard formats for external tool integration

### 4. Storage Helpers

**Purpose**: Use the gens store as the single source of truth: `data/gens/<stage>/<gen_id>/{raw.txt,parsed.txt,prompt.txt,metadata.json}`.

- Prefer reading/writing via the gens store layout and metadata.
- Avoid versioned side-writes; create new `gen_id` partitions for new outputs.

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

**Evaluation Tasks**: `{generation_task}_{eval_template}_{eval_model}/{eval_model_version}`
- Example with stable ID: `combo_v1_1f3a9c2d7b2c_02_problem_solving_deepseek_creativity_metrics_deepseek/deepseek-r1:free`

## Storage Architecture

### File System Organization

```
data/
├── 1_raw/                      # External inputs only
│   ├── concepts/
│   ├── concepts_metadata.csv
│   ├── generation_templates/
│   │   ├── draft/              # Phase‑1 draft Jinja templates
│   │   └── essay/              # Phase‑2 essay Jinja templates
│   ├── draft_templates.csv     # Active draft templates (+ optional parser)
│   ├── essay_templates.csv     # Active essay templates (+ generator: llm|copy)
│   ├── evaluation_templates.csv
│   └── llm_models.csv          # Available models with flags: for_generation|for_evaluation
├── 2_tasks/                    # Generated task definitions
│   ├── content_combinations.csv
│   ├── draft_generation_tasks.csv
│   ├── essay_generation_tasks.csv
│   └── evaluation_tasks.csv
├── gens/                       # Canonical gens store (prompt/raw/parsed/metadata)
│   ├── draft/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}
│   ├── essay/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}
│   └── evaluation/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}
├── 3_generation/               # Legacy only (not used by current pipeline)
├── 5_parsing/                  # Parsed evaluation scores
│   └── parsed_scores.csv
├── 6_summary/                  # Final aggregated results
│   └── final_results.csv
└── combo_mappings.csv          # Mapping of stable combo IDs to concept components
```

### Storage Design Principles

1. **Human-Readable**: All outputs in CSV/text for easy inspection
2. **Granular**: Individual files for each prompt/response for debugging
3. **Hierarchical**: Clear directory structure reflecting data flow
4. **Cacheable**: File-based storage enables Dagster caching
5. **Portable**: Standard formats for external tool integration

### Overwrite and Retention Policy

- Task CSVs in `data/2_tasks/` are rewritten by their assets on materialization.
- Dynamic partitions for tasks are managed by the task definition assets (`draft_generation_tasks`, `essay_generation_tasks`, `evaluation_tasks`).
- Prompts are allowed to overwrite to reflect current templates.
- Runtime persistence uses the gens store: `data/gens/<stage>/<gen_id>` with metadata. Legacy directories under `data/3_generation/` are retained for historical analysis; scripts may read them for migration only. New runs write canonical artifacts to the gens store and do not create versioned files.

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

The evaluation flow is document-centric and decoupled from cross-partition IO. Key elements:

- `document_index` (Hybrid Document Axis): unified view of all evaluable documents with normalized columns.
- `evaluation_tasks`: builds dynamic partitions by taking a cross-product of selected documents × active evaluation templates × evaluation models.
- `evaluation_prompt`: reads the source document directly from a concrete `file_path` carried in each `evaluation_tasks` row and renders the evaluation template with `response=<document text>`.

### Unified Document Axis (document_index)

Each row is one evaluable document with explicit provenance and lookup fields:
- `document_id`, `stage` (`draft`), `origin` (`draft | legacy`), `file_path`
- `combo_id`, `generation_template`, `generation_model_id`, `generation_model_name`
- `generation_task_id`, `source_asset`, `source_dir`

Sources merged:
- Generated drafts from `data/3_generation/generation_responses/`
- Curated historical docs emitted via standard generation task CSVs and placed under canonical folders (no legacy directory scan).

### Evaluation Task Identity

- `evaluation_task_id = {gen_id}__{evaluation_template}__{evaluation_model_id}` (double-underscore separators)
- `evaluation_tasks` rows are denormalized with all document fields plus evaluation template/model names.

### Prompt Loading (Direct Path)

- `evaluation_prompt` loads document content via `file_path` (no FK hop to IO managers), renders the Jinja template, and records rich metadata including `source_asset`, `source_dir`, and content length.

### Results Processing

- `parsed_scores` parses evaluator responses; cross‑experiment aggregation reads `template_id` and `model_id` from metadata.json directly (no join required).
- Normalized outputs include `stage`, `origin`, `generation_response_path` (copied from document `file_path`), `source_asset`, `source_dir`.
- `generation_scores_pivot` pivots by `combo_id`, `stage`, `generation_template`, `generation_model`.

### Legacy Inputs

- Historical single‑phase artifacts may exist under `data/3_generation/generation_responses/`. The `document_index`/`evaluation_tasks` flow can incorporate them as one‑phase documents when building evaluation sets.

### Notes

- Evaluation reads source documents by path for uniformity across stages/sources.
- Legacy directory scanning has been removed from evaluation task discovery; curated inclusion is handled by writing standard generation task CSVs and placing files in canonical folders.

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
