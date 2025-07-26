# Daydreaming Experiment System Design

## Overview

The Daydreaming Experiment is a Python research system that tests whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept when provided with minimal contextual hints through a structured prompt DAG (Directed Acyclic Graph) approach.

## Core Hypothesis

The experiment aims to determine if language models can independently discover the concept of a continuous creative process involving random idea combination, criticism, and iterative refinement when given incremental contextual prompts organized in a hierarchical structure.

## System Architecture

### High-Level Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ConceptDAG    │───▶│ PromptGenerator │───▶│ExperimentExecutor│
│   (Storage)     │    │   (Templates)   │    │  (LLM Interface)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────┐                            ┌─────────────────┐
│  File Storage   │                            │   DataStorage   │
│  (Articles)     │                            │   (Results)     │
└─────────────────┘                            └─────────────────┘
```

### Data Flow

1. **Concept Creation**: Ideas are structured in a DAG with three levels (sentence, paragraph, article)
2. **Prompt Generation**: Templates extract concepts from DAG nodes to create contextual prompts
3. **Experiment Execution**: Prompts are sent to multiple LLM models via OpenRouter API
4. **Result Storage**: Responses are stored with metadata for analysis

## Core Modules

### 1. ConceptDAG (`concept_dag.py`)

**Purpose**: Manages hierarchical concept relationships with three representation levels.

**Key Classes**:
- `ConceptNode`: Individual concept with content, level, and parent relationships
- `ConceptDAG`: Graph structure managing nodes and their relationships
- `ConceptLevel`: Enum defining sentence/paragraph/article levels

**Features**:
- Hierarchical content storage (articles as files, shorter content in memory)
- DAG validation to prevent cycles
- Traversal methods for building full concepts from ancestors
- Persistence to/from disk

### 2. Configuration (`config.py`)

**Purpose**: Centralized configuration management.

**Components**:
- OpenRouter API key management
- Pre-June 2025 model list
- Reproducibility seed
- Scoring rubric for response evaluation

### 3. Experiment Execution (`execution.py`)

**Purpose**: Orchestrates the experiment across models and concept nodes.

**Key Class**: `ExperimentExecutor`

**Features**:
- OpenRouter API integration via OpenAI client
- Batch processing across models and concept nodes
- Error handling and logging
- Deterministic execution with fixed seeds

### 4. Data Storage (`storage.py`)

**Purpose**: Handles experiment result persistence and retrieval.

**Key Classes**:
- `ExperimentResult`: Data structure for individual results
- `DataStorage`: Storage management with JSON and CSV support

**Features**:
- Individual result files (JSON)
- Batch result aggregation (CSV)
- Timestamped storage
- Pandas integration for analysis

### 5. Prompt Generation (`prompt.py`)

**Purpose**: Creates contextual prompts from DAG concepts using templates.

**Key Classes**:
- `PromptTemplate`: Template with placeholders and concept requirements
- `PromptGenerator`: Generates prompts from DAG nodes
- `PromptTemplateLibrary`: Pre-built template collection

**Features**:
- Multi-level concept extraction (sentence/paragraph/article)
- Template-based prompt construction
- Missing concept validation
- Batch generation for leaf nodes

## Key Design Patterns

### 1. Hierarchical Concept Representation

Concepts are organized in three levels of detail:
- **Sentence**: Brief one-sentence description
- **Paragraph**: Moderate detail (200-500 characters)
- **Article**: Full detailed content (stored as files)

### 2. Template-Based Prompt Generation

Uses Python's `string.Template` for safe placeholder substitution:
```python
template = "Analyze this concept: $concept\nProvide insights about its implications."
```

### 3. Directed Acyclic Graph Structure

Ensures concepts build upon each other without circular dependencies:
- Parent-child relationships define concept hierarchies
- Leaf nodes represent the most specific/complex concepts
- Full concept construction traverses from leaf to root

### 4. API Abstraction

Uses OpenAI client library with OpenRouter as the base URL, providing:
- Unified interface across different LLM providers
- Consistent API patterns
- Built-in error handling

## Data Models

### ConceptNode
```python
@dataclass
class ConceptNode:
    id: str
    content: Union[str, Path]  # String or file path
    level: ConceptLevel
    parents: List[str]
    metadata: Dict
```

### ExperimentResult
```python
@dataclass
class ExperimentResult:
    model: str
    node_id: str
    prompt: str
    response: str
    seed: int
    timestamp: str
```

## File Organization

```
daydreaming_experiment/
├── __init__.py              # Package initialization
├── concept_dag.py           # Core DAG implementation
├── config.py                # Configuration settings
├── execution.py             # Experiment orchestration
├── storage.py               # Data persistence
├── prompt.py                # Prompt generation
└── test_*.py               # Unit tests (colocated)
```

## External Dependencies

- **openai**: LLM API client (used with OpenRouter)
- **pandas**: Data manipulation and CSV handling
- **numpy**: Data handling support
- **pytest**: Testing framework
- **json**: Built-in JSON serialization
- **pathlib**: Modern path handling

## Testing Strategy

- **Unit Tests**: Colocated with source files (`test_*.py`)
- **Integration Tests**: Located in separate `tests/` directory
- **Fixtures**: Test data stored in `tests/fixtures/`
- **Framework**: pytest with custom configurations

## Security and Reproducibility

- **API Keys**: Environment variable based (`OPENROUTER_API_KEY`)
- **Deterministic Execution**: Fixed seed for reproducible results
- **File Safety**: UTF-8 encoding, proper path handling
- **Error Handling**: Graceful degradation with error logging

## Extensibility Points

1. **New Models**: Add model identifiers to `PRE_JUNE_2025_MODELS`
2. **Custom Templates**: Extend `PromptTemplateLibrary`
3. **Alternative Storage**: Implement new storage backends
4. **Analysis Tools**: Build on `DataStorage` CSV exports
5. **Concept Levels**: Extend `ConceptLevel` enum for new granularities

## Limitations and Considerations

- **API Rate Limits**: No built-in rate limiting for OpenRouter calls
- **Memory Usage**: Large DAGs with article-level content may consume significant memory
- **Error Recovery**: Limited retry mechanisms for failed API calls
- **Concurrency**: Sequential processing may be slow for large experiments
- **Model Compatibility**: Assumes OpenRouter-compatible model identifiers

## Scoring and Evaluation

The system includes a rubric for evaluating LLM responses to determine if they "reinvent" the daydreaming loop concept:

- **Random Pair Generator**: Detection of combinatorial processes
- **Critic Filter**: Identification of evaluation mechanisms
- **Feedback Loop**: Recognition of iterative refinement
- **Background Process**: Bonus points for continuous operation aspects

This structured approach allows researchers to systematically test whether different models can independently arrive at similar creative process concepts when given graduated contextual hints.