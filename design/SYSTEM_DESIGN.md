# System Design: DayDreaming LLM Experiment

## System Goal

This system tests whether pre-June 2025 LLMs can "reinvent" Gwern's Day-Dreaming Loop concept when provided with minimal contextual hints through combinatorial testing of concept sets. The core hypothesis is that certain minimal combinations of concepts can elicit the Day-Dreaming idea from offline LLMs using automated evaluation.

## High-Level Architecture

### Design Philosophy

The system follows a **separation of concerns** approach with two distinct pipelines:

1. **Generation Pipeline**: Combinatorial concept testing → prompt generation → LLM responses
2. **Evaluation Pipeline**: Response analysis → automated rating → confidence scoring

This separation enables flexible experimentation where generation can happen independently of evaluation, allowing different evaluation strategies to be applied to the same generated responses.

### Core Architecture

```
ConceptDB → PromptFactory → ModelClient (Generation) 
                                ↓
                          Response Storage
                                ↓
                     EvaluationRunner → ResultsAnalysis
```

**Key Architectural Decisions**:
- **Exhaustive combinatorial search**: Test all concept combinations up to k_max size
- **Template-based prompt generation**: Jinja2 templates for consistent, extensible prompts
- **Separated generation/evaluation**: Enable independent experimentation with evaluation methods
- **File-based storage**: CSV results + individual response files for reliability and inspection

## Core Components

### ConceptDB (`concept_db.py`)
**Responsibility**: Concept storage and combinatorial generation engine

**Architecture**: Registry pattern with JSON manifest + external article files
- Loads concepts from `data/concepts/day_dreaming_concepts.json`
- Supports three granularity levels: sentence, paragraph, article
- Generates all k-sized combinations via `itertools.combinations`

**Key Design**: Flat concept structure (no DAG dependencies) for simplified combinatorial testing

### PromptFactory (`prompt_factory.py`) 
**Responsibility**: Template-based prompt generation

**Architecture**: Jinja2 template engine with file-based template loading
- Templates in `data/templates/*.txt` (loaded alphabetically)
- Template variables: `concepts` (list), `level` (string), `strict` (boolean)
- Renders concept combinations into structured prompts

**Key Design**: Template system enables prompt experimentation without code changes

### ModelClient (`model_client.py`)
**Responsibility**: LLM API abstraction layer

**Architecture**: Simple client wrapper for OpenRouter/OpenAI APIs
- `generate()`: Prompt → Response
- `evaluate()`: Evaluation prompt + response → (rating, confidence, reasoning)
- Environment-based configuration (API keys, base URLs)

**Key Design**: Minimal abstraction to enable easy model switching and testing

### ExperimentRunner (`experiment_runner.py`)
**Responsibility**: Main combinatorial testing pipeline

**Architecture**: CLI-driven experiment execution
- Generates all concept combinations up to k_max
- Tests each combination with all templates
- Supports both integrated (generation+evaluation) and generation-only modes
- Saves results to structured CSV + individual response files

**Key Design**: Stateless execution with complete result persistence for reproducibility

### EvaluationRunner (`evaluation_runner.py`)
**Responsibility**: Standalone evaluation pipeline

**Architecture**: Post-hoc evaluation of generated responses
- Loads existing experiment results and response files
- Applies evaluation templates via `EvaluationTemplateLoader`
- Supports different evaluator models and evaluation strategies
- Saves evaluation results separately from generation results

**Key Design**: Enables independent evaluation experimentation on existing response data

### EvaluationTemplateLoader (`evaluation_templates.py`)
**Responsibility**: Evaluation template management

**Architecture**: Jinja2-based evaluation prompt generation
- Templates in `data/evaluation_templates/*.txt`
- Template variable: `response` (generated text to evaluate)
- Default template selection logic (prefers 'iterative_loops')

## Data Flow & Storage

### Generation Flow
1. ConceptDB loads concepts from JSON manifest
2. PromptFactory generates all k-sized concept combinations
3. Each combination rendered with each template
4. ModelClient generates responses via LLM API
5. Results saved to CSV with individual response files

### Evaluation Flow
1. EvaluationRunner loads existing experiment data
2. EvaluationTemplateLoader renders evaluation prompts
3. ModelClient evaluates responses for Day-Dreaming concepts  
4. Evaluation results saved to separate CSV

### Storage Architecture
- **Concept data**: `data/concepts/day_dreaming_concepts.json` + `articles/*.txt`
- **Templates**: `data/templates/*.txt` (generation) + `data/evaluation_templates/*.txt` (evaluation)
- **Experiment results**: `experiment_YYYYMMDD_HHMMSS/` directories containing:
  - `config.json`: Experiment parameters
  - `results.csv`: Main results (crucial fields: `experiment_id`, `concept_names`, `automated_rating`, `confidence_score`)
  - `evaluation_results.csv`: Separate evaluation results (adds `evaluator_model`, `evaluation_template`)
  - `responses/`: Individual LLM response files

## Key Design Patterns

### Template-Driven Configuration
Both prompt generation and evaluation use Jinja2 templates stored as files, enabling experimentation without code changes. Templates are loaded alphabetically and accessed by index.

### Dependency Injection
Components accept dependencies as constructor parameters (e.g., ModelClient, ConceptDB) rather than creating them internally, enabling easy testing and configuration.

### Combinatorial Exhaustive Search
The system tests all possible concept combinations up to k_max size. For n concepts and k_max=4: C(n,1) + C(n,2) + C(n,3) + C(n,4) combinations.

### Separated Pipelines
Generation and evaluation are architecturally separate, enabling:
- Generation-only experiments for faster iteration
- Multiple evaluation strategies on the same responses
- Different evaluator models without re-generation

## Extension Points

### Adding New Concepts
Update `data/concepts/day_dreaming_concepts.json` with new concept entries. System automatically includes them in combinatorial generation.

### Custom Prompt Templates
Add `.txt` files to `data/templates/` using Jinja2 syntax. Templates loaded alphabetically by filename (use numeric prefixes for ordering).

### Custom Evaluation Methods
Add evaluation templates to `data/evaluation_templates/` or extend EvaluationRunner for different evaluation strategies.

### Alternative Search Strategies
Extend `ConceptDB.get_concept_combinations()` to implement heuristic or guided search instead of exhaustive combinatorial testing.

### Multi-Model Evaluation
Extend EvaluationRunner to support multiple evaluator models for cross-validation of Day-Dreaming concept detection.

## Debugging Guide

### Experiment Fails to Start
- **Check**: ConceptDB loading (`data/concepts/day_dreaming_concepts.json` exists and valid)
- **Check**: Template directory (`data/templates/` contains .txt files)
- **Check**: API credentials (environment variables set)

### Generation Issues
- **Component**: PromptFactory template rendering
- **Files**: Check `data/templates/*.txt` for Jinja2 syntax errors
- **API**: Verify model names and rate limiting in ModelClient

### Evaluation Issues  
- **Component**: EvaluationRunner + EvaluationTemplateLoader
- **Files**: Check `data/evaluation_templates/*.txt` exist
- **Data**: Verify experiment directory contains `results.csv` and `responses/` folder

### Performance Issues
- **Combinatorial explosion**: n concepts with k_max=4 generates C(n,1)+...+C(n,4) combinations
- **API rate limits**: Built-in delays, consider generation-only mode for large experiments
- **File I/O**: Thousands of small response files for large experiments

### Data Issues
- **Concept loading**: Check JSON syntax in manifest file
- **Template rendering**: Verify concept description levels exist
- **Result parsing**: Check CSV column structure matches expected schema

## Testing Architecture

The system uses separated unit tests (fast, mocked) and integration tests (data-dependent):

- **Unit tests**: `daydreaming_experiment/test_*.py` - Mock all external dependencies
- **Integration tests**: `tests/test_*.py` - Use real data files, mock only API calls

This separation enables fast development iteration (unit tests) while ensuring data compatibility (integration tests).

---

*This document focuses on architectural understanding. See individual module files for implementation details and CLAUDE.md for development workflow.*