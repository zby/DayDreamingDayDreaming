# DayDreaming LLMs Concept Experiment Design

## Overview

This system implements a simplified experimental framework to find the **minimal set of concepts** that can elicit Gwern's Day-Dreaming idea from offline LLMs. The design focuses on combinatorial testing with automated LLM-based evaluation.

## System Architecture

### 1. Core Data Models

#### Concept Structure
```python
@dataclass
class Concept:
    name: str
    sentence: Optional[str] = None      # 1-sentence summary
    paragraph: Optional[str] = None     # Multi-sentence description  
    article: Optional[str] = None       # Full comprehensive content
    article_path: Optional[str] = None  # External file for large articles
```

**Key Design Decisions:**
- All concepts have three granularity levels for future experiments
- **Current experiments use only paragraph level** for optimal combination testing
- Articles stored externally to keep manifest manageable
- Simple flat structure (no DAG dependencies)

#### ConceptDB
- Registry for all available concepts
- Provides batch retrieval and combination iteration
- Supports save/load with JSON manifest + article files
- Level-agnostic combination generation

#### Model Integration
```python
class SimpleModelClient:
    """Lightweight LLM client for experiment execution."""
    def generate(self, prompt: str) -> str:
        # Simple interface to LLM APIs for content generation
    
    def evaluate(self, prompt: str, response: str) -> tuple[bool, float, str]:
        # LLM-based evaluation returning (rating, confidence, reasoning)
```

### 2. Prompt Generation System

#### PromptFactory Class
```python
class PromptFactory:
    """Template-based prompt generation from concept combinations."""
    
    def __init__(self, templates: tuple[str, ...] = DEFAULT_TEMPLATES):
        self.templates = templates
    
    def generate_prompt(self, concepts: list[Concept], level: str, template_idx: int = 0) -> str:
        """Generate prompt by combining concepts at specified granularity level."""
        concept_texts = [getattr(concept, level) for concept in concepts]
        formatted_concepts = '\n'.join(f"- {text}" for text in concept_texts)
        return self.templates[template_idx].format(concepts=formatted_concepts)
```

#### Template System
```python
DEFAULT_TEMPLATES = (
"""Below are several background concepts:
 
 {concepts}
 
Can you combine ideas from above and make new insights or inventions?
Generate a list of possbilities.
""",
"""Here are some concepts:

{concepts}

When you combine them - do they suggest some novel ideas?
Generate a list of possible inventions, suprising facts, research
questions, new insights - etc, any thoughts that could be valuable.
""")
```

#### Template Design Principles
- **Minimal priming**: Avoid leading questions that bias toward Day-Dreaming concepts
- **Open-ended generation**: Encourage broad creative exploration
- **Combination focus**: Explicitly ask for synthesis of provided concepts
- **Multiple variants**: Different phrasings to test prompt sensitivity

### 3. Evaluation System

#### Day-Dreaming Concept Detection
The evaluator LLM assesses responses for Day-Dreaming-like elements:

**Core Elements:**
- Iterative refinement loops and cyclical processes
- Exploration-exploitation balance concepts
- Meta-cognitive awareness and self-monitoring systems
- Combinatorial creativity and automated ideation
- Quality filtering and selection mechanisms

**Evaluation Prompt:**
```
Does this response contain ideas similar to iterative creative loops that automatically generate, evaluate, and refine concepts?

Response: {response}

Answer: YES/NO
Confidence: 0.0-1.0
Reasoning: Brief explanation
```

#### Automated LLM Evaluation
- Use a separate LLM (evaluator model) to assess generated responses
- Structured evaluation prompt with scoring rubric for Day-Dreaming concept detection
- Binary scoring with confidence metrics and detailed reasoning traces
- Store evaluation metadata (timestamp, confidence score, reasoning)

## File Organization

```
daydreaming_experiment/
├── concept.py                    # Core Concept dataclass
├── concept_db.py                 # ConceptDB registry and I/O
├── prompt_factory.py             # PromptFactory for template-based generation
├── experiment_runner.py          # CLI experiment execution
├── model_client.py              # Simple LLM interface
└── results_analysis.py          # Post-experiment analysis tools

data/
├── concepts/                     # New concept database
│   ├── day_dreaming_concepts.json            # Manifest
│   └── articles/               # Article files
└── experiments/                # Experiment results
    └── experiment_YYYYMMDD_HHMMSS/
        ├── config.json                 # Experiment parameters
        ├── results.csv                 # Main results table
        └── responses/
            ├── response_001.txt        # LLM response files
            ├── response_002.txt
            └── ...
```

## Data Storage Schema

### 1. Concept Database Schema

#### Manifest Structure (day_dreaming_concepts.json)
```json
{
  "version": "1.0",
  "created": "2025-07-28T14:30:22Z", 
  "concepts": [
    {
      "name": "neural_networks",
      "sentence": "Artificial neural networks mimic biological brain structures.",
      "paragraph": "Neural networks are computational models inspired by biological neural networks...",
      "article_path": "articles/neural_networks.txt"
    }
  ]
}
```

#### Article File Format
- Plain text files in `data/concepts/articles/`
- Filename matches concept name with `.txt` extension
- UTF-8 encoding

### 2. Results Storage Schema

#### Results Schema (CSV)
| Column | Description |
|--------|-------------|
| experiment_id | Unique experiment identifier |
| attempt_id | Sequential attempt number |
| concept_names | Pipe-separated concept names |
| concept_count | Number of concepts in combination |
| level | Description level used (always "paragraph" initially) |
| template_id | Template index used |
| response_file | Filename of LLM response |
| automated_rating | Binary evaluation (0/1) from evaluator LLM |
| confidence_score | Evaluator's confidence in rating (0.0-1.0) |
| evaluation_reasoning | LLM evaluator's reasoning trace |
| evaluation_timestamp | When automated evaluation occurred |
| generation_timestamp | When prompt was generated |
| model_used | LLM model identifier |

#### Configuration Storage (JSON)
```json
{
  "experiment_id": "exp_20250728_143022",
  "timestamp": "2025-07-28T14:30:22Z",
  "k_max": 4,
  "level": "paragraph",
  "templates": [...],
  "model": "gpt-4",
  "evaluator_model": "gpt-4",
  "concept_count": 6,
  "total_combinations": 57
}
```

## Experimental Process

### 1. Search Strategy
- **Brute-force combinatorial**: Test all combinations from 1 to k_max concepts
- **Current limit**: k_max = 4 (combinations of 1-4 concepts)
- **Future extensions**: Heuristic guidance, random sampling, concept relationships

### 2. Two-Stage Search (Future)
1. **Stage 1**: Test concept combinations at target level (paragraph)
2. **Stage 2**: For successful combinations, test different granularity levels

**Current Implementation**: Single-stage testing at paragraph level only.

## Technical Dependencies

### API Dependencies

#### Required API Access
- **OpenRouter API**: Multi-model access for generation and evaluation
- **API Keys**: Configured via environment variables

#### Core Dependencies
```python
openai>=1.0.0           # API client (OpenRouter compatible)
pandas>=2.0.0           # Results analysis
click>=8.0.0            # CLI interface
pytest>=7.0.0           # Testing framework
black>=23.0.0           # Code formatting
flake8>=6.0.0           # Style checking
```

## Usage Workflow

### 1. Initial Setup
```bash
# Verify concept database
uv run python -c "
from daydreaming_experiment.concept_db import ConceptDB
db = ConceptDB.load('data/concepts')
print(f'Loaded {len(db._concepts)} concepts')
"
```

### 2. Run Experiment
```bash
# Run complete experiment with automated evaluation
uv run python -m daydreaming_experiment.experiment_runner \
    --k-max 4 \
    --level paragraph \
    --generator-model gpt-4 \
    --evaluator-model gpt-4 \
    --output experiments/my_experiment
```

### 3. Analysis
```bash
# Analyze results
uv run python -m daydreaming_experiment.results_analysis \
    experiments/experiment_20250728_143022
```

## Design Principles

1. **Simplicity First**: No complex DAG structures, focus on core experimental loop
2. **Automated Evaluation**: LLM-based evaluation enables large-scale experimentation  
3. **Extensible**: Clean interfaces for future enhancements (concept relationships, automated evaluation, etc.)
4. **Reproducible**: Complete experiment logging for scientific rigor
5. **Modular**: Separate concerns (concept management, experiment execution, evaluation, analysis)

## Implementation Strategy

1. **Phase 1**: Run initial experiments (k_max=4, paragraph level)
2. **Phase 2**: Analyze results and identify successful concept patterns
3. **Phase 3**: Extend system based on experimental insights

## Future Extensions

- **Multi-Model Evaluation**: Cross-validation using multiple evaluator models
- **Concept Relationships**: Dependency-guided combination selection
- **Multi-Level Optimization**: Stage-2 granularity level testing
- **Search Heuristics**: Guided exploration for larger concept spaces  
- **Statistical Analysis**: Success pattern identification and prediction
- **Concept Generation**: Automatic expansion of concept database