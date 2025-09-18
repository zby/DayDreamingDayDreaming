# DayDreaming Dagster Pipeline

Dagster-based pipeline exploring whether offline LLMs can reinvent the “Daydreaming Loop” via structured, two‑phase generation and evaluation.

- Start here: docs/index.md
- Goals and scope: docs/project_goals.md
- Architecture: docs/architecture/architecture.md
- Operating guide: docs/guides/operating_guide.md

## Two‑Phase Generation (Draft → Essay)
- Phase 1 (draft): brainstorm connections between concepts.
- Phase 2 (essay): compose a structured essay from the draft.
- Details and rationale: docs/architecture/architecture.md#two-phase-llm-generation

## Quick Start

Prereqs: Python 3.9+, uv, LLM API key in `.env`.

UI flow (recommended):
```bash
export DAGSTER_HOME=$(pwd)/dagster_home
uv run dagster dev -f src/daydreaming_dagster/definitions.py
# Open http://localhost:3000 and materialize assets
```

CLI flow (minimal):
```bash
# Seed tasks and register partitions
uv sync
export DAGSTER_HOME=$(pwd)/dagster_home
uv run dagster asset materialize --select "group:cohort" -f src/daydreaming_dagster/definitions.py

# Run a single draft or essay partition by gen_id (from data/2_tasks/*.csv)
# Drafts/essays/evaluations expose prompt/raw/parsed assets; run by partition id
uv run dagster asset materialize --select "group:generation_draft"   --partition <gen_id> -f src/daydreaming_dagster/definitions.py
uv run dagster asset materialize --select "group:generation_essays"  --partition <gen_id> -f src/daydreaming_dagster/definitions.py
uv run dagster asset materialize --select "group:evaluation"        --partition <gen_id> -f src/daydreaming_dagster/definitions.py

# Parse and pivot evaluation scores (cross‑experiment)
uv run python scripts/aggregate_scores.py --output data/7_cross_experiment/parsed_scores.csv
uv run python scripts/build_pivot_tables.py --parsed-scores data/7_cross_experiment/parsed_scores.csv
```

More examples and troubleshooting: docs/guides/operating_guide.md

## Data & Partitions
- Inputs: `data/1_raw/` (concepts, templates, `llm_models.csv` with `for_generation` / `for_evaluation`).
- Task CSVs: `data/2_tasks/` (contain `gen_id` partition keys).
- Gens store: `data/gens/<stage>/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`.
- Dynamic partitions: one `gen_id` per stage (draft, essay, evaluation).
- Details: docs/architecture/architecture.md#storage-architecture

Source of truth for storage conventions: `src/daydreaming_dagster/data_layer/paths.py`.
Example:
```
from daydreaming_dagster.data_layer.paths import Paths
paths = Paths.from_str("data")
base = paths.generation_dir("essay", "<gen_id>")
print(paths.parsed_path("essay", "<gen_id>"))  # data/gens/essay/<gen_id>/parsed.txt
```

## Development
- Tests: `.venv/bin/pytest` (unit in `daydreaming_dagster/`, integration in `tests/`).
- Formatting: `uv run black .`; Lint: `uv run ruff check`.
- Conventions and agent guidelines: AGENTS.md
- Storage conventions (single source of truth): `src/daydreaming_dagster/data_layer/paths.py`

---
For curated selection, cohorts, and advanced workflows, see:
- docs/cohorts.md
- docs/guides/selection_and_cube.md
export DAGSTER_HOME="$(pwd)/dagster_home"
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f src/daydreaming_dagster/definitions.py

# 3) Materialize drafts → essays → evaluations using the registered partitions
uv run dagster asset materialize --select "group:cohort" -f src/daydreaming_dagster/definitions.py
```

The membership asset writes `data/cohorts/<cohort_id>/membership.csv` (wide rows by stage) and registers dynamic partitions add‑only. Task assets project their tables from membership; generation/evaluation assets run as before.

### Cross‑Experiment Views

Cross‑experiment analysis is derived directly from the gens store (data/gens/**) and task CSVs:

- Use assets: `filtered_evaluation_results`, `template_version_comparison_pivot`.
- Use scripts for backfills or one‑off tables under data/7_cross_experiment/ (no auto‑appenders).

**Initial setup** (populate tables from existing data):
```bash
# 1) Parse evaluation scores from gens store into parsed_scores.csv
uv run python scripts/aggregate_scores.py --output data/7_cross_experiment/parsed_scores.csv

# 2) Build pivot over parsed_scores
uv run python scripts/build_pivot_tables.py --parsed-scores data/7_cross_experiment/parsed_scores.csv

# Outputs (under data/7_cross_experiment/):
# - parsed_scores.csv (canonical cross-experiment evaluation scores)
# - evaluation_scores_by_template_model.csv  (pivot: rows=essay_task, cols=evaluation_template__evaluation_model)
```

Note: Auto‑materializing appenders were removed. Derive views on demand from the gens store and tasks.

### Raw CSV Change Handling (Schedule)

- The project includes a lightweight schedule that scans the raw CSVs under `data/1_raw/` (concepts_metadata.csv, llm_models.csv, draft_templates.csv, essay_templates.csv, evaluation_templates.csv).
- On detecting a change, the schedule writes a pending fingerprint and launches a run (run_key = fingerprint) that refreshes the cohort/membership layer (`group:cohort`).
- If the run fails or is canceled, a future tick will retry the same pending fingerprint (same run_key). On success, the schedule promotes the pending fingerprint to last and skips until the next change.
- This gives at‑least‑once behavior for task updates without auto‑running any LLM assets. Downstream runs still “pull” and rebuild stale upstream as needed.

## Development

### Testing

```bash
# Run all tests (preferred in sandboxed envs)
.venv/bin/pytest

# Run only unit tests (fast, isolated)
.venv/bin/pytest daydreaming_dagster/

# Run only integration tests (data-dependent)
.venv/bin/pytest tests/

# Run with coverage
uv run pytest --cov=daydreaming_dagster
```

## Configuration

### Environment Variables

- `OPENROUTER_API_KEY`: OpenRouter API key for LLM access
- `DAGSTER_HOME`: Dagster metadata storage (set to absolute path of `dagster_home/` directory)
- Gens store layout: Draft/essay/evaluation prompts and responses are stored under `data/gens/<stage>/<gen_id>` as `prompt.txt`, `raw.txt`, `parsed.txt`, and `metadata.json`.

### Cohorts (deterministic run IDs)

- A dedicated asset `cohort_id` (group `cohort`) computes a deterministic cohort identifier from the current manifest of combos, active templates, and models.
- The manifest is written to `data/cohorts/<cohort_id>/manifest.json`; the `cohort_id` value is threaded into all task CSVs and gens metadata when present.
- Gen IDs are reserved as `reserve_gen_id(stage, task_id, run_id=cohort_id)` so all artifacts are tied to a visible, reproducible cohort.

Usage:
- Default deterministic cohort (recommended baseline):
  - `uv run dagster asset materialize --select cohort_id -f src/daydreaming_dagster/definitions.py`
  - Then materialize tasks (inherits the same cohort):
    `uv run dagster asset materialize --select "group:cohort" -f src/daydreaming_dagster/definitions.py`
- Override explicitly for curated re-runs:
  - Env var: `export DD_COHORT=my-curated-2025-09-09` (tasks will use this value)
  - Asset config (Dagster UI or YAML):
    ```yaml
    ops:
      cohort_id:
        config:
          override: "baseline-v3"
    ```

Notes:
- If you materialize a subset (e.g., only tasks) the tasks will compute and persist a cohort manifest automatically unless `DD_COHORT` is set.
- Each stage’s `metadata.json` includes `cohort_id`; task CSVs add a `cohort_id` column.
- Prefer deterministic cohorts for the full Cartesian baseline; use explicit/timestamped IDs for curated or ad‑hoc runs to avoid overwrites.

**Important**: Set `DAGSTER_HOME=$(pwd)/dagster_home` to use the project's Dagster configuration, which includes auto-materialization settings. Without this, Dagster uses temporary storage and ignores the project configuration.

### Pipeline Parameters

Configure in `daydreaming_dagster/resources/experiment_config.py`:

- `k_max`: Maximum concept combination size
- `description_level`: Description level ("sentence", "paragraph", "article")
- `template_names_filter`: Optional list of template names to load (None = all)
- Model selection in `data/1_raw/llm_models.csv` (use `for_generation`/`for_evaluation` columns)

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
- **Templates**: `data/1_raw/templates/` (Jinja2 templates with two-phase structure; override root with `GEN_TEMPLATES_ROOT`)
- **Models**: `data/1_raw/llm_models.csv` (available LLM models; flags `for_generation`, `for_evaluation`)

#### Template Structure (Two-Phase)

Templates are organized in a two-phase structure:

```
data/1_raw/templates/
├── draft/                    # Phase 1: Concept draft generation
│   ├── creative-synthesis-v7.txt
│   ├── systematic-analytical.txt
│   └── ...
└── essay/                    # Phase 2: Essay composition
    ├── creative-synthesis-v7.txt
    ├── systematic-analytical.txt
    └── ...
```

**Phase 1 Templates** (`draft/`): 
- Focus on generating the structure that will drive the essay phase. Multiple styles are supported.
- Output: Structured Markdown according to the selected template schema.
- Template variables: `concepts` (list with name, content)

Adding a new draft template:
- Place the template file under `data/1_raw/templates/draft/<template_id>.txt`.
- Register it in `data/1_raw/draft_templates.csv` with the same `template_id` and set `active=true` (set others to `false`). If the draft output requires extraction into an essay‑ready fragment, set the `parser` column (e.g., `essay_idea_last`). Supported parser names live in `daydreaming_dagster/utils/draft_parsers.py`. Missing or unknown parsers cause a hard failure during draft generation (Phase‑1), and the RAW draft is still saved for debugging.
- Optional: set `GEN_TEMPLATES_ROOT` to point to a different root if you maintain templates outside the repo.

Active draft templates are controlled in `data/1_raw/draft_templates.csv` via the `active` column. Examples include:
- `deliberate-rolling-thread-v2` / `-v3` — Tagged rolling idea with per-step `<essay-idea>` blocks designed for downstream parsing.
- `rolling-summary-v1` — Readable idea-thread recursion with enforced link types and a rolling “Idea So Far — i” summary.

**Phase 2 Templates** (`essay/`):
- Focus on structured essay composition.
- Input: `links_block` / `draft_block` (parsed output from Phase‑1).
- Output: essays developing the best combinations.
- Built-in validation: depends on template; Phase‑1 parsing and minimum‑lines validation happen earlier.

### Output Data
- **Tasks**: `data/2_tasks/*.csv` (generated combinations and tasks; include `gen_id` for each row)
- **Gens Store (primary)**: `data/gens/<stage>/<gen_id>/`
  - Contains `raw.txt`, `parsed.txt`, optional `prompt.txt`, and `metadata.json`
  - Stages: `draft`, `essay`, `evaluation`
- **Optional RAW side-writes**: `data/3_generation/*_raw/` (enabled via ExperimentConfig; useful for debugging truncation/parser issues)
- **Results**: `data/5_parsing/` (processed results) and `data/6_summary/` (summaries)
- **Global Mapping**: `data/combo_mappings.csv` (append-only mapping of stable combo IDs to their concept components)

### Prompt Persistence (Gens IO)
- Prompts are saved directly under the gens store via a small IO manager:
  - `data/gens/draft/<gen_id>/prompt.txt`
  - `data/gens/essay/<gen_id>/prompt.txt`
  - `data/gens/evaluation/<gen_id>/prompt.txt`
- The IO manager (`GensPromptIOManager`) accepts `gen_id` partition keys directly and writes prompts to `data/gens/<stage>/<gen_id>/prompt.txt`.
- Responses are written to the gens store by the assets themselves (not via IO managers). Scripts and downstream assets read from the gens store using `metadata.json` and file paths.

## Key Features

- **🚀 Two-Phase Generation**: Innovative separated brainstorming and essay composition with quality validation
- **Partitioned Processing**: Efficient processing of large task sets with automatic recovery
- **Template System**: Flexible Jinja2-based prompt generation with phase-aware loading
- **Multi-Model Support**: Test across different LLM providers
- **Selective Loading**: Optional filtering for faster development and testing
- **Robust Parser**: Automatic detection and parsing of various LLM evaluation response formats
- **Scalable Processing**: Memory-efficient sequential processing handles large datasets
- **Backward Compatibility**: Legacy single-phase directories are readable for historical analysis, but new runs always materialize essays (use an essay template with generator=copy to mirror single-phase).
- **Isolated Testing**: Complete test environment separation from production data

## Troubleshooting

### Common Issues

1. **Missing Asset Dependencies**:
   ```
   FileNotFoundError: [Errno 2] No such file or directory: '.../storage/templates_metadata'
   ```
   **Solution**: Template assets need their metadata dependencies. Use one of these approaches:
   ```bash
   # Recommended: Use asset groups
  uv run dagster asset materialize --select "group:raw_data,group:cohort" -f src/daydreaming_dagster/definitions.py
   
   # Alternative: Use dependency resolution
  uv run dagster asset materialize --select "+generation_tasks,+evaluation_tasks" -f src/daydreaming_dagster/definitions.py
   ```

2. **Missing DAGSTER_HOME**:
   ```
   DagsterHomeNotSetError: The environment variable $DAGSTER_HOME is not set
   ```
   **Solution**: Always set DAGSTER_HOME before running any commands:
   ```bash
   export DAGSTER_HOME=$(pwd)/dagster_home
   # Ensure the directory exists if missing
   [ -d "$DAGSTER_HOME" ] || mkdir -p "$DAGSTER_HOME"
   ```

3. **Partition Not Found Error**:
   ```
  DagsterUnknownPartitionError: Could not find a partition with key `combo_v1_<hex>_...` (or legacy `combo_001_...`)
   ```
  **Solution**: Make sure task CSV files exist by running step 1 first. Use `awk -F, 'NR==1{for(i=1;i<=NF;i++)h[$i]=i} NR==2{print $h["gen_id"]}' data/2_tasks/draft_generation_tasks.csv` to retrieve a valid draft partition key (gen_id).

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
   - **Strategy selection (CSV-driven)**: `data/1_raw/evaluation_templates.csv` uses a required `parser` column (`in_last_line` or `complex`).

## Documentation

For detailed architecture and implementation information, see:

- [Architecture Overview](docs/architecture.md) - Detailed system design and components
- [LLM Concurrency Guide](docs/llm_concurrency_guide.md) - LLM API optimization patterns
- [Gens Store Guide](docs/guides/docs_store.md) - Gen-id filesystem layout, invariants, and helpers
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
