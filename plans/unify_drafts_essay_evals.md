# Unified Plan: Consolidating Draft, Essay, and Evaluation Processing

This plan replaces three near-duplicate flows with a single stage-agnostic runner, while preserving your data layout, Dagster partitions, and the existing `llm_client` resource. It aligns with the current membership-first pipeline where cohort membership is the authoritative configuration (no dependence on curated selection CSVs for generation/evaluation).

---

## 1) Goals & Constraints

**Goals**

* One code path for: render template → (optional) LLM call → parse → persist artifacts
* Keep existing gens layout: `data/gens/<stage>/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`
* First-class support for essay copy mode (pass-through draft parsed → essay parsed) per essay template
* Make parsing pluggable per template with stage-aware parser registry
* Membership-first: resolve `template_id`, `llm_model_id`, parents via cohort membership (no auxiliary CSV inputs)
* Maintain Dagster partitioning by `gen_id` and current OpenRouter-based `llm_client` resource

**Non-Goals (for now)**

* Changing partitioning, cohorts, or cohort selection strategy
* Replacing CSV sources with a database
* Adding new evaluation formats beyond current parsing strategies

---

## 2) Target Architecture

### A. Stage-Agnostic Runner

A unified module (`daydreaming_dagster/unified/stage_runner.py`) that:

1. Renders a Jinja template from a provided `values` dict → writes `prompt.txt`
2. Either:
   * Calls LLM with OpenRouter client → writes `raw.txt`, then parses to `parsed.txt`, or
   * Skips LLM (essay copy mode) and passes through from draft `parsed.txt` → essay `parsed.txt`
3. Writes consistent `metadata.json` with timing, error handling, and file references

**Core Interface: `StageRunSpec`**

* **Required**: `stage`, `gen_id`, `template_path`, `template_id`, `values`, `out_dir`
* **LLM path**: add `model` and `parser_name`
* **Pass-through (essay)**: set `mode="copy"` and `pass_through_from=<path to draft parsed.txt>`
* **Options**: `overwrite=True/False`, `metadata={...}`

### B. Parser Registry with Stage Awareness

* Parsers are functions: `(raw_text: str, ctx: dict) -> str`
* Registry resolves by **name** with stage-scoped lookup:
  * First check: `daydreaming_dagster/utils/{stage}_parsers.py`
  * Fallback to `identity` (no-op) if not found
* Evaluation: use the canonical `parser` column in `evaluation_templates.csv` to select between supported strategies (e.g., `in_last_line`, `complex`)

### C. Template CSVs (Normalized, membership-first, strict)

Align these fields across template CSVs while maintaining backward compatibility:

| Column | Where | Usage | Notes (strict) |
|--------|-------|-------|-----------------|
| `template_id` | all | Stable ID | Must exist in every row; matches file stem by default |
| `active` | all | Selection switch | Required; cohort Cartesian uses this |
| `generator` | all | Generation mode | Required in ALL CSVs; use `llm` for draft/evaluation; `llm` or `copy` for essay |
| `parser` | draft/eval | Parser name | Required for evaluation (e.g., `in_last_line`, `complex`); optional for draft; not used for essay |
| `content` | any | Inline template | Optional; if present, used instead of file |
| `template_path` | any | File override | Optional; otherwise infer from `data/1_raw/...` |

Column order
- When editing CSVs, place the `active` column as the last column in each file for consistency (`template_id, ..., active`).

### D. Dagster Integration (Membership-First, strict)

* Keep existing asset groups and `gen_id` partitions
* Replace stage-specific logic with thin wrappers that:
  1. Lookup the cohort membership row by `(stage, gen_id)` (authoritative source)
  2. Prepare template `values` (for draft: concepts; for essay: parent draft parsed; for eval: essay parsed + rubric params)
  3. Resolve `template_id`, stage mode (`generator` for essay), `llm_model_id`, parser name, and pass-through settings from CSVs + membership
  4. Call the unified runner using `context.resources.openrouter_client`
* Partitions: dynamic partition keys must be registered by the `cohort_membership` asset before selecting partitioned assets. Declaring a dependency on membership does not replace partition registration.
* Strictness: assets and helpers error out if required columns are missing (e.g., `generator` in any template CSV; `parser` in evaluation CSV); no runtime fallbacks.

### E. Storage & Error Handling

* Maintain same gens directory structure and filenames
* Atomic file writes with `overwrite=False` for idempotency
* On LLM/parse errors: write error details to `raw.txt` and annotate `metadata.json`
* Preserve all current downstream script compatibility

---

## 3) Implementation Structure

**New Files**
```
daydreaming_dagster/unified/
  ├─ __init__.py
  ├─ stage_runner.py          # Main runner + parser registry
  └─ (none additional; reuse existing `resources/llm_client.py`)
```

**Modified Assets (light wrappers)**
```
daydreaming_dagster/assets/
  ├─ group_generation_draft.py     # Use unified runner; no ad-hoc CSV fallback
  ├─ group_generation_essays.py    # Use unified runner; enforce essay `generator` (llm|copy)
  └─ group_evaluation.py           # Use unified runner; resolve eval parser from `evaluation_templates.csv`
```

**Existing (unchanged)**
```
daydreaming_dagster/utils/{draft,essay,evaluation}_parsers.py
daydreaming_dagster/resources/llm_client.py
data/1_raw/generation_templates/{draft,essay}/
data/1_raw/evaluation_templates.csv (+ optional files under `1_raw/evaluation_templates/`)
data/1_raw/llm_models.csv (for_generation/for_evaluation flags)
```

---

## 4) Runner Contract

### Metadata Schema
```json
{
  "stage": "essay",
  "gen_id": "gen_abc123",
  "template_id": "essay_v19", 
  "template_path": "data/1_raw/generation_templates/essay/essay_v19.txt",
  "template_sha1": "abc123...",
  "model": "anthropic/claude-3-sonnet",
  "parser_name": "essay_idea_last",
  "skip_llm": false,
  "pass_through_from": null,
  "files": {
    "prompt": "data/gens/essay/gen_abc123/prompt.txt",
    "raw": "data/gens/essay/gen_abc123/raw.txt", 
    "parsed": "data/gens/essay/gen_abc123/parsed.txt"
  },
  "started_at": 1734567890.123,
  "finished_at": 1734567891.456,
  "duration_s": 1.333,
  "mode": "llm",
  "error": "none",
  "error_message": null,
  "cohort_id": "cohort_001"
}
```

### Runner Behavior
1. **Render**: Jinja template with `values` → `prompt.txt`
2. **Branch**:
   * If `skip_llm`: copy `pass_through_from` → `parsed.txt` (no `raw.txt`)
   * Else: OpenRouter API call → `raw.txt` → parse → `parsed.txt`
3. **Persist**: Write/update `metadata.json` with timing and error details

---

## 5) Stage-Specific Wrapper Logic

### Draft Assets (wrapper)
* **Values**: Concept combinations for the membership `combo_id` (via `content_combinations`), description level from ExperimentConfig
* **Template**: `data/1_raw/generation_templates/draft/{template_id}.txt`
* **Model**: From membership `llm_model_id` (validated against `llm_models.csv` `for_generation=true`)
* **Parser**: Optional for draft; if present, use it; otherwise treat draft parsing as identity in the runner spec (no CSV fallback is needed since omission is allowed)
* **Output**: `data/gens/draft/{gen_id}/`

### Essay Assets (wrapper)
* **Mode**: Read `generator` from `essay_templates.csv` → `llm` or `copy`
* **Copy mode**: set `pass_through_from = data/gens/draft/{parent_gen_id}/parsed.txt`; no LLM call
* **LLM mode**: include draft `parsed.txt` in template `values`; call LLM; parse
* **Template**: `data/1_raw/generation_templates/essay/{template_id}.txt`
* **Model**: From membership `llm_model_id`
* **Output**: `data/gens/essay/{gen_id}/`

### Evaluation Assets (wrapper)
* **Values**: Include essay `parsed.txt` content plus rubric parameters
* **Template**: `data/1_raw/evaluation_templates/{template_id}.txt` (or inline `content` column)
* **Model**: From membership `llm_model_id` (validated against `llm_models.csv` `for_evaluation=true`)
* **Parser**: Use `evaluation_templates.csv` `parser` column via the evaluation parsing config
* **Output**: `data/gens/evaluation/{gen_id}/`

---

## 6) Testing Strategy

**Unit Tests**
* Runner core: render-only, pass-through, LLM path with mocked client
* Parser registry: stage-scoped resolution, fallback to identity
* Metadata: schema validation, timing fields, error annotation

**Integration Tests**
* End-to-end flows with synthetic templates:
  * `draft → essay (copy mode) → evaluation`
  * `draft → essay (LLM+parse) → evaluation`
* Use ephemeral Dagster instance and register dynamic partitions (from membership) for selected `gen_id`s
* Seed strict CSVs in `1_raw/` with required columns (`generator` present in all; `parser` present in evaluation)
* File presence/content assertions for all artifact types

**Regression Validation**
* Pick known partition, compare `parsed.txt` output before/after migration
* Validate downstream script compatibility (`scripts/aggregate_scores.py`, etc.)

---

## 7) Rollout Plan (Sequential PRs)

0. **PR-0: Data Migration (Unified CSVs; Strict)**
   * Edit the three template CSVs to the unified, strict schema before any code changes:
     - `data/1_raw/draft_templates.csv`: add column `generator` with value `llm` for all rows; keep/optionally add `parser` (draft parser is optional).
     - `data/1_raw/essay_templates.csv`: ensure column `generator` exists and is set to `llm` or `copy` for each row (copy mode drives pass-through).
     - `data/1_raw/evaluation_templates.csv`: add column `generator` with value `llm` for all rows; ensure `parser` is present and valid (e.g., `in_last_line`, `complex`) for each active row.
   * Validate inputs:
     - Spot-check CSV headers and values.
     - Reorder columns so that `active` is the last column in each CSV (`template_id, ..., active`).
     - Seed strict CSVs in `1_raw/` with required columns (generator present in all; parser present in evaluation) for tests.
   * From this point on, the codebase treats the unified schema as authoritative (no fallbacks).

1. **PR-A: Core Runner**
   * Add `unified/stage_runner.py` with comprehensive unit tests
   * Reuse existing `resources/llm_client.py`
   * No asset changes yet

2. **PR-B: Evaluation Integration**
   * Wire evaluation assets to use runner
   * Handle `parsing_strategy` → parser name mapping
   * Add integration test for evaluation flow

3. **PR-C: Essay Integration**
   * Wire essay assets with both LLM and pass-through branches
   * Test essay copy behavior (`skip_llm=true`)
   * Integration test covering both essay modes

4. **PR-D: Draft Integration**
   * Wire draft assets to runner
   * Complete end-to-end integration test across all stages

5. **PR-E: Cleanup & Documentation**
   * Enforce membership-only lookups for `llm_model_id` (no `generation_model`/`evaluation_model` fallbacks)
   * Optionally introduce a minimal `CohortContext` asset (or pass `cohort_membership` DF) for explicit dependencies in tests
   * Update docs for essay `generator` semantics and evaluation `parser` requirements

---

## 8) Consistency & Missing Steps Checklist

- Membership is the only source for `llm_model_id`, `template_id`, and parent linkage; no ad-hoc CSV fallbacks.
- Essay copy/LLM mode controlled via `essay_templates.csv` `generator` column; plan reflects current implementation.
- Evaluation parser selection via `evaluation_templates.csv` `parser` column; required and validated (no `parsing_strategy` alias).
- Dynamic partitions must be registered by `cohort_membership` before materializing partitioned assets.
- Runner writes the same files and metadata schema expected by downstream results processing.
- Tests use ephemeral instance + `add_dynamic_partitions` and seed strict `1_raw/*` CSVs.
   * Remove duplicated stage-specific code
   * Update CSV column documentation
   * Document parser naming conventions

---

## 9) Observability & Operations

**Logging & Monitoring**
* Log per-stage timings (`render_ms`, `llm_ms`, `parse_ms`) to Dagster logs
* Include model parameters and token counts in metadata when available
* Preserve existing error handling patterns with enhanced context

**Operational Features**
* `overwrite=False` mode for safe re-runs and backfills
* Optional "dry-run" mode stopping after template rendering
* Clear error messages with stack traces in `raw.txt` and `metadata.json`

**CLI Integration**
```bash
# Existing patterns continue to work
export DAGSTER_HOME="$(pwd)/dagster_home"
uv run dagster asset materialize --select "group:generation_draft" --partition <gen_id>
```

---

## 10) Risk Mitigation

**Template/Values Compatibility**
* Start with evaluation assets (most constrained templates)
* Use Jinja `StrictUndefined` to fail fast on missing variables
* Provide minimal fixture templates for comprehensive testing

**Parser Resolution**  
* Strict: require `parser` for evaluation; error if missing or invalid
* Stage-scoped parser modules prevent naming conflicts

**Concurrency & Data Integrity**
* Runner operates within single `gen_id` scope preventing conflicts  
* Use Dagster run configuration to avoid partition overlap
* Atomic file writes maintain data consistency

---

## 11) Acceptance Criteria

✅ **Single Code Path**: One runner handles all three stages (draft/essay/evaluation)

✅ **Pass-Through Support**: Essay copy works without LLM calls, producing only `prompt.txt`, `parsed.txt`, `metadata.json` 

✅ **Artifact Compatibility**: All files remain in same locations with same formats

✅ **Parser Correctness**: Stage-aware parser registry; strict evaluation parser presence

✅ **Downstream Preservation**: Existing scripts work unchanged (with optional CSV defaults)

✅ **OpenRouter Integration**: Maintains current API key and model selection patterns

---

## 12) Future Enhancements (Optional)

* **Template Front-Matter**: YAML headers for `parser/skip_llm/copy_from` to reduce CSV coupling
* **Enhanced Metadata**: Template fingerprints, detailed model parameters, token usage
* **Asset Factory**: Generate stage assets from table specifications for further code reduction
* **Advanced Caching**: Content-based caching for expensive LLM calls during development

---

This unified plan eliminates code duplication while preserving all current functionality, data layouts, and operational patterns. The staged rollout ensures safe migration with comprehensive testing at each step.
