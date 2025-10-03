# Experiment Spec DSL

Reference for the tuple-capable DSL used to compile cohort plans into deterministic design rows.

## 1. Overview

The DSL turns declarative specs (YAML/JSON/TOML or directory bundles) into ordered rows encoding all axis levels, pairings, tuple bindings, and replicate indices. Output rows become the source of truth for generation IDs and downstream Dagster assets.

Highlights:
- Ordered axis definitions with optional catalog validation.
- Rule pipeline (`subset → tie → pair → tuple`) to bound Cartesian growth.
- Replication configuration that appends deterministic `replicate` indices without manual duplication.
- CLI (`scripts/compile_experiment_design.py`) for producing CSV/JSONL output with catalog data sourced from JSON/CSV files.
- Cohort integration: `cohort_membership` loads spec bundles via `daydreaming_dagster.cohorts.load_cohort_plan` when `data/cohorts/<cohort_id>/spec/` is present, ensuring spec + catalogs fully determine cohort rows.
- Migration helper (`scripts/migrations/generate_cohort_spec.py`) snapshotting existing cohorts into spec bundles so legacy runs can adopt the DSL without manual transcription.

### Why this design?

The cohort planner operates over a large, full-factorial search space (templates × models × evaluation settings). We rarely want the raw Cartesian product—most experiments depend on structured couplings (e.g., draft template ↔ essay template, essay template ↔ evaluation template). The DSL encodes those couplings explicitly while keeping the generative space deterministic: every combination we intend to compare appears exactly once, every replicate is numbered, and stats across draft/essay/evaluation axes remain balanced. This structure lets us run consistent comparisons across inputs regardless of how many axes we add in the future.

## 2. Spec Structure

Top-level keys supported by `load_spec` / CLI:

| Key | Type | Notes |
|-----|------|-------|
| `axes` | mapping axis → list/`@file:` string | Required. Values are inline lists or shorthand references like `@file:levels.csv`. CSV includes must provide a header; the loader reads the first column when a single value is needed. |
| `rules` | mapping | Optional. Sectioned by rule type (`subsets`, `ties`, `pairs`, `tuples`). The loader converts each section into the canonical rule pipeline order. |
| `output` | mapping | Optional. Currently used for column ordering (`field_order`) and shuffle seed. |
| `replicates` | mapping | Optional. Defines per-axis replication counts (`{axis: count}`), emitting `<axis>_replicate` columns automatically. |

> Specs must be single files. Directory bundles (`spec/axes/*.txt`, `spec/rules/*.yaml`) are no longer supported—use `@file:` references instead.

### 2.1 Axes

```yaml
axes:
  draft_template: [creative-v2, gwern_original]
  essay_template: '@file:items/essay_templates.csv'
```

Axis values are declared inline or loaded from sibling files via the `@file:` shorthand. Paths resolve relative to the spec file (or bundle root). CSV is the recommended format: provide a header row and one value per line for simple lists. Multi-column CSVs feed tuple or pair rules, returning a tuple of strings ordered by the header. (YAML/JSON remain supported if richer structures are required.)

### 2.2 Rules

Rules run after axis deduplication. Each mapping must contain exactly one rule key:

```yaml
rules:
  subsets:
    draft_template: [creative-v2]
  ties:
    draft_essay:
      axes: [draft_template, essay_template]
  pairs:
    draft_eval:
      left: draft_template
      right: evaluation_template
      allowed:
        - [creative-v2, eval-a]
        - [creative-v2, eval-b]
      balance: left
  tuples:
    essay_bundle:
      axes: [essay_template, essay_llm]
      items: '@file:items/essay_bundle.csv'
```

See §3 for semantics.

File references are also supported inside rules. For example:

```yaml
rules:
  tuples:
    curated_bundle:
      axes: [essay_template, essay_llm, evaluation_template, evaluation_llm]
      items: '@file:items/curated_bundles.csv'
```

`items/curated_bundles.csv` should contain a header matching the listed axes and one row per allowed tuple.

### 2.3 Output Options

```yaml
output:
  field_order: [draft_template, essay_template, evaluation_template]
```

`field_order` pins column order in the emitted rows/CSV. Pair, tuple, and tie rules always expand back into their component axes; the synthetic axes are removed after expansion.

### 2.4 Replicates

```yaml
replicates:
  draft_template: 2
```

Each referenced axis (post-rule application) is duplicated `count` times, with replicate indices `1..count` written to the automatically derived column `<axis>_replicate`. Those columns participate in deterministic `gen_id` construction.

## 3. Rule Semantics

- **Subset** (`subsets: axis → levels`): Filters the axis to the listed values; empty results raise `SpecDslError`.
- **Tie** (`ties: canonical → {axes}`): Intersects levels across the listed axes, collapses them into the canonical key, then restores the original axis names with the canonical values.
- **Pair** (`pairs: name → {left, right, allowed, balance?}`): Replaces two axes with a new synthetic axis (named after the mapping key) containing the allowed pairs. Optional `balance` (`left`, `right`, `both`) enforces uniform degrees. After rules run, the compiler always rehydrates the original left/right columns and removes the synthetic axis.
- **Tuple** (`tuples: name → {axes, items}`): Couples N axes into a single tuple-valued axis. Items can be inline or `@file:`. After rule evaluation the compiler restores the individual axes and drops the tuple axis.

Rules execute in declaration order: `subset` → `tie` → `pair` → `tuple` → Cartesian product → output expansion/replication.

## 4. Catalog Validation

Validation now keys off the axis identifiers themselves. When `compile_design` receives a `catalogs` mapping, any axis present in that mapping is checked for membership.

- `--catalog path.json` (repeatable). Each JSON file must contain mappings `{axis_name: [values...]}` or `{axis_name: {value: ...}}`.
- `--catalog-csv axis=PATH[:column]` (repeatable). Reads values from a CSV column (defaults to `id`).
- `--data-root /path/to/data` enables shortcuts like `--catalog-csv draft_template=@stage_templates_csv:template_id`, resolving attributes on `daydreaming_dagster.data_layer.paths.Paths`.

If any axis value is missing from its provided catalog, the compiler raises `SpecDslError(INVALID_SPEC)` with the offending `missing` list.

## 5. Replication Flow

Replication happens after pair/tuple expansion but before field ordering:

1. For each row, look up every `ReplicateSpec` whose axis remains present.
2. Duplicate the row `count` times, inserting a 1-based replicate index into the derived column `<axis>_replicate`.
3. Downstream signature builders can hash `(combo, draft_template, ..., draft_template_replicate)` to form unique generation IDs deterministically.

If an axis is missing or the replicate column conflicts with an existing field, the compiler raises `SpecDslError` to protect schema integrity.

## 6. CLI Usage

`scripts/compile_experiment_design.py` wraps the DSL and exposes convenience flags:

```bash
uv run python scripts/compile_experiment_design.py spec/dir \
  --out design.csv \
  --format csv \
  --catalog data/catalogs/drafts.json \
  --catalog-csv templates=@stage_templates_csv:template_id \
  --data-root /path/to/data \
  --seed 17
```

Flags:

| Flag | Description |
|------|-------------|
| `spec` | Path to spec file or directory. |
| `--out PATH` | Optional output target. Without it, rows print to stdout (respecting `--limit`). |
| `--format {csv,jsonl}` | Overrides format when `--out` is set; defaults based on extension. |
| `--catalog PATH` | JSON catalog file (repeatable). |
| `--catalog-csv NAME=PATH[:COLUMN]` | CSV source for catalog levels (repeatable). |
| `--data-root PATH` | Base directory for resolving `@attribute` shortcuts via `Paths`. |
| `--seed INT` | Deterministic shuffle seed. |
| `--limit INT` | When printing to stdout, limits row count. |

Exit codes follow standard Python semantics (`0` on success, `SpecDslError` message on failure).

## 7. Example

```yaml
# spec/examples/dual_llm_cartesian.yaml
axes:
  draft_template: [creative-v2, application-v2]
  essay_template: [essay-copy, essay-llm]
  evaluation_template: [eval-a, eval-b]
  draft_llm: [gemini_25_pro]
  essay_llm: [sonnet-4]
  evaluation_llm: [sonnet-4]

rules:
  pairs:
    draft_essay:
      left: draft_template
      right: essay_template
      allowed:
        - [creative-v2, essay-llm]
        - [application-v2, essay-copy]
  tuples:
    essay_bundle:
      axes: [essay_template, essay_llm]
      items:
        - [essay-copy, copy_llm]
        - [essay-llm, sonnet-4]

replicates:
  draft_template: 2

output:
  field_order:
    - draft_template
    - draft_llm
    - essay_template
    - essay_llm
    - evaluation_template
    - evaluation_llm
    - draft_template_replicate
```

Compile with:

```bash
uv run python scripts/compile_experiment_design.py spec/examples/dual_llm_cartesian.yaml \
  --out dual_llm_design.csv \
  --catalog data/catalogs/drafts.json
```

The resulting rows provide every column needed to derive deterministic generation IDs, including `draft_template_replicate` indices.
