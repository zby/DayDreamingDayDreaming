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

### Why this design?

The cohort planner operates over a large, full-factorial search space (templates × models × evaluation settings). We rarely want the raw Cartesian product—most experiments depend on structured couplings (e.g., draft template ↔ essay template, essay template ↔ evaluation template). The DSL encodes those couplings explicitly while keeping the generative space deterministic: every combination we intend to compare appears exactly once, every replicate is numbered, and stats across draft/essay/evaluation axes remain balanced. This structure lets us run consistent comparisons across inputs regardless of how many axes we add in the future.

## 2. Spec Structure

Top-level keys supported by `load_spec` / CLI:

| Key | Type | Notes |
|-----|------|-------|
| `axes` | mapping axis → list _or_ mapping | Required. Each axis is either a list of literal levels or a mapping containing `levels` plus optional `catalog_lookup` metadata. Directory specs may place lists in `axes/<axis>.txt`; the loader wraps them automatically. |
| `rules` | list | Optional. Each entry is a single-rule mapping (`subset`, `tie`, `pair`, `tuple`). Execution order is preserved. |
| `output` | mapping | Optional. Controls expansion flags, field ordering, shuffle seed. |
| `replicates` | mapping | Optional. Defines per-axis replication (`{axis: {count, column?}}`). |

### 2.1 Axes

```yaml
axes:
  draft_template:
    levels: [creative-v2, gwern_original]
    catalog_lookup: { catalog: drafts }
  essay_template: [copy_v1]
```

`catalog_lookup` instructs the compiler to verify axis levels against the named catalog. Catalog data is supplied at compile time (see §4).

To reuse existing lists, point a level declaration at an external file with `@file:`. Paths are resolved relative to the spec file (or spec directory when using bundled configs). Supported formats include YAML/JSON (parsed into native structures), CSV (returned as lists of rows), and plain text (newline-separated values):

```yaml
axes:
  essay_template:
    levels: '@file:items/essay_templates.yaml'
```

### 2.2 Rules

Rules run after axis deduplication. Each mapping must contain exactly one rule key:

```yaml
rules:
  - subset:
      axis: draft_template
      keep: [creative-v2]
  - tie:
      axes: [draft_template, essay_template]
      to: draft_essay
  - pair:
      left: draft_template
      right: evaluation_template
      name: draft_eval
      allowed:
        - [creative-v2, eval-a]
        - [creative-v2, eval-b]
      balance: left
  - tuple:
      name: essay_bundle
      axes: [essay_template, essay_llm]
      items:
        - [copy_v1, copy_llm]
      expand: true
```

See §3 for semantics.

File references are also supported inside rules. For example:

```yaml
  - tuple:
      name: curated_bundle
      axes: [essay_template, essay_llm, evaluation_template, evaluation_llm]
      items: '@file:items/curated_bundles.yaml'
```

`items/curated_bundles.yaml` should contain a YAML/JSON array describing the tuple entries.

### 2.3 Output Options

```yaml
output:
  expand_pairs: true
  keep_pair_axis: false
  expand_ties: true
  expand_tuples: true
  field_order: [draft_template, essay_template, evaluation_template]
```

Flags default to expanding pairs/tuples and dropping synthetic axes; override as needed. `field_order` pins column order in the emitted rows/CSV.

### 2.4 Replicates

```yaml
replicates:
  draft_template:
    count: 2
    column: draft_replicate  # optional (defaults to "draft_template_replicate")
```

Each referenced axis (post-rule application) is duplicated `count` times, with replicate indices `1..count` written to the given column. Those columns participate in deterministic `gen_id` construction.

## 3. Rule Semantics

- **Subset** (`subset: {axis, keep}`): Filters the axis to the listed values; empty results raise `SpecDslError`.
- **Tie** (`tie: {axes: [...], to?}`): Intersects levels across the axes, collapses them into a canonical axis (`to` or the first name), and records original aliases so they can be re-expanded if `expand_ties` is true.
- **Pair** (`pair: {left, right, name, allowed, balance?}`): Replaces two axes with a new synthetic axis containing the allowed pairs. Optional `balance` (`left`, `right`, `both`) enforces uniform degrees. During emission, original columns reappear unless `expand_pairs` is false; `keep_pair_axis` retains the synthetic axis alongside the expansion.
- **Tuple** (`tuple: {name, axes, items, expand?}`): Couples N axes into a single tuple-valued axis. Items can be inline or `@file:`. Expansion restores the original axes when `expand` (or global `expand_tuples`) is true; otherwise the tuple axis persists.

Rules execute in declaration order: `subset` → `tie` → `pair` → `tuple` → Cartesian product → output expansion/replication.

## 4. Catalog Validation

Axes with `catalog_lookup: { catalog: name }` are validated against provided catalog data. Supply catalogs via the CLI:

- `--catalog path.json` (repeatable). Each JSON file must contain mappings `{catalog_name: [values...]}` or `{catalog_name: {value: ...}}`.
- `--catalog-csv name=PATH[:column]` (repeatable). Reads values from a CSV column (defaults to `id`).
- `--data-root /path/to/data` enables shortcuts like `--catalog-csv templates=@stage_templates_csv:template_id`, resolving attributes on `daydreaming_dagster.data_layer.paths.Paths`.

If any axis value is missing, the compiler raises `SpecDslError(INVALID_SPEC)` with the offending `missing` list.

## 5. Replication Flow

Replication happens after pair/tuple expansion but before field ordering:

1. For each row, look up every `ReplicateSpec` whose axis remains present.
2. Duplicate the row `count` times, inserting a 1-based replicate index into the configured column (default `<axis>_replicate`).
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
  draft_template:
    levels: [creative-v2, application-v2]
    catalog_lookup: { catalog: drafts }
  essay_template: [essay-copy, essay-llm]
  evaluation_template: [eval-a, eval-b]
  draft_llm: [gemini_25_pro]
  essay_llm: [sonnet-4]
  evaluation_llm: [sonnet-4]

rules:
  - subset:
      axis: essay_template
      keep: [essay-copy, essay-llm]
  - pair:
      left: draft_template
      right: essay_template
      name: draft_essay
      allowed:
        - [creative-v2, essay-llm]
        - [application-v2, essay-copy]
  - tuple:
      name: essay_bundle
      axes: [essay_template, essay_llm]
      items:
        - [essay-copy, copy_llm]
        - [essay-llm, sonnet-4]

replicates:
  draft_template:
    count: 2
    column: draft_replicate

output:
  field_order:
    - draft_template
    - draft_llm
    - essay_template
    - essay_llm
    - evaluation_template
    - evaluation_llm
    - draft_replicate
```

Compile with:

```bash
uv run python scripts/compile_experiment_design.py spec/examples/dual_llm_cartesian.yaml \
  --out dual_llm_design.csv \
  --catalog data/catalogs/drafts.json
```

The resulting rows provide every column needed to derive deterministic generation IDs, including `draft_replicate` indices.
