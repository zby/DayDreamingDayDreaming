# Plan: Standardise `@file:` Includes on CSV

## Context
- Specs currently accept YAML/JSON/TXT data via `@file:`; CSV support returns raw row arrays that require extra downstream handling.
- We already prefer CSV across the project for catalog/config data; aligning spec includes with CSV keeps tooling consistent.
- Curated fixtures (e.g. `tests/fixtures/spec_dsl/curated_essays/`) are ideal candidates to demonstrate the new CSV flow.

## Objectives
1. Make CSV the primary format for `@file:` references while maintaining YAML/JSON for the main `config.yaml`.
2. Ensure the loader enforces predictable CSV semantics (header required, single-column lists vs. multi-column tuples).
3. Update fixtures, docs, and migration tooling to emit/read CSV includes exclusively.
4. Retire YAML/TXT includes where feasible and provide clear error messages for unsupported formats.

## Work Items
- **Loader Enhancements**
  - Update `_load_inline_payload` to require a header and to coerce:
    - single-column CSV → `list[str]` (for axis levels).
    - multi-column CSV → `list[tuple[str, ...]]` in header order (for tuple/pair rules).
  - Emit `INVALID_SPEC` when a CSV row is missing data or lacks a header.
  - Optional: keep YAML/JSON support for backwards compatibility but mark TXT/YAML list usage as discouraged in docs.

- **Spec Fixtures & Tests**
  - Convert existing fixtures to CSV-based includes; ensure tests validate the new loader behaviour.
  - Provide regression coverage for error cases (missing header, empty cells, header/axes mismatch).

- **Docs & Examples**
  - Update `docs/spec_dsl.md` to showcase CSV usage both for axes and tuple rules.
  - Mention the required header and alignment with the `axes` declaration.

- **Migration Tooling**
  - Teach `cohorts/spec_migration.py` to write CSV files for tuple items (`cohort_rows.csv`) and adjust generated `config.yaml` accordingly.

- **Changelog & Deprecation Notes**
  - Document the shift in `AGENTS.md`/release notes so future work avoids YAML includes.

## Example Fixture Snippets
Below are representative snippets for existing tests after the CSV transition.

### `tests/fixtures/spec_dsl/cohort_cartesian/config.yaml`
```yaml
axes:
  combo_id: [combo-1]
  draft_template: [draft-A]
  draft_llm: [draft-llm]
  essay_template: '@file:items/essay_templates.csv'
  essay_llm: [essay-llm]
  evaluation_template: [eval-1]
  evaluation_llm: [eval-llm]

rules:
  tuples:
    essay_bundle:
      axes: [essay_template, essay_llm]
      items: '@file:items/essay_llm_pairs.csv'
  pairs:
    evaluation_bundle:
      left: evaluation_template
      right: evaluation_llm
      allowed: '@file:items/evaluation_pairs.csv'

replicates:
  draft_template: 2
  essay_template: 2

output:
  field_order:
    - combo_id
    - draft_template
    - draft_llm
    - essay_template
    - essay_llm
    - evaluation_template
    - evaluation_llm
    - draft_template_replicate
    - essay_template_replicate
```

`tests/fixtures/spec_dsl/cohort_cartesian/items/essay_templates.csv`
```csv
essay_template
essay-X
essay-Y
```

`tests/fixtures/spec_dsl/cohort_cartesian/items/essay_llm_pairs.csv`
```csv
essay_template,essay_llm
essay-X,essay-llm
essay-Y,essay-llm
```

`tests/fixtures/spec_dsl/cohort_cartesian/items/evaluation_pairs.csv`
```csv
evaluation_template,evaluation_llm
eval-1,eval-llm
```

### `tests/fixtures/spec_dsl/curated_essays/config.yaml`
```yaml
axes:
  essay_template: '@file:items/essay_templates.csv'
  essay_llm: [sonnet-4, gemini_25_pro]
  evaluation_template: [verification-eval-v1]
  evaluation_llm: [sonnet-4]

rules:
  tuples:
    curated_bundle:
      axes: [essay_template, essay_llm, evaluation_template, evaluation_llm]
      items: '@file:items/curated_bundles.csv'

output:
  field_order:
    - essay_template
    - essay_llm
    - evaluation_template
    - evaluation_llm
```

`tests/fixtures/spec_dsl/curated_essays/items/essay_templates.csv`
```csv
essay_template
essay-copy-v1
essay-llm-v1
```

`tests/fixtures/spec_dsl/curated_essays/items/curated_bundles.csv`
```csv
essay_template,essay_llm,evaluation_template,evaluation_llm
essay-copy-v1,sonnet-4,verification-eval-v1,sonnet-4
essay-llm-v1,sonnet-4,verification-eval-v1,sonnet-4
```

## Rollout Notes
- Introduce loader checks before converting all fixtures to catch CSV issues early.
- Coordinate with any teams maintaining out-of-tree specs—provide a migration script or doc snippet illustrating YAML→CSV conversion.
- After fixtures/docs/tests are updated, remove residual YAML include files from the repo to prevent regressions.

