# Plan: Tuple-Capable Experiment DSL Module

## 1. Current Inventory (2025-02-14)
- No runtime code yet for the subset/tie/pair DSL; only conceptual docs (`plans/notes/full-factorial-structured-couplings.md`).
- Cohort pipeline still relies on legacy `active` flags; spec-driven planner exists only as a plan (`plans/spec_driven_cohort_plan.md`).
- No reusable helpers for loading axis values from local files or catalogs; current assets manually read CSVs.
- Example scenarios we must cover:
  * Baseline Cartesian cohort with two draft/essay LLMs and two evaluation LLMs yielding a 2×2 fanout.
  * Dual-generation LLM cohort where drafts and essays use different models but evaluation axes stay aligned.
  * Curated essays cohort that selects a fixed list of essay artifacts and drives evaluations only for those selections, expressed as explicit essay bundles (essay template + essay LLM + evaluation template + evaluation LLM) supplied inline or via file reference, with tuples produced directly by selector scripts (e.g., `scripts/select_top_essays.py`) rather than legacy `gen_id` surrogates.
  * Mixed copy vs. two-phase cohort where certain draft templates map to copy-mode essays while others trigger fresh essay generations.
  * Full two-phase Cartesian cohort mirroring today’s behavior with paired draft/essay templates sharing the same LLMs to retain parity.

## 2. Goals
- Build a standalone Python module that ingests declarative experiment specs and emits deterministic rows over transformed axes.
- Support `subset`, `tie`, `pair`, and new first-class `tuple` rules that can couple N axes in one step.
- Allow axis levels and tuple items to be declared inline or via file references (relative paths resolved through `Paths`).
- Provide CLI + Python API that compiles a spec, validates against catalogs, and writes CSV/JSONL outputs for downstream assets.
- Ensure behavior is well-tested and independent of the existing cohort implementation so it can be integrated later.

## 3. Target Design
- Module namespace: `src/daydreaming_dagster/spec_dsl/` with:
  - `models.py`: dataclasses/Pydantic models for spec schema, rule enums, file reference handling.
  - `loader.py`: utilities to load YAML/TOML/JSON specs, resolve `@file:` references, and read catalog CSVs through `Paths` helpers.
  - `compiler.py`: core engine applying rules in fixed order (`subset → tie → pair → tuple → product → expand`).
  - `emitter.py`: CSV/JSONL writers and deterministic shuffler.
  - `errors.py`: module-local `Enum` + exception that surfaces structured validation failures without extending the global `Err` registry yet (bridge to `DDError` when we integrate with Dagster).
- Keep external surface minimal: expose a small public API (e.g., `compile_design`, `load_spec`, CLI entry) so the module can be extracted into a package later without refactoring internals.
- **Python API proposal**: 
  - `load_spec(path: Path | str) -> ExperimentSpec`: load + validate a directory/file bundle into in-memory models.
  - `compile_design(spec: ExperimentSpec, *, catalogs: CatalogIndex | None = None, seed: int | None = None) -> list[OrderedDict[str, Any]]`: accepts the parsed spec and optional catalog lookup helpers, returns a deterministic list of ordered row mappings (preserving axis expansion order). `CatalogIndex` is a thin protocol exposing `lookup(axis_name: str, value: str) -> CatalogEntry`; tests may pass simple dicts.
  - `compile_to_file(spec: ExperimentSpec, out: Path, fmt: Literal["csv", "jsonl"] = "csv", *, seed: int | None = None) -> CompileArtifact`: helper bundling the rows and emitted path metadata for callers that need side effects.
- **Error surface**: all validation failures raise `SpecDslError(code: SpecDslErrorCode.INVALID_SPEC, ctx: dict)`. Dagster-facing call sites can catch and wrap with `DDError` later; tests assert on the module-local error code.
- Spec format:
  - `axes`: mapping from axis name to `levels` (list) or `source` (`@file:...`). Support optional `catalog_lookup` metadata for validating IDs.
  - `rules`: sequence; each rule includes inline values or file references.
  - `output`: ordering, expand flags (`expand_pairs`, `expand_tuples`, `keep_tuple_axis`, seed for shuffling).
- `tuple` rule contract:
  ```yaml
  - tuple:
      name: essay_bundle
      axes: [essay_template, essay_llm, evaluation_template, evaluation_llm]
      items:
        - [essay-A, llama-essay, eval-1, llama-eval]
        - @file:spec/tuples/essay_pairs.csv
      expand: true  # optional (defaults to true)
  ```
  - Engine replaces listed axes with synthetic `essay_bundle` axis whose levels are the provided tuples. Optional expansion emits original columns post-product.

## 3A. Rule Semantics (Authoritative)
- **Axis normalization**: Every axis declared in `axes` must resolve to a list. Levels are deduplicated in first-seen order before any rules run. Missing or non-list axis definitions raise `Err.INVALID_SPEC` (legacy raised `CompileError`).
- **Processing order**: apply `subset` → `tie` → `pair` → `tuple` (new) before taking the Cartesian product. Later rules operate on the axis map produced by earlier ones.
- **Subset**:
  - Syntax: `subset: {axis: <name>, keep: [values...]}`.
  - Must reference an existing axis. Keeps only the listed levels (preserving prior order). Empty results are invalid. Multiple subset rules on the same axis compose (later subsets see the filtered levels).
- **Tie**:
  - Syntax: `tie: {axes: [a, b, ...], to: optional_canonical}`.
  - All listed axes must exist pre-tie. Canonical axis defaults to the first name or `to` if supplied. Calculates the ordered intersection of the level lists (using axis0 order, deduped). Empty intersection is an error. Non-canonical axes are removed from the active axis map, but we record `original → canonical` so expansion can reintroduce them if `output.expand_ties` (default true) is enabled.
- **Pair**:
  - Syntax: `pair: {left: <axis>, right: <axis>, name: <new_axis>, allowed: [[l, r], ...], balance: optional}`.
  - `left`/`right` names resolve through the tie rename map. After resolution they must be present. Every allowed pair must draw from the existing domains. Optional `balance` (`left`, `right`, `both`) enforces uniform degree counts on the corresponding margins; violations raise an error. Replaces the consumed axes with a new axis whose levels are the unique ordered `allowed` pairs. The compiler memoizes `(left_raw, right_raw)` so rows can expand back to the original axis names when `output.expand_pairs` (default true) is set; `output.keep_pair_axis` controls whether the synthesized axis is preserved as a structured column.
- **Tuple (new)**:
  - Syntax: `tuple: {name: <new_axis>, axes: [a, b, ...], items: [[...], ...] | @file:..., expand: bool?}`.
  - Runs after pair. All listed axes must exist at this point. Each tuple in `items` must match the axis count; values must belong to the current domains of the referenced axes. We construct a synthetic axis whose levels are the provided tuples (deduped order). Unless `expand` is false, the compiler expands the tuple during row emission and restores the original axis columns post-product. Setting `expand` false keeps only the tuple axis in the output.
- **Cartesian product and emission**:
  - After rules, compute the product over remaining axes. Output options allow field ordering, tuple/pair expansion, and optional structured columns. Reconstructed fields rely on the recorded tie/pair/tuple metadata.
- **Boundary wrapper**: Dagster-facing call sites wrap `SpecDslErrorCode.INVALID_SPEC` into `DDError(Err.INVALID_SPEC, ctx=...)` once we extend the global contract during cohort integration.
- **Error policy**: raise `SpecDslError(SpecDslErrorCode.INVALID_SPEC, ctx={...})` for all validation failures. Include axis names, tuple widths, or offending values in `ctx` for observability. Cohort/Dagster integration can up-convert to `DDError` when wiring the boundary.

## 4. Implementation Steps
1. **Scaffold & Schema**
   - Define rule models, axis descriptors, file reference type with validation.
   - Implement loader that reads spec files, resolves inline vs `@file:` sources (supports `.txt`, `.csv`, `.yaml`), and normalizes to internal structures.
2. **Compiler Core**
   - Implement rule application pipeline with pure functions handling each rule; ensure tuple rule integrates after pair.
   - Add explicit validation (empty domains, duplicate IDs, tuple length mismatches, catalog membership).
   - Provide `compile_design(spec, catalogs, *, seed=None)` returning ordered dict rows plus metadata (log of transformations).
3. **Emission & CLI**
   - Build CLI (`scripts/compile_experiment_design.py`) that runs compiler on a spec directory, writes outputs under `data/2_tasks/designs/` (path configurable).
   - CLI delegates into `daydreaming_dagster.spec_dsl` functions (no standalone logic) and is exposed via Poetry/uv entry point once ready.
   - Include dry-run mode printing row counts and sample rows.
4. **Testing**
   - Unit tests for loader (file references), each rule, tuple expansion, error cases, and deterministic ordering.
   - Integration-style tests using examples from `plans/notes/spec_examples/` to verify row counts and schema.
   - Snapshot/fixture tests for CLI output (ensuring consistent CSV header/order).
5. **Docs & Examples**
   - Document schema in `docs/spec_dsl.md` with examples (including tuple use cases).
   - Update `plans/notes/spec_examples/` README to map each scenario to DSL constructs (non-code deliverable but ensures alignment).

## 5. Validation & Tooling
- All new tests placed under `src/daydreaming_dagster/spec_dsl/tests/` (colocated unit tests) and `tests/spec_dsl/` for CLI coverage.
- Add `uv run python -m daydreaming_dagster.spec_dsl.cli --help` to CI smoke tests once integrated.
- Provide optional Radon target to monitor compiler complexity.

## 6. Risks & Mitigations
- **File Reference Errors**: differentiate between missing files and empty files; include path context in `DDError.ctx`.
- **Tuple/Axis Drift**: enforce tuple width equals len(`axes`); add catalog validation early to catch typos.
- **Performance**: guard against runaway Cartesian products by logging row counts and allowing `output.max_rows` guard.
- **Future Integration**: keep module self-contained with no Dagster dependencies so cohort planner can adopt it gradually.

## 7. Deliverables
- New `spec_dsl` package with loader, compiler, CLI, and tests.
- Documentation summarizing DSL usage and tuple patterns.
- Ready-to-use command for generating design CSVs from spec directories.
