# Experiment Design with the Cohort Mechanism

This guide explains how to plan, author, and validate experiments using the cohort pipeline. Read it alongside the high-level [cohort overview](../cohorts.md) and the detailed [experiment spec DSL reference](../spec_dsl.md).

## Scenario-First Walkthrough

Before diving into theory, consider two recurring questions that cohorts solve with minimal manual bookkeeping.

### Rescoring the Best Essays Collected So Far

You may want to revisit the strongest essays gathered across multiple cohorts and score them against a fresh set of rubrics. The raw sample is usually irregular—different drafts were evaluated with distinct LLMs, rubric versions, or replicate counts. A cohort spec lets you:

1. Build a helper CSV that lists the `gen_id`s of the winning essays and include it with `@file:` in the spec.
2. Declare axes only for the evaluations you care about today (e.g., rubric, evaluation LLM, replicate index).
3. Let the compiler expand the cross-product so every chosen essay receives the full, uniform set of new evaluations.

During materialization, existing evaluations are reused; the evaluation assets only call LLMs for combinations that do not already exist. The resulting cohort provides a balanced dataset that is safe for comparisons or reporting without overwriting prior work.

### Comparing Templates with Uneven Historical Coverage

Sometimes organic usage shows that template A seems stronger than template B, but the historical data lacks symmetry: each template may have run with different evaluation rubrics, models, or replicate counts. To validate the hunch:

1. Define a cohort spec whose axes include just the two templates, plus every other dimension required for a fair comparison (essay template, evaluation rubric, generation LLM, replicates, etc.).
2. Materialize the cohort and its downstream assets. The compiler yields drafts, essays, and evaluations that fill any missing combinations.
3. Existing artifacts are reused, and new generations are triggered only where coverage was missing. Once the cohort completes, pivot tables (or Dagster asset checks) can compare the templates on a perfectly aligned dataset.

These scenarios illustrate the core value of cohorts: they turn messy historical slices into uniform samples without manual deduplication.

## When to Use This Guide

Reach for cohorts whenever you need a reproducible slice of the search space—draft → essay → evaluation—without hand-writing target lists for every stage. The cohort build compiles a declarative spec into a canonical `membership.csv` that downstream assets consume. Use this guide to:

- Translate product or research questions like the scenarios above into cohort specs.
- Pick the right DSL features (axes, subsets, pairs, tuples, replicates) for your design.
- Avoid the common pitfalls that break cohort compilation or yield unbalanced comparisons.

## End-to-End Design Loop

Follow this checklist whenever you shape a new cohort. Each step corresponds to a section in the spec DSL or cohort asset flow.

1. **Clarify the experiment objective.** Identify the comparison you need (e.g., baseline prompt vs. new prompt, model bake-off, rubric calibration). Tie every axis choice back to that objective so you avoid over-growing the search space.
2. **Inventory catalog coverage.** Confirm the drafts, templates, rubrics, and models already exist in the catalog CSVs referenced by the cohort build. Missing catalog entries will cause `cohort_membership` to fail validation.
3. **Sketch axes and replicates.** Define the independent dimensions you intend to vary. Include explicit replicate axes so deterministic replicate indices appear in `membership.csv` and reporting can stay balanced.
4. **Encode structured couplings.** Use subsets for narrowing, ties for shared selections, pairs for enumerated two-way couplings, and tuples when multiple axes must move together. The DSL evaluates them in that order; plan ahead to keep the rule pipeline minimal.
5. **Author the spec bundle.** Copy an existing cohort directory, edit `spec/config.yaml`, and add helper CSVs under `spec/`. Keep one directory per cohort ID and commit everything under version control.
6. **Compile locally.** Run `uv run python scripts/compile_experiment_design.py data/cohorts/<cohort_id>/spec/config.yaml --format csv --limit 5` to sanity-check output before Dagster loads it. The script shares the same loader as the asset.
7. **Materialize the cohort assets.** Trigger `cohort_id` followed by `cohort_membership` for the new partition. Inspect the generated `membership.csv` to confirm the design matches your sketch.
8. **Register and run downstream stages.** Once membership is persisted, the cohort partitions become available to draft, essay, and evaluation assets. Backfills should always reference the cohort ID rather than bespoke `gen_id` lists.
9. **Annotate learnings.** After the run, keep the spec around for reproducibility, or clone it into a new cohort ID when you iterate on the design.

## Choosing Rule Constructs

The spec DSL supports four rule types; consult the [reference](../spec_dsl.md#rule-blocks) for full syntax. Keep the cheat-sheet below handy when mapping a question into the right construct:

- **Subsets** constrain an axis to a named allowlist—ideal for freezing baselines or scoping to a vertical without touching other axes.
- **Ties** keep multiple axes in lockstep when they should select the same catalog entry (for example, essay and evaluation templates sharing a draft source).
- **Pairs** list specific two-way combinations so you can compare curated prompt ↔ rubric pairings without materializing the full cross-product.
- **Tuples** move three or more axes together when templates, models, or rubrics must change as a bundle.

After each rule executes, the compiler restores the original axis columns, so downstream assets still receive the familiar schema. Keep helper CSVs narrow—headers should match the axis names they constrain.

## Guardrails for Catalog and Metadata Hygiene

- **Treat `membership.csv` as the contract.** Downstream code only reads the `stage,gen_id` columns. Any additional context must be encoded via catalogs or manifest metadata inside the cohort build.
- **Seed generation metadata.** The cohort pipeline writes `data/gens/<stage>/<gen_id>/metadata.json` for every membership row. If you add new axes that downstream assets rely on, extend the seeding logic rather than mutating membership output.
- **Prune and rebuild when necessary.** If you need to reset partitions, run the `prune_dynamic_partitions` maintenance asset before re-materializing `cohort_membership` so Dagster forgets stale `gen_id`s.

## Common Design Scenarios

### Baseline vs. Variant Prompt

**Goal:** Compare a new draft prompt against an established baseline while holding evaluation constant.

1. Declare `draft_template` with both prompt IDs.
2. Add a `subset` rule or axis-level list for a single evaluation template.
3. Include a replicate axis (e.g., `draft_template_replicate: [1, 2, 3]`) so each prompt gets equal coverage.
4. Let the Cartesian product generate the combinations; no pairing is required if the evaluation template stays constant.

This design keeps `membership.csv` minimal: the only variance is the draft prompt and replicate index, making downstream analysis straightforward.

### Model Bake-Off with Shared Templates

**Goal:** Run the same prompts across multiple model families to assess sensitivity.

1. Axis sketch: `draft_template`, `essay_template`, `llm_model_id`.
2. Tie the draft and essay templates if essays should inherit drafts (e.g., `ties: draft_essay: {axes: [draft_template, essay_template]}`).
3. Use a `pair` rule to restrict which models run with each template if the catalog includes incompatible combinations.
4. Define a replicate axis (e.g., `model_replicate: [1, 2]`) only if you need multiple samples per model.

Keep catalog CSVs for models up to date; missing entries will fail validation during cohort materialization.

### Rubric Calibration Across Existing Essays

**Goal:** Evaluate a new rubric against essays already written in a previous cohort without regenerating drafts.

1. Clone the prior cohort's spec directory and change the cohort ID.
2. Replace the generation axes with a `@file:` include that lists the essay `gen_id`s you want to reuse.
3. Add a tuple that binds `evaluation_template`, `evaluation_llm`, and the imported essay IDs to ensure alignment.
4. Remove draft and essay stages from the axis list if they will not be re-run; the cohort build still needs `stage,gen_id` rows for evaluations only.

Remember to re-seed metadata for reused essays so evaluation assets can locate the parent draft information via the manifest.

## Validating and Iterating

- Run the CLI compiler (`scripts/compile_experiment_design.py`) with `--limit` before every Dagster run. It surfaces syntax errors and obvious imbalances quickly.
- Inspect `data/cohorts/<cohort_id>/manifest.json` when debugging rule interactions; it reveals the allowlists produced by the spec.
- When iterating, copy the previous cohort directory and increment the ID. This preserves history and lets you diff specs to see exactly which axis levels changed.
- Capture deviations or dropped behaviour in `REFactor_NOTES.md` or the cohort's `plans/` entry so reviewers understand intentional changes.

By treating the cohort pipeline as the contract for experiment design, you get deterministic partitions, clear provenance, and a reusable catalog-driven workflow for every study.
