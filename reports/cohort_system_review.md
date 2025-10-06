# Cohort System Review

## Overview of the current workflow

- Cohort execution starts in the `cohort_id` Dagster asset. It loads the selected spec bundle by calling `load_cohort_context`, derives allowlists, template modes, and replication settings, and emits a manifest snapshot before registering the cohort partition.
- The manifest is persisted to `data/cohorts/<cohort_id>/manifest.json` and lists all combos, templates, LLMs, and replication factors that the cohort spec referenced.
- `cohort_membership` uses the compiled spec definition to generate draft, essay, and evaluation rows, validates catalog coverage, seeds per-generation metadata, writes `membership.csv`, and returns the wide membership DataFrame.
- `persist_membership_csv` trims the DataFrame down to the stage and `gen_id` columns, deduplicates rows, and writes the canonical `data/cohorts/<cohort_id>/membership.csv` file.
- After membership is written, `register_cohort_partitions` registers each `gen_id` as a Dagster dynamic partition so downstream stage assets can materialize using cohort-specific identifiers.

## Downstream consumption patterns

- Stage assets (draft/essay/evaluation) operate entirely on dynamic partitions that were created from `membership.csv`, resolving prompts and raw/parsed artifacts per `gen_id`. They do not read the spec or manifest directly once partitions exist.
- Results processing uses an injectable `MembershipServiceResource` that wraps `membership_lookup.stage_gen_ids`, which in turn scans cohort directories for `membership.csv` files and returns `gen_id` lists.
- Operational helpers such as `CohortScope` also read per-cohort `membership.csv` snapshots to answer stage/`gen_id` queries.
- Outside of the cohort asset group, we do not import the spec bundle or manifest; the persisted `membership.csv` is the authoritative interface for determining which generations belong to a cohort.

## Areas of complexity

- The `cohort_membership` asset performs many responsibilities: compiling the spec, generating membership rows, validating catalogs, seeding metadata, writing `membership.csv`, and publishing Dagster metadata. This makes it difficult to reason about each step independently.
- Spec compilation and allowlist derivation have overlapping helpers spread across `cohort_id`, `load_cohort_context`, and the spec planner utilities. Understanding which helper owns catalog validation versus template mode inference requires tracing through several modules.
- The manifest is stored alongside `membership.csv` but is only used by the cohort asset group (e.g., `selected_combo_mappings` and `content_combinations`). Downstream assets rely exclusively on `membership.csv`, so maintaining both structures doubles the surface area that has to stay in sync.

## Potential redesign directions

1. **Split membership generation responsibilities.** Extract catalog validation, metadata seeding, and CSV persistence into separate Dagster ops or helpers so each concern can be tested and evolved independently. A thinner asset could focus solely on translating the spec into membership rows, with explicit dependencies for validation and persistence.
2. **Clarify the manifest contract.** If downstream code never reads `manifest.json`, consider dropping it in favor of derived views over `membership.csv` (e.g., compute combo allowlists from membership rows). Alternatively, document the scenarios where manifest data is required so future changes do not inadvertently break internal tooling.
3. **Introduce a single membership service.** Consolidate `membership_lookup`, `CohortScope`, and CSV readers into one injectable component that exposes high-level queries (e.g., fetch essays with parents). This would reduce duplicate CSV parsing logic and centralize the contract around `membership.csv`.

These steps would keep `membership.csv` as the canonical artifact while reducing the amount of bespoke glue scattered across the cohort asset implementations.
