# DayDreaming Pipeline Architecture

This document explains how the Dagster implementation in `src/daydreaming_dagster/`
is wired together and how it consumes the cohort contract described in
[`docs/cohorts.md`](cohorts.md). Read the cohorts guide first—it is the
authoritative reference for the spec bundle, manifest, and `membership.csv`
contract as produced from the spec DSL in [`docs/spec_dsl.md`](spec_dsl.md).
The architecture notes below focus on how those artifacts are used by the
assets, resources, and helpers inside the codebase.

## Core flow at a glance

```
cohort spec bundle (docs/cohorts.md)              raw catalogs (data/1_raw/*.csv)
                │                                              │
                ▼                                              ▼
      cohort asset group (group_cohorts.py) ───►  raw_data assets (raw_data.py)
                │                                              │
                ▼                                              ▼
    membership.csv + metadata seeds                 catalog DataFrames
                │
                ▼
   dynamic partitions (partitions.py)  ⇆  unified stage runner (unified/)
                │                                   │
                ▼                                   ▼
   draft / essay / evaluation gens (group_* assets + GensPromptIOManager)
                │
                ▼
   cohort-scoped reporting (results_processing.py, results_summary.py,
   results_analysis.py, scripts/build_pivot_tables.py)
```

The cohort assets translate a declarative spec into deterministic `gen_id`
partitions and seed the gens store with metadata. Stage assets then materialize
per-partition runs through the unified runner, and reporting assets consume the
persisted artifacts using the cohort membership contract.

## Architectural principles

1. **Cohort-first orchestration.** The cohort bundle is the only declarative
   input. Assets in `group_cohorts.py` compile it into `membership.csv`, which
   downstream code reads through helpers like `MembershipServiceResource` and
   `CohortScope`. No stage asset re-opens the spec or manifest directly. See
   [`docs/cohorts.md`](cohorts.md) for the expected contents of the spec bundle
   and [`docs/spec_dsl.md`](spec_dsl.md) for how structured couplings and
   factorial axes are authored.
2. **Deterministic partitions.** Generation IDs are derived from the
   `stage`-specific signatures in `daydreaming_dagster.utils.ids`. The IDs are
   written to disk and registered as Dagster dynamic partitions via
   `register_cohort_partitions` (`assets/partitions.py`). Re-materialising the
   same cohort with unchanged specs reuses the same IDs and yields Dagster
   `SKIPPED` runs.
3. **Filesystem-backed transparency.** All prompts, raw LLM responses, parsed
   outputs, and metadata live under `data/gens/<stage>/<gen_id>/` using the
   canonical filenames exported by `daydreaming_dagster.data_layer.paths`. This
   makes it possible to debug or reprocess artefacts offline.
4. **Unified execution primitives.** The stage assets call into the shared
   runner (`src/daydreaming_dagster/unified/`) so that prompt rendering, LLM
   execution, parsing, and persistence follow the same contract across draft,
   essay, and evaluation stages.

## Implementation map

| Layer | Modules | Notes |
| --- | --- | --- |
| Cohort compilation | `assets/group_cohorts.py`, `cohorts/`, `cohorts.md` | Builds `membership.csv`, seeds metadata via `seed_cohort_metadata`, and writes `manifest.json`. Validates against raw catalogs (`data/1_raw/*.csv`). |
| Dynamic partitions | `assets/partitions.py` | Registers per-stage `gen_id` partitions from the cohort membership DataFrame; add-only by design. |
| Stage execution | `assets/group_draft.py`, `assets/group_essay.py`, `assets/group_evaluation.py`, `unified/stage_core.py` | Assets invoke the unified runner with IO managers from the stage registry (see below). Essay assets can operate in `copy` mode by passing `pass_through_from` to the runner. |
| Raw inputs | `assets/raw_data.py` | Loads catalog CSVs and template text into Dagster assets. These are regular assets, not auto-refreshing source assets. |
| Reporting | `assets/results_processing.py`, `assets/results_summary.py`, `assets/results_analysis.py`, `results_summary/transformations.py`, `scripts/build_pivot_tables.py` | Operate exclusively on persisted gens artefacts and cohort membership lookups. |
| Shared utilities | `data_layer/paths.py`, `data_layer/gens_data_layer.py`, `resources/`, `utils/` | Provide filesystem indirection, IO managers, membership lookups, and parsing helpers. |

### Stage registry and resources

`src/daydreaming_dagster/definitions.py` builds the Dagster `Definitions` object
by combining:

- **Stage registry (`STAGES`).** Each stage entry enumerates its assets and
  exposes factories that build stage-specific IO managers. For example, the
  draft stage wires `draft_prompt_io_manager` (a `GensPromptIOManager`) and
  `draft_response_io_manager` (a `RehydratingIOManager`). Adding a new stage only
  requires extending the registry.
- **Shared resources.** The shared map injects the cohort spec compiler,
  membership service, experiment configuration, and CSV IO managers. These
  resources are accessed by cohort, generation, and reporting assets to resolve
  config and storage paths.

The `build_definitions` helper also loads the cohort assets, stage assets, raw
catalog assets (`RAW_SOURCE_ASSETS`), reporting assets, schedules, and asset
checks into a single Dagster `Definitions` object. The executor switches between
multi-process and in-process mode based on the `DD_IN_PROCESS` environment
variable.

### IO managers and data locations

All stage IO managers derive paths from `Paths`, which resolves the data root
(`data/` by default or `DAYDREAMING_DATA_ROOT` when set). Key helpers include:

- `generation_dir(stage, gen_id)` – gens store directory for a partition.
- `prompt_path`, `raw_path`, `parsed_path`, `metadata_path` – canonical file
  locations used by the unified runner and tests.
- `cohort_membership_csv(cohort_id)` – location of the canonical cohort
  membership table consumed by `MembershipServiceResource` and reporting
  assets.

`GensPromptIOManager` writes prompt files into the gens store, and the
`RehydratingIOManager` reloads parsed responses or metadata when assets are
retrieved.

### Cohort-aware helpers

- `seed_cohort_metadata` (called inside `cohort_membership`) populates each
  generation directory with a `metadata.json` stub so downstream stage runs know
  their parent IDs, templates, and cohort provenance before LLM calls happen.
- `MembershipServiceResource.stage_gen_ids` offers a thin wrapper around
  `utils.membership_lookup.stage_gen_ids`, allowing assets and scripts to pull
  stage-specific `gen_id` lists for the active cohort. Reporting assets use this
  to scope their work without touching raw CSVs.
- `ScoresAggregatorResource` reads `parsed.txt` and `metadata.json` for each
  evaluation `gen_id`, providing the foundation for `cohort_aggregated_scores`.

## Lifecycle summary (integrated with cohorts doc)

1. **Author a cohort spec.** Follow [`docs/cohorts.md`](cohorts.md) to produce a
   spec bundle under `data/cohorts/<cohort_id>/spec/`.
2. **Materialize cohort assets.** Run the `cohort_id` and `cohort_membership`
   assets. They load the spec via `CohortSpecResource`, validate against the
   raw catalogs (`raw_data.py`), write `manifest.json`, `membership.csv`, and
   seed gens metadata (`seed_cohort_metadata`).
3. **Register partitions.** `register_cohort_partitions` consumes the returned
   membership DataFrame and registers add-only dynamic partitions for draft,
   essay, and evaluation stages. Use `prune_dynamic_partitions` before this step
   if you need a clean slate.
4. **Execute stage assets per partition.** The unified runner renders prompts,
   makes LLM calls (or copies parsed drafts), parses outputs, and persists
   artefacts into the gens store. Parent/child relationships use the deterministic
   `gen_id` links generated during cohort compilation.
5. **Aggregate and analyse results.** Reporting assets read persisted files via
   `Paths` and `MembershipServiceResource`, produce cohort-scoped CSVs under
   `data/cohorts/<cohort_id>/reports/`, and expose convenience pivots and
   variance analyses.

This end-to-end flow keeps the experiment reproducible: the cohorts document
defines the contract, the architecture described here turns it into deterministic
Dagster partitions, and the reporting layer consumes only the persisted
artefacts and membership metadata.
