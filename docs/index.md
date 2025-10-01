# DayDreaming Documentation

Start here to find the right guide for your task.

## Quickstarts
- Curated Selection: docs/cohorts.md#curated-selection-quickstart

## Core Concepts
- Cohorts and Membership: docs/cohorts.md
- One-phase essay (copy mode): docs/guides/one_phase_copy_mode.md
- Unified Runner: docs/architecture/architecture.md#unified-stage-runner
- Two-Phase Generation (draft → essay): README.md#two-phase-generation-draft--essay

## How‑To Guides
- Add/activate templates: docs/guides/operating_guide.md#restrict-generation-templates
- Add/activate models: docs/guides/operating_guide.md#controlling-overwrites-of-generated-files
- Run evaluation only: docs/cohorts.md#running-the-curated-set

## Operations
- Operating Guide: docs/guides/operating_guide.md
- Schedules and auto‑refresh: docs/guides/operating_guide.md#raw-csv-change-handling-schedule

## Reference
- Membership CSV schema: docs/cohorts.md#what-the-assets-do
- Directory layout and gens store: README.md#data--partitions

## Maintenance Cadence
| Doc | Owner | Review cadence / validation |
| --- | --- | --- |
| README.md | Pipeline maintainers | Quarterly: verify quickstart commands and environment variables align with `src/daydreaming_dagster/definitions.py` (check `DAYDREAMING_DATA_ROOT`). |
| docs/architecture/architecture.md | Pipeline maintainers | Quarterly: rerun `scripts/data_checks/report_gen_id_lengths.py` and `scripts/data_checks/recompute_gen_ids.py`; confirm stage registry matches Definitions wiring. |
| docs/guides/operating_guide.md | Operations rotation | Monthly: spot-check curated selection flow (`scripts/select_top_essays.py`) and IO manager troubleshooting steps. |
| docs/cohorts.md | Cohort tooling owners | Monthly during active runs: ensure CLI snippets (`cohort_id,cohort_membership`) and selection script usage stay current. |
| docs/guides/gen_id_migration_analysis.md | Migration stewards | After migrations or quarterly: rerun `report_gen_id_lengths.py` and update metrics/notes. |

Set a quarterly reminder (calendar or CI checklist) to confirm the validation scripts run and numbers in the docs are refreshed.
