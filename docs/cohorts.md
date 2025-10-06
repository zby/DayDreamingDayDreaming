# Cohorts

Cohorts capture a reproducible slice of the experiment space. Each cohort owns a spec bundle rooted at `data/cohorts/<cohort_id>/spec/` with `config.yaml` as the entry point (and optional `@file:` helpers in the same directory). The cohort assets compile that spec into a manifest and a canonical `membership.csv`. Downstream assets, scripts, and tooling only rely on the persisted `membership.csv`; the spec bundle and manifest are read exclusively by the cohort asset group while building those artifacts.

## Core artifacts

| Artifact | Location | Owner | Purpose |
| --- | --- | --- | --- |
| Spec bundle | `data/cohorts/<cohort_id>/spec/` | Authors | Declarative definition of combos, templates, models, and replication. Parsed by `CohortSpecResource` and compiled via `load_cohort_context`. |
| Manifest | `data/cohorts/<cohort_id>/manifest.json` | `cohort_id` asset | Snapshot of the allowlists derived from the spec. Used by `selected_combo_mappings` and `content_combinations` to hydrate combo metadata. |
| Membership table | `data/cohorts/<cohort_id>/membership.csv` | `cohort_membership` asset | Canonical list of stage/gen IDs for the cohort. Drives partition registration and downstream lookups. |
| Generation metadata | `data/gens/<stage>/<gen_id>/metadata.json` | `seed_cohort_metadata` | Pre-seeded to ensure generation assets have origin context before they run. |

## Lifecycle

1. **Choose a spec.** Set `DD_COHORT=<cohort_id>` or configure the `cohort_id` asset override. The asset loads `spec/config.yaml`, computes allowlists, and persists a manifest before registering the cohort as a dynamic partition for report assets.
2. **Compile membership.** The `cohort_membership` asset compiles the spec into draft/essay/evaluation rows, validates catalog coverage, seeds generation metadata, and writes `membership.csv`. The persisted CSV is slimmed to `stage,gen_id` while the in-memory DataFrame retains parent and template information.
3. **Register partitions.** `register_cohort_partitions` reads the returned DataFrame and registers every `gen_id` as an add-only dynamic partition for the draft, essay, and evaluation assets.
4. **Run stage assets.** Generation assets materialize per `gen_id` partition. They never re-read the spec or manifest; partition keys and the seeded metadata tell them which combos, templates, and parents to use.
5. **Process results.** Reporting assets query `membership.csv` through `MembershipServiceResource` (and helpers like `CohortScope`) to scope analytics to the cohort.

## `membership.csv` schema

`membership.csv` is the only cohort artifact that downstream code reads. It is a deduplicated two-column table with headers `stage,gen_id` written by `persist_membership_csv`. The in-memory membership DataFrame (available inside the asset run) also includes:

- `origin_cohort_id` — provenance for the generated asset.
- `parent_gen_id` — draft parent for essays, essay parent for evaluations.
- `combo_id`, `template_id`, `llm_model_id` — identifiers for reproducibility and metadata seeding.
- `replicate` — normalized integer replicate index.

Scripts and resources use `membership.csv` to filter partitions, drive backfills, and feed reporting pipelines. If you need richer context, join against the manifest or hydrated catalogs during the cohort build—never add extra columns to the CSV, because downstream assets only expect the two-column layout.

## Operational guidance

- **Spec curation.** Keep `spec/config.yaml` under version control and copy an existing cohort's `spec/` bundle when you need a starting point. Update axis allowlists and helper files manually so the definition reflects the catalog on disk.
- **Validation.** The cohort build validates parent integrity and catalog coverage before writing `membership.csv`. Keep catalog CSVs in sync with the spec bundle to avoid runtime failures.
- **Downstream access.** Inject `MembershipServiceResource` or `CohortScope` rather than reading CSVs manually. They handle filtering by stage and cohort ID and keep future schema changes centralized.
- **Resets.** To rebuild partitions from scratch, run the maintenance asset `prune_dynamic_partitions` before re-materializing `cohort_membership`. Then rematerialize the stage assets per `gen_id`.

By keeping the spec-to-membership translation inside the cohort asset group and treating `membership.csv` as the sole contract for downstream consumers, we maintain reproducible experiments while minimizing coupling between planning code and runtime assets.
