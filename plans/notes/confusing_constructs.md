# Confusing Constructs Catalog

- `src/daydreaming_dagster/checks/documents_checks.py:66`: Asset checks registered via `globals()` mutation, obscuring actual exported names. Refactor to return explicit mappings in Definitions.
- `src/daydreaming_dagster/assets/group_cohorts.py:598`: Nested closures capture outer mutable state, making curated/cartesian flows hard to follow and test.
- `src/daydreaming_dagster/models/content_combination.py:38`: Combo IDs derived from Python’s salted `hash`, producing non-deterministic identifiers across runs.
- `src/daydreaming_dagster/unified/stage_core.py:48`: `_templates_root` duplicates logic already provided by `Paths`, leading to split configuration behaviour.
- `src/daydreaming_dagster/resources/llm_client.py:145`: Mandatory delay/retry decorators live inside the resource class, interleaving logging and control flow in a way that’s difficult to unit test.
- `src/daydreaming_dagster/schedules/raw_schedule.py:36`: Multiple tiny helpers (`_cron`, `_dagster_home`) obscure straightforward configuration and scatter environment lookups.
- `src/daydreaming_dagster/utils/generation.py:18`: Separate filesystem helpers reimplement `GensDataLayer`, leading to parallel write paths that can drift.
