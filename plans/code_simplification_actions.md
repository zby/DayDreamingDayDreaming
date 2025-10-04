# Plan: Code Simplification Actions

## Core Foundations
- **Drop config shim** — `src/daydreaming_dagster/config/paths.py:3` only re-exports `Paths` and friends. `rg` shows no imports from `daydreaming_dagster.config`. Remove the module and update `__all__` in `src/daydreaming_dagster/__init__.py` if necessary. *Verification:* run `rg "config.paths"` to confirm zero call sites before deletion, then run `pytest` and `mypy` (if configured) to ensure nothing depended on the shim.

## Cohort Planning
- ✅ **Retire `CohortPlan*` aliases** — `src/daydreaming_dagster/cohorts/spec_planner.py` and `src/daydreaming_dagster/cohorts/__init__.py` no longer export the legacy plan helpers. `rg "CohortPlan"` now returns 0 results outside this plan file; cohort spec bundle tests continue to pass.

## Data & Assets
- ✅ **Fail fast on missing membership** — `cohort_aggregated_scores_impl` no longer masks `FileNotFoundError`; assets/tests now rely on explicit membership failures.
- ✅ **Simplify spec catalogs** — `_build_spec_catalogs` removed the `combo_mappings.csv` backfill path; catalogs now hydrate strictly from raw template/LLM tables.

## Resource Layer
- **Trim model-id mapping in LLM client** — `src/daydreaming_dagster/resources/llm_client.py:42` loads `llm_models.csv` and silently maps ID → provider model names. To reduce hidden behaviour, require the prompt templates to supply provider-ready model strings and delete `_load_model_map` / `_resolve_model_name`. *Verification:* audit template CSVs to ensure they already contain provider identifiers, update any fixtures, then run `unified` stage tests and pipeline smoke test.
- **Relax ultra-conservative rate limiting** — the same file enforces `mandatory_delay=65s` and wraps calls with `ratelimit`/`tenacity` decorators (`resources/llm_client.py:120`). Evaluate replacing this with a simpler retry-on-429 strategy so the client fails fast instead of sleeping for a minute per call. *Verification:* add unit tests for the revised retry logic and run the staged generation service tests.

- ✅ **Prune cohort-scope extras** — Removed the unused signature cache, metadata helpers, `GenerationScope`, and `find_existing_gen_id`; tests now focus solely on membership lookups.

## Follow-up
- Track progress by checking off each item above and capturing any unexpected dependencies that block deletion. Update this plan (or add notes) if new simplification targets emerge during implementation.
