# Plan: Make `content_combinations` a single-source reader of selected combos

## Context
- Today, `content_combinations` already reads only `data/2_tasks/selected_combo_mappings.csv` and fails fast when it’s missing or invalid (strict subset of `data/combo_mappings.csv`).
- We want to formalize this as the only supported mode and ensure the selected file can be produced by either:
  - Curation (existing selection scripts / manual lists), or
  - Automatic generation from active concepts (k_max, description_level) with stable `combo_id`s.

## Goals
- Single contract: `content_combinations` always reads `data/2_tasks/selected_combo_mappings.csv` (row‑subset of `data/combo_mappings.csv`).
- Two producers for the selected CSV:
  1) Curated selection (already supported via scripts/registration).
  2) Auto‑selection from active concepts (k_max) with stable IDs via `ComboIDManager`.
- Robust validation: fail fast on missing/invalid selection; keep subset/integrity checks.
- Clear docs and tests that reflect the single‑source design.

## Non‑Goals (now)
- No fallback in `content_combinations` to on‑the‑fly k‑max generation.
- No automatic background updates; selection is an explicit operator action.

## Architecture
- Source of truth (reader):
  - `content_combinations` reads `data/2_tasks/selected_combo_mappings.csv` only.
  - Validation: `selected ⊆ superset` (identical columns), unique `(combo_id, concept_id)`, valid concept IDs.
- Producers (writers):
  - Curated path (existing):
    - `scripts/register_partitions_for_generations.py` writes `selected_combo_mappings.csv` by filtering `data/combo_mappings.csv` to the chosen `combo_id`s.
    - `scripts/select_combos.py` filters `data/combo_mappings.csv` by an ID list.
  - Auto‑selection path (new):
    - New script `scripts/seed_selected_combos_from_kmax.py`:
      - Reads active concepts (`data/1_raw/concepts_metadata.csv`) and `k_max`/`description_level`.
      - Enumerates all size‑k combinations; uses `ComboIDManager.get_or_create_combo_id` to ensure stable `combo_id`s and append to `data/combo_mappings.csv` if missing.
      - Writes `data/2_tasks/selected_combo_mappings.csv` as a strict subset of the superset (same schema, not just minimal columns).
      - Optional flags: `--k-max`, `--description-level`, `--append` vs `--replace` (default replace).

## Execution Plan
1) Confirm single‑source read (done)
   - `content_combinations` no longer generates in‑memory combos; it only reads the selected CSV and validates (already implemented).

2) Add auto‑selection seeding script (new)
   - `scripts/seed_selected_combos_from_kmax.py`:
     - Inputs: `--k-max`, `--description-level`, `--data-root`.
     - Output: `data/2_tasks/selected_combo_mappings.csv` (full superset schema rows).
     - Logic:
       1) Load active concepts; enumerate size‑k combinations (deterministic order).
       2) For each combination, call `ComboIDManager.get_or_create_combo_id` (writes/validates superset rows as needed).
       3) Read `data/combo_mappings.csv`; filter rows to the enumerated `combo_id`s; write selected CSV.
       4) Print summary and integrity checks (row counts, unique combos, sample rows).

3) Keep curated writers (existing)
   - Leave `register_partitions_for_generations.py` and `select_combos.py` as is; both already produce `selected_combo_mappings.csv` by filtering the superset.

4) Validation and tests
   - Unit tests for the auto‑selection script using a tiny concepts fixture:
     - Produces expected `combo_id` count = C(n_active, k_max).
     - Selected CSV schema equals superset schema; subset invariant holds.
   - Ensure existing tests continue to pass (selected subset validator, no legacy curated name guard).

5) Docs
   - README + Guides:
     - Clarify that `content_combinations` always reads `selected_combo_mappings.csv`.
     - Show two ways to produce the selected CSV: curated vs. auto‑selection.
     - Include commands for the new seeding script and existing register/select scripts.
   - Architecture notes:
     - Document subset invariant and provenance: superset (`data/combo_mappings.csv`) is append‑only and versioned via `ComboIDManager`.

6) Operational notes
   - Auto‑selection is explicit (one‑shot); if active concepts or k_max change, re‑run the seeding script to refresh the selected CSV.
   - Dagster auto‑rematerialization picks up changes to `data/2_tasks/selected_combo_mappings.csv`.

## Risks / Mitigations
- Risk: Operators forget to seed the selected file → current fail‑fast error remains, with clear guidance.
- Risk: Divergence from superset schema → selection writer always filters superset rows, not a minimal projection.
- Risk: Duplicate or stale IDs in selected → validator enforces uniqueness and subset inclusion.

## Deliverables
- New script: `scripts/seed_selected_combos_from_kmax.py` (with unit tests and usage docs).
- Docs updates: README + guides + architecture notes on single‑source selection.
- CI/tests: ensure subset validator and no‑legacy guards remain green.

