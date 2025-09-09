# Plan: Rename doc_id → gen_id and docs/ → gens/

## Scope
- Rename `doc_id` → `gen_id` everywhere (code, tests, metadata).
- Rename `parent_doc_id` → `parent_gen_id`.
- Rename filesystem paths `.../docs/...` → `.../gens/...`.
- Replace `reserve_doc_id` with `reserve_gen_id` (remove old API).
- Update all Dagster assets, IO, and readers/writers to new names.

## Decisions
- No backward compatibility window; single cutover.
- Migration strategy: copy then prune; ~12K files under `docs/`.
- No external systems depend on `docs/` or `doc_id`.
- Naming confirmed: `parent_gen_id` and `reserve_gen_id`.

## Steps
1) Confirm rename scope and entities.
2) Rename `doc_id` → `gen_id`.
3) Rename `parent_doc_id` → `parent_gen_id`.
4) Rename `docs/` paths → `gens/` paths.
5) Add `reserve_gen_id`; remove `reserve_doc_id`.
6) Update assets to emit DataFrames with new columns.
7) Update IO managers/readers/writers to use `gens/`.
8) Update Dagster `add_output_metadata` keys to `gen_id`.
9) Update tests/fixtures to new field names and paths.
10) Copy on-disk data from `docs/` → `gens/` (~12K files).
11) Prune `docs/` after validation of `gens/`.
12) Run test suite; fix any failures.
13) Remove any legacy mentions/shims.
14) Update docs/examples and developer notes.

## Notes
- Keep ID determinism identical to current behavior.
- Fail fast if `parent_gen_id` is missing where required.
- Avoid silent fallbacks; no dual-writing or compat layer.
- Verify Definitions import and asset wiring still valid after rename.
