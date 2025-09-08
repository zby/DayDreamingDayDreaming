# Back‑Compat Fallbacks Audit and Cleanup Plan

Date: YYYY-MM-DD
Owner: Daydreaming Dagster
Scope: Identify backcompat/legacy fallbacks and propose safe removals to reduce cruft.

## Summary

- No Python-version/library-compat cruft detected (no `typing_extensions`, `sys.version_info`, Pydantic v1/v2 bridges, or `pkg_resources` fallbacks).
- Back-compat is concentrated in data schema and filesystem layout support. Core themes:
  - Legacy “links/one‑phase” naming coexisting with two‑phase (`draft`/`essay`).
  - Accepting old CSVs/columns (`link_templates.csv`, `link_task_id`, `link_template`).
  - Locating/reading files from legacy directories (`links_responses/`, `generation_responses/`, `parsed_generation_responses/`).
  - IO managers reading unversioned `{pk}.txt` as fallback.
  - Parsers supporting legacy formats (`section_headers`, “fallback” parsing).
  - Migration/backfill scripts that include legacy paths by design.

The codebase appears already strongly oriented to the modern two‑phase system. Below are specific findings with proposed actions.

## Findings by Area

### IO Managers

- File: `daydreaming_dagster/resources/io_managers.py`
  - `VersionedTextIOManager.load_input()` falls back to unversioned files (`{pk}.txt`).
  - `PartitionedTextIOManager` itself reads/writes unversioned `{pk}.txt` (expected by design) but is separate from versioned manager.

Recommendation:
- If all new artifacts are versioned, consider removing the legacy fallback in `VersionedTextIOManager` and error if no versioned file exists. Alternatively, keep for safety and schedule a time‑boxed deprecation:
  - Phase 1: Warn when reading legacy file, emit metric.
  - Phase 2: Turn warning into error and remove fallback.

Risk: Low–Medium (depends on whether any historical runs still read unversioned files).

---

### Raw Readers and Sources

- File: `daydreaming_dagster/utils/raw_readers.py`
  - `read_draft_templates()` prefers `draft_templates.csv` but falls back to `link_templates.csv`.
- File: `daydreaming_dagster/assets/raw_data.py`
  - Defines `LINK_TEMPLATES_KEY` (deprecated) and a SourceAsset for `link_templates.csv`.
- File: `daydreaming_dagster/schedules/raw_schedule.py`
  - `RAW_CSVS` includes `data/1_raw/link_templates.csv` with a deprecated comment.

Recommendation:
- Drop fallback to `link_templates.csv` in `read_draft_templates` once data is migrated.
- Remove `LINK_TEMPLATES_KEY` SourceAsset, and remove `link_templates.csv` from `RAW_CSVS`.
- Ensure docs and tests no longer refer to `link_templates.csv`.

Risk: Low (as long as `data/1_raw/draft_templates.csv` is present and used everywhere).

---

### Document Location

- File: `daydreaming_dagster/utils/document_locator.py`
  - Searches legacy directories: `links_responses/`, `generation_responses/`, `parsed_generation_responses/` after modern `essay_responses/` and `draft_responses/`.

Recommendation:
- Narrow to `essay_responses/` and `draft_responses/` only. For historical inspection workflows, move legacy scanning to a dedicated archival helper function or script.

Risk: Medium (scripts/tests referencing old dirs will need updates).

---

### Parsers (Generation/Evaluation)

- File: `daydreaming_dagster/utils/generation_response_parser.py`
  - Strategy supports `xml_tags` (modern), `section_headers` (legacy templates list), and a generic `fallback` path.
  - `section_header_templates` enumerates older templates (`creative-synthesis-v3`, `research-discovery-v3`).
- File: `daydreaming_dagster/utils/eval_response_parser.py`
  - Provides a `complex` legacy pattern and `in_last_line` modern path. This appears robust rather than purely back‑compat.

Recommendation:
- If legacy templates are retired, remove `section_headers` branch and list. Keep a generic fallback parser for resilience, or gate it with a feature flag for strict mode.
- Keep evaluation parser coverage; it doubles as robustness to prompt drift.

Risk: Low–Medium (only if truly no legacy template responses remain).

---

### Assets and Cross‑Experiment Glue

- File: `daydreaming_dagster/assets/cross_experiment.py`
  - Uses `essay_row.get("draft_template", essay_row.get("link_template", "unknown"))` in two places.

Recommendation:
- Require `draft_template` and remove `link_template` fallback once all tables are migrated. Validate columns upfront and raise clear errors in dev.

Risk: Low.

---

### Tests Indicating Transitional Support

- `daydreaming_dagster/resources/test_versioned_io_manager.py`
  - Verifies legacy read fallback for versioned IO manager. Update/retire with fallback removal.
- `daydreaming_dagster/assets/test_two_phase_generation.py`
  - Accepts legacy `links_block` in templates. Remove once legacy templates dropped.
- `tests/test_pipeline_integration.py`
  - Accepts both legacy `combo_XXX` and new stable `combo_vN_*` formats. Decide on final ID policy; then narrow tests accordingly.
- `daydreaming_dagster/test_no_legacy_curated_name.py`
  - Ensures no references to `curated_combo_mappings.csv` (already moving in right direction).

Recommendation:
- Stage test updates aligned to each code removal. Prefer failing fast for legacy inputs.

---

### Scripts (Migration/Backfill/Tools)

- File: `scripts/build_generation_results_table.py`
  - Explicitly supports legacy dirs and tasks; prints legacy tables summary.
- File: `scripts/copy_essays_with_drafts.py`
  - Falls back to `links_responses/`, `link_templates.csv`, and accepts `link_*` columns.
- File: `scripts/migrate_generated_tables_to_draft_schema.py`
  - Migration utility that preserves legacy columns (by design).
- Files: `scripts/find_legacy_evals.py`, `scripts/find_malformed_legacy_links.py`
  - Legacy analysis/migration aids.

Recommendation:
- Keep these scripts as archival utilities but mark them as “legacy/migration” in headers, or move to `scripts/legacy/`.
- If desired, hard‑fail on legacy fallbacks by default and add `--allow-legacy` flags for opt‑in.

Risk: None for re‑org; Low for changing default behavior.

## Proposed Cleanup Plan

1) Data sources and schedule
   - Remove `link_templates.csv` from: `raw_readers.py` fallback, `assets/raw_data.py` SourceAsset, `schedules/raw_schedule.py` `RAW_CSVS`.
   - Update docs to reference only `draft_templates.csv`.

2) Document locator
   - Limit to `essay_responses/` and `draft_responses/` only. Provide a separate archival locator in `scripts/legacy/` if needed.

3) Cross‑experiment and assets
   - Remove `link_template`/`link_task_id` fallbacks in code paths and require canonical fields.
   - Fail early with clear messages if legacy columns are encountered.

4) IO managers
   - Add a deprecation warning when `VersionedTextIOManager` reads unversioned files; log once per partition.
   - After a grace period, remove the fallback (or gate by env var `STRICT_VERSIONED_IO=1`).

5) Parsers
   - Remove `section_headers` strategy if legacy templates are fully migrated; keep `xml_tags` + strict fallback toggle.

6) Tests
   - Update or remove tests that assert legacy acceptance. Add tests that ensure strict failures where applicable.

7) Scripts
   - Move legacy‑backfill/migration scripts into `scripts/legacy/` and add header notes. Optionally add `--allow-legacy` flags.

## Validation Checklist

- Run unit tests focused on changed modules (`uv run pytest daydreaming_dagster/...` selectors).
- Materialize a small subset of assets with modern inputs to verify no legacy paths are exercised.
- Grep for `link_template`, `links_responses`, `generation_responses`, `parsed_generation_responses` and ensure only scripts/legacy remain.
- Confirm Dagster dev runs cleanly and the daemon auto‑updates on `data/1_raw` changes without the removed legacy CSV.

## Open Questions

- Are there any historical runs still requiring reads from `generation_responses/` or `links_responses/`? If yes, isolate usage to scripts only.
- Is there any external consumer depending on `link_*` columns in `2_tasks/*_generation_tasks.csv`? If yes, plan a CSV migration first.

## Appendices: Quick Grep Targets

- Legacy dirs: `links_responses|generation_responses|parsed_generation_responses`
- Legacy columns: `link_task_id|link_template|link_templates.csv|content_combinations_csv.csv`
- IO fallback: `VersionedTextIOManager._legacy_path`, `.load_input()` fallback
- Parsers: `section_header_templates`, `ParseStrategy = Literal[... "fallback"]`

