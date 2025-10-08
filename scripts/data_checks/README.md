# Data Check Scripts

Small utilities for auditing the gens store and raw configuration files.

- `check_cohort_parsed_coverage.py`: report `parsed.txt` coverage for the partitions registered in a cohort membership (per stage and overall).
- `check_gens_store.py`: validate required gens-store artifacts and optionally prune incomplete evaluation runs.
- `find_duplicate_generations.py`: hash `raw.txt` payloads to highlight duplicate generations by stage.
- `find_missing_combo_concepts.py`: cross-check `combo_mappings.csv` against `concepts_metadata.csv` to spot missing concept rows.
- `parent_chain_check.py`: ensure essay/evaluation parents (and grandparent drafts) exist with readable metadata.
- `report_metadata_health.py`: surface gens-store directories missing metadata and highlight resumptions captured via the `resume` flags.
- `report_gen_id_lengths.py`: summarize generation ID length distribution per stage (helps track migration progress).
- `restore_evaluation_parent_ids.py`: repair evaluation metadata missing `parent_gen_id` by matching essays via `parent_task_id` and replicate.
- `relink_prompts_to_parents.py`: ensure essay/evaluation prompts embed their parent text and optionally repair `parent_gen_id` when a unique match is found.
- `prune_empty_generations.py`: delete generation directories that have no prompt/raw/parsed data once their origin cohort has been removed.
- `recompute_gen_ids.py`: recompute deterministic IDs from metadata and surface mismatches or signature errors.
- `templates_without_generations.py`: list templates with zero gens-store usage (supports stage filters and draft-only listings).
- `validate_template_csvs.py`: enforce schema expectations for the template CSVs under `data/1_raw/`.

Each script accepts `--data-root` (default `data`). Run with `--help` for details.
