# One‑Phase Pipeline (Essay Copy Mode)

This guide shows how to run a “one‑phase” pipeline where the Phase‑2 essay is a verbatim copy of the parsed draft. This is useful for baselines, parser validation, or when you want to evaluate draft quality directly without an LLM essay step.

Key idea
- Essay templates declare a `generator` mode in `data/1_raw/essay_templates.csv`:
  - `copy`: copy parsed draft → essay (no LLM call in essay stage)
  - `llm`: generate essay from draft via LLM

Prerequisites
- Ensure drafts (Phase‑1) are configured and can run: entries exist in `data/1_raw/draft_templates.csv` and at least one model is listed in `data/1_raw/llm_models.csv` that you plan to use.
- Include at least one essay template with `generator=copy` in the cohort spec. The default repo includes `parsed-from-links-v1` as an example.

Setup
1) Configure essay copy template
   - Edit `data/1_raw/essay_templates.csv` and ensure one row sets `generator=copy` (e.g., `parsed-from-links-v1`). Reference that `template_id` in the cohort spec.
   - Optional: limit the cohort spec to only copy-mode essays to avoid mixing modes in catalog expansions.
2) Build cohort
   - Catalog mode: ensure `data/combo_mappings.csv` contains the desired combos and materialize `cohort_id` (which writes the manifest) followed by `selected_combo_mappings`/`content_combinations` to hydrate combo content:
     - `uv run dagster asset materialize --select "cohort_id,selected_combo_mappings,content_combinations,cohort_membership" --partition "<cohort_id>" -f src/daydreaming_dagster/definitions.py`
3) Materialize partitions
   - Drafts: `uv run dagster asset materialize --select "group:generation_draft" --partition "<draft_gen_id>" -f src/daydreaming_dagster/definitions.py`
   - Essays (copy mode): `uv run dagster asset materialize --select "group:generation_essays" --partition "<essay_gen_id>" -f src/daydreaming_dagster/definitions.py`
     - The `essay_prompt` will surface `COPY_MODE: ...` and the `essay_raw`/`essay_parsed` assets will copy the parsed draft content.
   - Evaluations: `uv run dagster asset materialize --select "group:evaluation" --partition "<evaluation_gen_id>" -f src/daydreaming_dagster/definitions.py`
4) Results
   - Parse and summarize: `uv run dagster asset materialize --select "parsed_scores,final_results" -f src/daydreaming_dagster/definitions.py`

Notes and gotchas
- Parent link is mandatory: essays read the draft via `parent_gen_id`. The cohort membership ensures this link exists.
- Mixed cohorts: in catalog mode, every draft combines with all essays (both `copy` and `llm`). For a pure one‑phase baseline, constrain the cohort spec to copy-mode templates or use a separate cohort.
- Cohort partitions: create `data/cohorts/<cohort_id>/spec/config.yaml` before launching Dagster (or reload definitions) so the static `cohort_spec_partitions` includes the new cohort. Stage assets still rely on dynamic `gen_id` partitions, so ensure `cohort_membership` runs first (or with the daemon) before materializing drafts/essays.

Where it’s enforced in code
- Essay generator mode resolution: `daydreaming_dagster/unified/stage_core.py::resolve_generator_mode`
- Essay copy execution path: `daydreaming_dagster/unified/stage_raw.py::_stage_raw_asset` and `stage_core.execute_copy`
- Cohort Cartesian expansion: `daydreaming_dagster/assets/group_cohorts.py::cohort_membership`

Design discussion: mixing one‑phase and two‑phase
- Full catalog expansion across draft and essay templates will produce both one‑phase (copy) and two‑phase (LLM) essays if both types are selected. This is intentional but can be confusing when analyzing results.
- Recommended patterns:
  - Separate cohorts per mode (e.g., `cohort-2025-09-11-copy` vs `cohort-2025-09-11-llm`).
  - If you need strict pairing, consider tagging templates into families and adding a lightweight join rule in `cohort_membership` (not implemented by default) to pair only within a family.
