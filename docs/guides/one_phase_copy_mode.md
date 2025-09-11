# One‑Phase Pipeline (Essay Copy Mode)

This guide shows how to run a “one‑phase” pipeline where the Phase‑2 essay is a verbatim copy of the parsed draft. This is useful for baselines, parser validation, or when you want to evaluate draft quality directly without an LLM essay step.

Key idea
- Essay templates declare a `generator` mode in `data/1_raw/essay_templates.csv`:
  - `copy`: copy parsed draft → essay (no LLM call in essay stage)
  - `llm`: generate essay from draft via LLM

Prerequisites
- Ensure drafts (Phase‑1) are configured and can run: active rows in `data/1_raw/draft_templates.csv` and at least one generation‑capable model in `data/1_raw/llm_models.csv` (`for_generation=true`).
- Have at least one essay template with `generator=copy` active. The default repo includes `parsed-from-links-v1` as an example.

Setup
1) Configure essay copy template
   - Edit `data/1_raw/essay_templates.csv` and set one row to `active=true` and `generator=copy` (e.g., `parsed-from-links-v1`).
   - Optional: deactivate `llm` essay templates to avoid mixing modes in Cartesian cohorts.
2) Build cohort
   - Cartesian: select combos (via `selected_combo_mappings.csv`) and ensure desired draft templates/models are active; then:
     - `uv run dagster asset materialize --select "cohort_id,cohort_membership" -f daydreaming_dagster/definitions.py`
   - Curated: create `data/2_tasks/selected_essays.txt` with essay `gen_id`s from prior runs; then materialize the two assets as above.
3) Materialize partitions
   - Drafts: `uv run dagster asset materialize --select "group:generation_draft" -f daydreaming_dagster/definitions.py`
   - Essays (copy mode): `uv run dagster asset materialize --select "group:generation_essays" -f daydreaming_dagster/definitions.py`
     - The `essay_prompt` will return `COPY_MODE: ...` and `essay_response` will copy the parsed draft content.
   - Evaluations: `uv run dagster asset materialize --select "group:evaluation" -f daydreaming_dagster/definitions.py`
4) Results
   - Parse and summarize: `uv run dagster asset materialize --select "parsed_scores,final_results" -f daydreaming_dagster/definitions.py`

Notes and gotchas
- Parent link is mandatory: essays read the draft via `parent_gen_id`. The cohort membership ensures this link exists.
- Mixed cohorts: in Cartesian mode, every active draft combines with all active essays (both `copy` and `llm`). For a pure one‑phase baseline, deactivate `llm` essays or use a separate cohort.
- Dynamic partitions: ensure `cohort_membership` runs first (or with the daemon) so draft/essay/evaluation partitions exist before materializing stage assets.

Where it’s enforced in code
- Essay generator mode resolution: `daydreaming_dagster/assets/group_generation_essays.py::_get_essay_generator_mode`
- Essay copy path: `daydreaming_dagster/assets/group_generation_essays.py::_essay_response_impl` (mode == `copy`)
- Cohort Cartesian expansion: `daydreaming_dagster/assets/group_cohorts.py::cohort_membership`

Design discussion: mixing one‑phase and two‑phase
- Full Cartesian expansion across draft and essay templates will produce both one‑phase (copy) and two‑phase (LLM) essays if both types are active. This is intentional but can be confusing when analyzing results.
- Recommended patterns:
  - Separate cohorts per mode (e.g., `cohort-2025-09-11-copy` vs `cohort-2025-09-11-llm`).
  - Or use curated mode to pick only the essays you want to evaluate together.
  - If you need strict pairing, consider tagging templates into families and adding a lightweight join rule in `cohort_membership` (not implemented by default) to pair only within a family.

