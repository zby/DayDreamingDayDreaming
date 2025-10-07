# DayDreaming Results

## Original DayDreaming Validation (`best_novelty_all_evals`)

The first experiment set out to verify whether pre-summer-2025 LLMs could independently recreate Gwern's "Daydreaming Loop". We ran a two-stage pipeline (draft → essay → evaluation) over 4–5 concept blends and scored every essay with 11 evaluation templates across two models (`gemini_25_pro`, `sonnet-4`).

| Rank | Essay ID | Avg Score | Draft Template | Essay Template | Generation Model |
|------|----------|-----------|----------------|----------------|------------------|
| #1 | e_sq5klak2lljyyom | 8.77 / 9 | creative-synthesis-v10 | creative-synthesis-v10 | gemini_25_pro |
| #2 | e_1cx6440bb5zj9466 | 8.75 / 9 | creative-synthesis-v10 | creative-synthesis-v10 | gemini_25_pro |
| #3 | e_4nqjjtqtnpxlljlz | 8.52 / 9 | creative-synthesis-v7 | parsed-from-links-v1 | deepseek_r1_p |

All three essays are copied verbatim in this directory (`e_sq5klak2lljyyom.txt`, `e_1cx6440bb5zj9466.txt`, `e_4nqjjtqtnpxlljlz.txt`). Each one explains the generator–verifier loop, ties it to data flywheels, and argues for an economic moat—exactly the behaviours we wanted to see when probing for independent rediscovery.

## Follow-up Cohort (`creative-synthesis-gap-v1`)

We later repeated the study with refreshed prompts and evaluation weighting to close a few scoring gaps. The top two essays from that cohort are now included:

| Essay ID | Avg Score | Notes |
|----------|-----------|-------|
| e_2b4k6tvo4kf7ibjh | 8.8 / 9 | Keeps the Daydreaming mechanism intact while elaborating on continuity plans for human evaluators. |
| e_3hdt16ed4bh528s0 | 8.6 / 9 | Frames the generator–verifier loop as a defendable moat and grounds the argument in feedback data. |

Both texts (`e_2b4k6tvo4kf7ibjh.txt`, `e_3hdt16ed4bh528s0.txt`) are the raw outputs produced by the pipeline. Reading them alongside the originals shows how template adjustments change tone and emphasis without losing the core mechanism.

## How to Treat the Scores

LLM scoring is a convenience layer—use it to triage, not to replace human judgement. The numbers simply elevated essays that already:

1. Restate the original Daydreaming AI insight (generator/verifier loop with iterative feedback).
2. Add their own supporting detail instead of paraphrasing the concept descriptions.
3. Stay coherent despite relying on older, freely accessible endpoints.

We recommend reading the essays yourself to confirm those behaviours.

## Two-Stage vs Single-Stage Runs

Both cohorts above employ the full two-stage pipeline (draft then essay). The Dagster assets also support single-stage experiments—skip the draft materialisation and use essay templates that read the concept combinations directly. That pathway is useful when you want a faster baseline or when comparing staged vs unstaged generations.

## Model Availability

We purposely limited the experiments to models that were freely available during Q1 2025. Release timestamps are documented in `data/openrouter_free_models.csv`:

- `deepseek/deepseek-r1` — released 20 Jan 2025 (used for reasoning-heavy drafts and essays).
- `google/gemini-2.5-pro-exp-03-25` — released 25 Mar 2025 (one of the primary draft/evaluation models).

Evaluation also uses `sonnet-4`; the exact endpoint is recorded in `data/1_raw/llm_models.csv`. When running new cohorts, capture the release date of any updated endpoints so comparisons stay honest.

## Replication Checklist

Step-by-step instructions are in the top-level `README.md`:

1. Register partitions with `register_cohort_partitions` for the cohort you care about.
2. Materialise the `generation_draft`, `generation_essay`, and `generation_evaluation` asset groups (skip `generation_draft` for single-stage runs).
3. Inspect the CSVs under `data/cohorts/<cohort>/reports/` and the essay copies in this directory.

The entire input stack—concept descriptions, templates, and model identifiers—lives in `data/1_raw/`, so the experiments are reproducible without hitting external services beyond the LLM endpoints.

---

Original cohort generated: 15 Jan 2025 • Follow-up updates: 06 Oct 2025
