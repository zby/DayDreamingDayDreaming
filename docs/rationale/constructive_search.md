## Constructive Search and Goal Alignment (Current Approach)

**Intent**
- Document how the constructive-search loop uses a finite combo catalog plus fixed template grammar to satisfy the existence goal under tractable budgets.【F:docs/rationale/project_goals.md†L22-L36】

**Search Envelope**
- Cohort planning scopes combos through `selected_combo_mappings` and `content_combinations`, which hydrate entries from the append-only `data/combo_mappings.csv` catalog before draft generation runs.【F:docs/cohorts.md†L9-L36】【F:data/README.md†L153-L180】
- `build_content_combinations` resolves each `combo_id` to concept objects, preserves the CSV row order within a combo, and raises `DDError(DATA_MISSING)` when catalog references drift out of sync, keeping failures early and deterministic.【F:src/daydreaming_dagster/cohorts/content_combination_builder.py†L42-L97】
- `ContentCombination.from_concepts` maintains the provided concept sequence, applies deterministic description fallbacks, and records level metadata so templates receive a stable payload per combo.【F:src/daydreaming_dagster/models/content_combination.py†L18-L152】

**Template Grammar**
- Latest cohorts (e.g., `creative-synthesis-gap-v1`) pair `creative-synthesis-v10` drafts with `creative-synthesis-v10` or `parsed-from-links-v1` essays, so the constructive loop remains a two-stage pipeline with deterministic prompt text and explicit essay parses.【F:data/cohorts/creative-synthesis-gap-v1/spec/config.yaml†L1-L44】
- The creative-synthesis draft family lives alongside the rolling-summary prompts in the catalog (`data/1_raw/draft_templates.csv`), while copy-mode essays such as `parsed-from-links-v1` are tracked in `data/1_raw/essay_templates.csv`; swapping grammar or generators is therefore a data edit, not a code change, which aligns with the versioned-template guidance in the project goals.【F:data/1_raw/draft_templates.csv†L10-L21】【F:data/1_raw/essay_templates.csv†L7-L7】【F:docs/rationale/project_goals.md†L65-L73】

**Why This Scaffold Works**
- Single-link attachments and deterministic summaries keep branching shallow while still documenting causal steps.
- Canonical combo order removes incidental variation, letting evaluation compare essays on content rather than permutation noise.
- Optional micro-tests and rolling context give adaptive propose-verify policies hooks to prune early and reuse partial work when a combo is rejected.

**Implementation Touchpoints**
- `selected_combo_mappings` filters the catalog to the cohort manifest and locks ordering for audit trails.【F:src/daydreaming_dagster/assets/group_cohorts.py†L343-L403】
- `content_combinations` materializes resolved combinations once per run so stage assets share an in-memory view instead of re-reading CSVs.【F:src/daydreaming_dagster/assets/group_cohorts.py†L405-L426】
- Draft-stage helpers request combos via `needs_content_combinations`, ensuring each generation partition receives the same hydrated objects without bespoke IO code.【F:src/daydreaming_dagster/assets/stage_asset_helpers.py†L44-L85】

**Existence Sketch**
- For a fixed `k_max`, the combo catalog × template grammar defines a finite, enumerable space; Dagster partitions walk that space without prompt drift, satisfying the practical existence criterion in the project goals.【F:docs/rationale/project_goals.md†L22-L36】
- Reinvention gates stay tied to versioned originality rubrics, so the verifier only accepts essays when every novel element is present with coherent justification (no reward for paraphrase alone).【F:data/results/RESULTS.md†L11-L70】

**Practicality Levers**
- Adaptive propose-verify trims proposals-per-accept by inspecting draft micro-tests or canonicalized intermediate notes before escalating to full essays.
- Canonical IDs in `combo_mappings.csv` allow memoizing evaluation results and collapsing duplicates across cohorts.
- Tightening description levels or swapping prompt scaffolds remains a catalog edit, enabling rapid iteration without destabilizing orchestration.

**Metrics & Stopping**
- Track proposals-per-accept, novelty@K per cost, and coverage/diversity across the enumerated slice; stop once reinvention gains hit diminishing returns under the declared budgets.

**Assumptions**
- Neutral, versioned concepts and combos live in tracked catalogs; evaluators remain faithful to the rubric; policies maintain non-zero exploration even when APV prunes aggressively.【F:data/README.md†L153-L169】

**Verifier (Phase Scope)**
- Assume a reliable, relatively cheap verifier paired with a versioned rubric; richer information-theoretic signals (e.g., Simplicity Theory priors) are logged as future work rather than blocking cohort runs.【F:docs/rationale/next_steps.md†L7-L38】

**Fit to Goals**
- Enumerated constructive search plus fixed grammar demonstrates a practical existence path stronger than token-level enumeration while staying aligned with the simplicity-first mandate.【F:docs/rationale/project_goals.md†L22-L36】

## Previous Approach (Unordered Free-Association)

- Summary: Earlier runs explored unordered combinations of concepts and encouraged free associations to surface target ideas indirectly.
- Limitations:
  - Weak anchoring: No problem-first root; attachments often decorative rather than causal.
  - Degeneracy: Many semantically similar outputs with different phrasing, inflating search space without added mechanism.
  - Pruning difficulty: Lacked per-step micro-tests and adjacency deltas; harder to prune early and compare structures.
- Improvements in current approach:
  - Seed-first ordering (C1 as initial seed) with a single primary attachment per step reduces branching and enforces causality.
  - Minimal, explicit transformations and enforced link-type selection constrain drift and clarify mechanisms.
  - Per-step micro-tests (optional) and rolling summaries enable earlier pruning and reproducible comparisons.
