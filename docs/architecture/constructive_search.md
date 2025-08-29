## Constructive Search and Goal Alignment (Current Approach)

**Intent**
- Describe the current approach: a structured, finite search paired with fixed templates that can meet the existence goal without relying on token permutations.

**Search Space**
- Finite, enumerable selections from a target‑neutral concept pool up to `k_max` (order‑sensitive: C1 is an initial seed; C2..Ck attach in order).
- Canonical ordering: The realized order of concepts is inherited from the
  row order in `data/1_raw/concepts/concepts_metadata.csv` after applying the
  `active` filter. Templates receive concepts in exactly this order, ensuring
  deterministic rendering across runs.
- Fixed, parseable grammar via `rolling-summary-v1` (quick Q1→Q5 probing with exactly one enforced link type per step and a rolling “Idea So Far — i”) constrains outputs and reduces degeneracy.

**Why This Template Helps**
- Single‑link decision with enforced link types and stop‑at‑first‑yes keeps branching low and attachments causal.
- Rolling “Idea So Far” summaries maintain readability and provide a stable default anchor; minimal, faithful transformations are recorded explicitly.
- Optional micro‑tests enable early pruning; earliest‑leaf default and Q1 preference for Mechanism Composition promote causal strength.

**Existence Sketch**
- For fixed `k_max`, ordered sequences × bounded rendering choices (link types + short summaries) are finite; a bounded enumerator (or stochastic policy with exploration) covers this space in finite time.
- Evaluation gates detect reinvention when all current novel elements are present with coherence/justification.

**Practicality Levers**
- Adaptive propose‑verify (APV) to reduce proposals‑per‑accept; early pruning via micro‑tests; canonicalization to collapse duplicates.

**Metrics & Stopping**
- Efficiency (PPA, verified_novelty@K per cost), quality (rubric + gates), progress (coverage/diversity). Stop on diminishing returns or stable reinventions.

**Assumptions**
- Broad, target‑neutral concepts; maintained evaluator fidelity; non‑zero exploration; realistic budgets.

**Verifier (Phase Scope)**
- Assume a reliable, relatively cheap verifier (automated/lightly model‑assisted) with versioned rubric; design is deferred. Information‑theoretic signals (e.g., Simplicity Theory) are a future direction.

**Fit to Goals**
- Structured, finite search + fixed grammar provides a practical existence path stronger than token‑level enumeration.

## Previous Approach (Unordered Free‑Association)

- Summary: Earlier runs explored unordered combinations of concepts and encouraged free associations to surface target ideas indirectly.
- Limitations:
  - Weak anchoring: No problem‑first root; attachments often decorative rather than causal.
  - Degeneracy: Many semantically similar outputs with different phrasing, inflating search space without added mechanism.
  - Pruning difficulty: Lacked per‑step micro‑tests and adjacency deltas; harder to prune early and compare structures.
- Improvements in current approach:
  - Seed‑first ordering (C1 as initial seed) with a single primary attachment per step reduces branching and enforces causality.
  - Minimal, explicit transformations and enforced link‑type selection constrain drift and clarify mechanisms.
  - Per‑step micro‑tests (optional) and rolling summaries enable earlier pruning and reproducible comparisons.
