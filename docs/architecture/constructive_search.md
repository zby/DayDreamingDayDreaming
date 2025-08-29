## Constructive Search and Goal Alignment (Current Approach)

**Intent**
- Describe the current approach: a structured, finite search paired with fixed templates that can meet the existence goal without relying on token permutations.

**Search Space**
- Finite, enumerable selections from a target‑neutral concept pool up to `k_max` (order‑sensitive: C1 = problem; C2..Ck attach in order).
- Fixed, parseable grammar via `recursive_construction` (one primary attachment per step) constrains outputs and reduces degeneracy.

**Why This Template Helps**
- Problem‑first anchoring and single‑link decision (Types 1–5) per step keep branching low and attachments causal.
- Transformation and Node‑Introduction gates canonicalize concepts and add `E::…` only when a true new capability appears.
- Local micro‑tests + adjacency deltas enable early pruning and reproducible structure.

**Existence Sketch**
- For fixed `k_max`, ordered sequences × bounded rendering choices are finite; a bounded enumerator (or stochastic policy with exploration) covers this space in finite time.
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
  - Problem‑first ordering (C1 as root) with a single primary attachment per step reduces branching and enforces causality.
  - Transformation/Node gates canonicalize concepts and only add `E::…` when genuine new capability appears.
  - Per‑step micro‑tests and explicit topology deltas enable earlier pruning and reproducible comparisons.
