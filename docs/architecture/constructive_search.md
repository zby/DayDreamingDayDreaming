## Constructive Search and Goal Alignment

**Purpose**
- Show existence of a practical search procedure which, paired with our generation templates, discovers all novel insights contained in the DayDreaming AI essay within finite time.
- Contrast with the trivial but impractical “infinite monkeys” existence proof (token permutations) and articulate why our approach is both finite and tractable enough to be meaningful.

**Trivial vs. Practical Existence**
- Trivial existence: Enumerating all token strings of bounded length will eventually reproduce the essay verbatim. This is uninformative because the space is astronomically large and provides no mechanism.
- Practical existence: Construct a search over structured concept selections and a constrained generation grammar that is:
  - Finite and enumerable.
  - Guided by mechanism‑aware heuristics and local verifiers (micro‑tests).
  - Guaranteed to reach any target configuration with non‑zero probability under mild exploration (or deterministically under bounded enumeration).

**Search Space (What We Explore)**
- Universal concept pool: large but finite set of defensible, general concepts.
- Selections: choose up to `k_max` concepts. The constructive template is order‑sensitive (C1 is the problem; C2..Ck attach in order), so worst‑case space is combinations × permutations: sum_{k≤k_max} C(n,k) · k!.
- Structured outputs: The `recursive_construction` links template enforces a parseable schema and a single primary attachment per step, preventing combinatorial explosion in free‑form text.

**Why `recursive_construction` Helps**
- Problem‑first anchoring: C1 is normalized into a problem statement; attachments are causal, not decorative.
- Transformation Gate: Each concept is lightly operationalized (mechanism, constraint, objective, interface, property, data/memory). This canonicalization reduces degeneracy (same idea in many phrasings).
- Decision Tree (Types 1–5): Forces a single causal relation type per step, constraining degrees of freedom.
- Node‑Introduction Gate: New capabilities become `E::…` nodes only when Q1=Yes and they add unique state/operators. This curbs gratuitous mechanism sprawl.
- Micro‑tests and adjacency deltas: Per‑step local tests prune weak branches early; adjacency logs ensure deterministic, comparable structure.

**Existence Sketch (Informal)**
- Finite enumeration: For fixed `k_max`, the set of ordered concept sequences is finite. Rendering via a finite‑state template with bounded choices yields a finite set of candidate graphs.
- Exploration completeness:
  - Stochastic policy: An epsilon‑greedy or random‑restart policy on the propose‑verify loop (APV) yields non‑zero probability of sampling any ordered sequence and attachment choices within the bounded schema.
  - Deterministic fallback: A bounded enumerator can iterate all ordered concept selections up to `k_max` and apply the schema, ensuring coverage in finite time.
- Verification: Our evaluation gates require the presence of all current novel elements. Therefore, if any reachable graph/essay expresses them, the pipeline will detect it.
- Conclusion: At least one sequence + rendering achieves reinvention; search discovers it given sufficient (but finite) budget. This is strictly stronger than token‑permutation existence because the grammar and gates encode mechanism.

**Practicality Levers (Why Not Infinite Monkeys)**
- Adaptive Propose‑Verify (APV): Operationalization of the Generator–Verifier Gap — feedback‑coupled propose→verify→allocate loop to reduce proposals‑per‑accept at fixed novelty.
- Bandits and priors: Allocate budget to anchors/types that historically improve verified_novelty@K; keep epsilon exploration for completeness.
- Early pruning: Per‑step micro‑tests and gate checks discard low‑promise attachments before long essays are generated.
- Canonicalization: Transformation Gate collapses semantically equivalent phrasings into roleful forms (mechanism, constraint, objective…), shrinking the search manifold.
- Structural bias: The decision tree limits link types; Node‑Introduction requires unique state/operators to add `E::…`, curbing graph bloat.
- Idle scheduling: DMN‑style idle windows execute short APV cycles to accumulate hits without blocking foreground tasks.

**Tradeoffs: Permutations vs. Combinations**
- Order sensitivity expands space from combinations to permutations. However:
  - Only one primary attachment per step is produced; branching factor is kept low.
  - Anchor heuristics focus attachments where causal effect is strongest (mechanisms first, then constraints/properties), reducing wasted permutations.
  - Micro‑tests and evaluation feedback steer the policy, so most sequences are never exhaustively elaborated.

**Metrics and Stopping Rules**
- Efficiency: proposals‑per‑accept (PPA); verified_novelty@K per unit cost/time.
- Quality: rubric score; “all novel elements present” binary gate; inter‑evaluator agreement.
- Progress: coverage of ordered sequences tried; diversity of link types/roles; number of unique `E::…` mechanisms.
- Stop when: marginal improvement in verified_novelty@K per cost falls below threshold or gate‑satisfying reinventions are found with stability.

**Acceleration Roadmap**
- Learned `E::NoveltyEstimator` to pre‑screen candidates (reduce verify calls).
- Anchor/ordering policy learning from historical successes (contextual bandits or small RL loops).
- Beam/MCTS over attachment options with micro‑test‑informed rollouts.
- Cross‑experiment caching of `E::…` nodes and reusable subgraphs; deduplicate via invariants.
- Curriculum: start with smaller k, transfer priors to larger k.
- Concept metadata filters to restrict anchors/types (domain, roles, constraints).

**Assumptions and Risks**
- Concept pool quality: Must be broad and target‑neutral; otherwise we bias toward the benchmark.
- Evaluator fidelity: Gates must robustly detect novel elements and avoid false positives.
- Non‑zero exploration: Policies must retain exploration to preserve completeness guarantees.
- Budget realism: While finite, worst‑case enumeration is large; acceleration levers are key to practicality.

**Fit to Project Goals**
- Meets existence goal: Finite, structured search over ordered concept selections plus a constrained template grammar; formal gates detect reinvention.
- Demonstrates practicality: Mechanism‑aware constraints + APV reduce the gap between trivial existence and feasible discovery; future acceleration can further narrow this gap.
