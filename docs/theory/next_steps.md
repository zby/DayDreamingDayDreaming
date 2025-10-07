# Next Steps: Building the Real Thing

## Where We Stand

We currently have a Gwern-specific reinvention scaffold that can reliably restate a known essay. That is not a discovery engine yet—it is intentionally narrow, brittle, and only judges whether an output matches the original Gwern ideas. Still, reproducing an already-made discovery gives us concrete scaffolding: we now have deterministic generators, evaluation templates, and data plumbing that can anchor the next experiment instead of starting from a blank page.【F:docs/theory/project_goals.md†L13-L94】【F:data/results/RESULTS.md†L11-L116】

## What the Current Stack Already Gives Us

- Reproducible combinatorial search over a versioned concept pool rendered through deterministic templates and Dagster assets. This keeps the search space finite and auditable, and we can enumerate concept combinations up to `k_max` without ad-hoc prompt drift.【F:docs/theory/project_goals.md†L13-L58】【F:docs/theory/constructive_search.md†L6-L33】
- A two-stage draft→essay generator with strong tracking of prompt, concept, and model lineage, plus bulk evaluation infrastructure (44 rubric-aligned scores per essay) that we can repurpose as programmable gates for candidate filtering.【F:data/results/RESULTS.md†L11-L70】
- Versioned originality rubrics that already separate template inputs from evaluator judgments, which is the right shape for plugging in new critics or human review layers without retraining the generator.【F:docs/theory/project_goals.md†L59-L94】

## Gaps We Must Close for "Interesting" Ideas

1. **Interestingness heuristics and open questions** – novelty is only meaningful when tied to a felt need. Seed the verifier with a backlog of open problems or research prompts so we can score candidates on how directly they address those gaps, and experiment with Simplicity Theory as a lightweight interestingness prior (rewarding concise explanations that compress observations) using the existing evaluation template machinery as the delivery vehicle.【F:data/results/RESULTS.md†L45-L70】【F:docs/theory/constructive_search.md†L6-L33】
2. **Prior-art boundaries beyond Gwern** – even while we aim for truly new ideas, we must still detect collisions with prior work. Grow the verifier’s context using reference corpora, embeddings, or citation graphs so it understands what is already known outside the Gwern benchmark, and couple that with the open-question backlog to highlight when a candidate meaningfully advances the frontier.【F:docs/theory/project_goals.md†L1-L38】【F:data/results/RESULTS.md†L71-L116】
3. **Learning verifier policies** – the generator already logs rich structured artifacts. Use them to train critics that learn from accepted/rejected ideas (contrastive reward models, preference-ranking over essays) so the verifier evolves with evidence instead of frozen rubric scores.
4. **Expanding and refreshing the concept frontier** – today’s concept pool is not just static but tightly bounded. To avoid rediscovering the same seams, hook the pipeline to ingestion assets that mine fresh concepts from reading, normalize them through the existing schema, and version snapshots so we can deliberately widen coverage without losing traceability.【F:docs/theory/project_goals.md†L23-L56】【F:data/results/RESULTS.md†L71-L116】

## Blueprint: Proposal → Critique → Advancement

1. **Proposal layer (reuse)**
   - Keep the existing draft/essay assets but expose concept selection policies as interchangeable modules (enumeration, novelty-guided sampling, active learning). Maintain deterministic rendering for auditability.

2. **Critique layer (new focus)**
   - Instantiate multiple verifier tracks:
     - *Redundancy check*: embedding similarity vs. a growing archive of accepted ideas.
     - *Prior-art sweep*: retrieval augmented evaluation prompts that cite nearest known works and ask LLMs (or symbolic tools) if the candidate adds a real delta.
     - *Value screens*: prompts or lightweight simulators that test for falsifiable predictions, economic upside, or experimental pathways.
   - Aggregate these tracks into a scored dossier (structured JSON) stored next to each essay so future reviewers or learning algorithms can inspect evidence rather than raw text blobs.

3. **Advancement layer (closed loop)**
   - When a proposal clears novelty + value thresholds, trigger follow-on assets:
     - Generate experiment designs or metrics the idea would move.
     - Schedule literature expansion to update the reference corpus in that locale.
     - Feed accepted ideas back into the concept pool (with provenance tags) so subsequent runs can build on them instead of rediscovering them.
   - Log decisions (accept, defer, reject) to a supervision table that can train future verifier policies.

The Gwern reinvention experiment remains limited, but by treating it as a scaffold—proof that we can replicate one discovery end-to-end—we gain the backbone needed to iterate toward a general novelty system that can surface, judge, and advance genuinely new ideas.
