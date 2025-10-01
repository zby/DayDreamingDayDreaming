# Top 3 Essays from best_novelty_all_evals Cohort

This directory contains the top 3 essays by average evaluation score after fixing evaluation templates to properly reward mechanism understanding.

## Rankings

### #1: e_sq5klak2lljyyom (avg score: 8.77/9)

**File:** `e_sq5klak2lljyyom.txt`

**Generation Details:**
- Model: `gemini_25_pro`
- Draft template: `creative-synthesis-v10`
- Essay template: `creative-synthesis-v10`
- Draft ID: `d_5ezrffv9uf3lr37d`
- Combo ID: `combo_v1_85128e4e5ef3`
- Origin cohort: `best_novelty_all_evals`

**Input Concepts (5 total):**
1. Dearth of AI-driven Discoveries
2. Default Mode Network
3. Economic Innovation Models
4. Economic Moat
5. Generator-Verifier Gap

**Replication:**
To reproduce this essay from source materials in the repository:
1. **Concept definitions:** `data/1_raw/concepts/{concept-id}.txt` for each concept above
2. **Draft template:** `data/1_raw/templates/draft/creative-synthesis-v10.txt`
3. **Essay template:** `data/1_raw/templates/essay/creative-synthesis-v10.txt`
4. **Model:** `gemini_25_pro` (via OpenRouter or equivalent API)
5. **Process:** Apply draft template to concepts → generate with LLM → apply essay template to draft output → generate final essay

### #2: e_1cx6440bb5zj9466 (avg score: 8.75/9)

**File:** `e_1cx6440bb5zj9466.txt`

**Generation Details:**
- Model: `gemini_25_pro`
- Draft template: `creative-synthesis-v10`
- Essay template: `creative-synthesis-v10`
- Draft ID: `d_3cxfemmhegfzaguo`
- Combo ID: `combo_v1_f74195c24ea7`
- Origin cohort: `best_novelty_all_evals`

**Input Concepts (5 total):**
1. Dearth of AI-driven Discoveries
2. Default Mode Network
3. Economic Innovation Models
4. Economic Moat
5. Flywheel Effect

**Replication:**
To reproduce this essay from source materials in the repository:
1. **Concept definitions:** `data/1_raw/concepts/{concept-id}.txt` for each concept above
2. **Draft template:** `data/1_raw/templates/draft/creative-synthesis-v10.txt`
3. **Essay template:** `data/1_raw/templates/essay/creative-synthesis-v10.txt`
4. **Model:** `gemini_25_pro` (via OpenRouter or equivalent API)
5. **Process:** Apply draft template to concepts → generate with LLM → apply essay template to draft output → generate final essay

### #3: e_4nqjjtqtnpxlljlz (avg score: 8.52/9)

**File:** `e_4nqjjtqtnpxlljlz.txt`

**Generation Details:**
- Model: `deepseek_r1_p`
- Draft template: `creative-synthesis-v7`
- Essay template: `parsed-from-links-v1`
- Draft ID: `d_1z2eobtuv71xs5az`
- Combo ID: `combo_v1_13c725cf02fa`
- Origin cohort: `best_novelty_all_evals`

**Input Concepts:**
1. Dearth of AI-driven Discoveries
2. Default Mode Network
3. Generator-Verifier Gap
4. Economic Moat

**Replication:**
To reproduce this essay from source materials in the repository:
1. **Concept definitions:** `data/1_raw/concepts/{concept-id}.txt` for each concept above
2. **Draft template:** `data/1_raw/templates/draft/creative-synthesis-v7.txt`
3. **Essay template:** `data/1_raw/templates/essay/parsed-from-links-v1.txt`
4. **Model:** `deepseek_r1_p` (DeepSeek R1 via API)
5. **Process:** Apply draft template to concepts → generate with LLM → apply essay template to draft output → generate final essay

## Evaluation Methodology

Essays were evaluated using 11 active templates:
- `creativity-metrics-v2`
- `daydreaming-verification-v3` (fixed to not penalize terminology)
- `gemini-prior-art-eval-v2`
- `interdisciplinary-coherence`
- `iterative-loops-v2`
- `novel-elements-coverage-v2` (fixed to not penalize terminology)
- `novelty`
- `novelty-v2`
- `o3-prior-art-eval`
- `scientific-rigor-v2`
- `style-coherence-v3`

Each essay received 44 evaluations (11 templates × 2 models × 2 replicates).

Evaluation models: `gemini_25_pro`, `sonnet-4`

## Key Insight

The evaluation template fixes (v2→v3 for `daydreaming-verification`, updated `novel-elements-coverage-v2`) changed the philosophy from penalizing outputs that use "Daydreaming LLMs" terminology to treating it as a **POSITIVE signal** of independent reinvention when accompanied by correct mechanism description.

This resulted in significant score improvements:
- Overall average: 7.95 → 8.29 (+0.34)
- #1 essay: 8.34 → 8.77 (+0.43)
- #2 essay: 8.33 → 8.75 (+0.42)

## Source Materials (in repository)

All inputs needed for replication are in `data/1_raw/`:
- **Concept definitions:** `data/1_raw/concepts/descriptions-{level}/{concept-id}.txt`
  - Concepts are organized by description level (sentence, paragraph, essay)
  - Used concepts: dearth-ai-discoveries, default-mode-network, economic-innovation-models, economic-moat, flywheel-effect, generator-verifier-gap
- **Draft templates:** `data/1_raw/templates/draft/{template-id}.txt`
- **Essay templates:** `data/1_raw/templates/essay/{template-id}.txt`
- **Evaluation templates:** `data/1_raw/templates/evaluation/{template-id}.txt`
- **Concept metadata:** `data/1_raw/concepts_metadata.csv`
- **Template configs:** `data/1_raw/{draft,essay,evaluation}_templates.csv`
- **Model configs:** `data/1_raw/llm_models.csv`

**Two-stage generation process:**
1. **Draft stage:** Concepts → Draft template → LLM → Structured concept links
2. **Essay stage:** Draft output → Essay template → LLM → Final essay

Generated: 2025-01-15
