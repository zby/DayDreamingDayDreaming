## Current Search Strategy: k_max-only Combinations

**Active Strategy**: The experiment currently uses a **focused search strategy** that tests only k_max-sized concept combinations (e.g., if k_max=4, only 4-concept combinations are tested).

**Strategy Rationale**: This approach is based on the insight that richer contextual combinations are more likely to elicit the complex DayDreaming concept from LLMs.

**Strategy Benefits:**
- **Higher success probability**: More concepts provide richer semantic context
- **More efficient**: Avoids testing smaller, less informative combinations  
- **Focused discovery**: Concentrates computational resources on the most promising combinations

**Example**: With 6 concepts and k_max=4:
- **Current strategy**: Tests C(6,4) = 15 combinations
- **Alternative strategies**: Could test C(6,1) + C(6,2) + C(6,3) + C(6,4) = 6+15+20+15 = 56 combinations

**Status**: Default strategy as of 2025-07-29