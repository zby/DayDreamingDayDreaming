"""
Group: generation_essays

Re-exports for discoverability; asset code lives in:
- daydreaming_dagster/assets/two_phase_generation.py
"""

GROUP = "generation_essays"

from ..two_phase_generation import (
    essay_prompt,
    essay_response,
)

