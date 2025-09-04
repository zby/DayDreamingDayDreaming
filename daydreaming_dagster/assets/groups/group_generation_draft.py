"""
Group: generation_draft

Re-exports for discoverability; asset code lives in:
- daydreaming_dagster/assets/two_phase_generation.py
"""

GROUP = "generation_draft"

from ..two_phase_generation import (
    draft_prompt,
    draft_response,
)

