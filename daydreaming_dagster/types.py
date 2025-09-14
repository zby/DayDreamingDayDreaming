from typing import Literal, Tuple, get_args, cast

# Shared stage literal across the codebase
Stage = Literal["draft", "essay", "evaluation"]

# Canonical stage sequence (single source of truth for runtime lists),
# derived from the Literal at import time to avoid drift.
STAGES: Tuple[Stage, ...] = cast(Tuple[Stage, ...], get_args(Stage))

__all__ = ["Stage", "STAGES"]
