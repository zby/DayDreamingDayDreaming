"""Centralized constants for stages and gens filenames.

Defines canonical stage names and per-generation file names used across
assets and utilities. Update here to change project-wide conventions.
"""

from .types import STAGES
from .config.paths import (
    PROMPT_FILENAME,
    RAW_FILENAME,
    PARSED_FILENAME,
    METADATA_FILENAME,
)

# Canonical files persisted under data/gens/<stage>/<gen_id>/
GEN_FILES = (PROMPT_FILENAME, RAW_FILENAME, PARSED_FILENAME, METADATA_FILENAME)

# Backcompat aliases for stage names (prefer using values from STAGES directly)
DRAFT, ESSAY, EVALUATION = STAGES

# DEPRECATED: Prefer using Paths methods (config.paths.Paths).
# These are kept for backcompat while code is migrated.
FILE_PROMPT, FILE_RAW, FILE_PARSED, FILE_METADATA = GEN_FILES
