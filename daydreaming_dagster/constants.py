"""Centralized constants for stages and gens filenames.

Defines canonical stage names and per-generation file names used across
assets and utilities. Update here to change project-wide conventions.
"""

from .types import STAGES

# Canonical files persisted under data/gens/<stage>/<gen_id>/
GEN_FILES = ("prompt.txt", "raw.txt", "parsed.txt", "metadata.json")

# Backcompat aliases for stage names (prefer using values from STAGES directly)
DRAFT, ESSAY, EVALUATION = STAGES
FILE_PROMPT, FILE_RAW, FILE_PARSED, FILE_METADATA = GEN_FILES
