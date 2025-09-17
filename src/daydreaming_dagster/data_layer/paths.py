"""Data-layer specific Paths shim.

Wraps the legacy config Paths while providing stage-input friendly helpers.
"""

from daydreaming_dagster.config.paths import (  # noqa: F401 re-export
    Paths as _ConfigPaths,
    PROMPT_FILENAME,
    RAW_FILENAME,
    PARSED_FILENAME,
    METADATA_FILENAME,
    RAW_METADATA_FILENAME,
    PARSED_METADATA_FILENAME,
)

INPUT_FILENAME = PROMPT_FILENAME


class Paths(_ConfigPaths):
    """Extends legacy Paths with stage-input aliases."""

    def input_path(self, stage: str, gen_id: str):
        return super().prompt_path(stage, gen_id)


__all__ = [
    "Paths",
    "INPUT_FILENAME",
    "PROMPT_FILENAME",
    "RAW_FILENAME",
    "PARSED_FILENAME",
    "METADATA_FILENAME",
    "RAW_METADATA_FILENAME",
    "PARSED_METADATA_FILENAME",
]
