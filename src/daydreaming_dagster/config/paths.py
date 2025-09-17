from __future__ import annotations

"""Deprecated shim: re-export path helpers from daydreaming_dagster.data_layer.paths."""

from daydreaming_dagster.data_layer.paths import (  # noqa: F401,F403
    Paths,
    PROMPT_FILENAME,
    RAW_FILENAME,
    PARSED_FILENAME,
    METADATA_FILENAME,
    RAW_METADATA_FILENAME,
    PARSED_METADATA_FILENAME,
)

__all__ = [
    "Paths",
    "PROMPT_FILENAME",
    "RAW_FILENAME",
    "PARSED_FILENAME",
    "METADATA_FILENAME",
    "RAW_METADATA_FILENAME",
    "PARSED_METADATA_FILENAME",
]
