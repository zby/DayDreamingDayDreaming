"""Smoke test to ensure the Dagster entrypoint imports.

This catches import-time errors (e.g., API drift) that normal tests may miss
if they import assets directly instead of going through `definitions.py`.
"""

from importlib import import_module
from dagster import Definitions


def test_import_definitions_smoke():
    mod = import_module("daydreaming_dagster.definitions")
    assert hasattr(mod, "defs"), "definitions module should expose `defs`"
    assert isinstance(mod.defs, Definitions), "`defs` should be a Dagster Definitions object"

