"""Shim module: re-export assets from groups/group_generation_draft.py.

This flattens imports to `daydreaming_dagster.assets.group_generation_draft`
without immediately moving files on disk.
"""

from .groups.group_generation_draft import *  # noqa: F401,F403

