"""Thin wrapper to import group_cross_experiment assets from the groups package.

This mirrors the pattern used by other top-level group_* modules so that
daydreaming_dagster.definitions can import a flat path for assets.
"""

from .groups.group_cross_experiment import *  # noqa: F401,F403

