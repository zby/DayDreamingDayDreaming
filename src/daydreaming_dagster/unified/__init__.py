"""Unified, stage-agnostic generation runner (scaffold).

Provides a single code path that can render a template, optionally call an LLM,
optionally parse a response, and persist artifacts in the gens store layout.

Assets can progressively adopt this runner without changing their external
interfaces or Dagster partitioning.
"""

