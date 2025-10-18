# Cohort Configuration Cleanup TODO

## Context
We want to simplify the cohort configuration story by adopting a shared CUE schema and eliminating redundant spec parsing logic that has accumulated around the current YAML flow.

## Tasks
- Convert the cohort configuration flow to CUE, retire the bulk of the bespoke spec parsing/helpers, and wire the pipeline to the new representation.
- Use the migration to simplify oversized orchestration modules (e.g., cohort membership) by splitting responsibilities and dropping redundant glue code.
