# Data Checks Cleanup Plan

## Objective
Reduce redundancy across the `scripts/data_checks/` utilities while keeping each task discoverable via its own file. We will consolidate overlapping functionality, retire obsolete scripts, and align the remaining tools with current data flows.

## Changes

1. **Retire `find_missing_parents.py`**
   - Confirm `parent_chain_check.py` covers all checks (metadata presence, missing parent/grandparent) and update docs/help if needed.
   - Add a note in `parent_chain_check.py` CLI help mentioning that it supersedes the older script.
   - Remove `find_missing_parents.py` after verifying no automation references it.

2. **Merge Draft Template Checks**
   - Extend `templates_without_generations.py` with stage filters (e.g., `--stage draft`).
   - Ensure the script can output the exact listing `find_unused_draft_templates.py` produced.
   - Delete `find_unused_draft_templates.py` once parity is confirmed.

3. **Fold Evaluation Cleanup into `check_gens_store.py`**
   - Introduce a `--stage` selector and `--delete-missing` (with dry-run prompt) to the main checker.
   - Port the deletion logic and output formatting from `clean_incomplete_evaluations.py`.
   - Remove `clean_incomplete_evaluations.py`.

4. **Handle `list_missing_draft_templates.py`**
   - Verify the script’s docs-store dependency is obsolete.
   - Either remove it or rewrite it to rely on the gens store (similar to `templates_without_generations.py`). Document the decision.

5. **Document Remaining Scripts**
   - Add module-level docstrings where missing.
   - Create a brief README (`scripts/data_checks/README.md`) summarizing each script and the scenarios it covers.

## Validation
- Run each updated script against a test data root.
- Update any CI or instructions referencing the removed scripts.
- Capture results in changelog or docs if operators rely on them.

