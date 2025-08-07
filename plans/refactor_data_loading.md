### Plan: Refactor Data Loading and Simplify Asset Definitions

**1. Goal:**
Streamline the data loading process by removing most `raw_data` assets and integrating their logic directly into the downstream assets that consume them. The `concepts` asset will be retained and refactored to be self-contained.

**2. Key Changes:**

*   **Refactor `concepts` Asset:**
    *   The `concepts` asset in `daydreaming_dagster/assets/raw_data.py` will be the sole asset in this file. No changes to its implementation are needed as it is already self-contained.

*   **Remove Other `raw_data` Assets:**
    *   The following assets will be deleted from `daydreaming_dagster/assets/raw_data.py`:
        *   `generation_models`
        *   `evaluation_models`
        *   `generation_templates`
        *   `evaluation_templates`

*   **Update Consumer Assets in `core.py`:**
    *   **`generation_tasks`:** This asset will be updated to directly load the generation models and templates, removing its dependencies on the now-deleted `raw_data` assets.
    *   **`evaluation_tasks`:** This asset will be updated to directly load the evaluation models and templates.

**3. Execution Steps:**

1.  **Modify `daydreaming_dagster/assets/raw_data.py`:**
    *   Remove the `generation_models`, `evaluation_models`, `generation_templates`, and `evaluation_templates` asset functions.

2.  **Modify `daydreaming_dagster/assets/core.py`:**
    *   Add `from pathlib import Path` to the imports.
    *   **In the `generation_tasks` asset:**
        *   Update the function signature to remove the `generation_models` and `generation_templates` parameters.
        *   Add `data_paths` to the `required_resource_keys` in the asset decorator.
        *   Inject the data loading logic at the beginning of the function.

    *   **In the `evaluation_tasks` asset:**
        *   Update the function signature to remove the `evaluation_models` and `evaluation_templates` parameters.
        *   Add `data_paths` to the `required_resource_keys` in the asset decorator.
        *   Inject the data loading logic at the beginning of the function.

4.  **Run `uv run ruff check --fix` and `uv run black .`** to ensure code quality and formatting.
5.  **Run `uv run pytest`** to verify that the refactoring has not broken any tests.