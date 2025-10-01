from __future__ import annotations

"""Integration checks for Definitions path wiring via the stage registry."""


def test_definitions_respect_custom_data_root(definitions_with_paths):
    defs, paths = definitions_with_paths()

    draft_prompt_manager = defs.resources["draft_prompt_io_manager"]
    assert draft_prompt_manager.gens_root == paths.gens_root
    assert getattr(defs.resources["draft_response_io_manager"], "_fallback_root") == paths.data_root

    # Shared CSV managers should target the derived paths rather than literals
    assert defs.resources["csv_io_manager"].base_path == paths.tasks_dir
    assert defs.resources["error_log_io_manager"].base_path == paths.data_root / "7_reporting"
