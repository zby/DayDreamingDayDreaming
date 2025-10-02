from __future__ import annotations

"""Integration checks for Definitions path wiring via the stage registry."""

from daydreaming_dagster.resources.io_managers import CohortCSVIOManager


def test_definitions_respect_custom_data_root(definitions_with_paths):
    defs, paths = definitions_with_paths()

    draft_prompt_manager = defs.resources["draft_prompt_io_manager"]
    assert draft_prompt_manager.gens_root == paths.gens_root
    assert getattr(defs.resources["draft_response_io_manager"], "_fallback_root") == paths.data_root

    # Shared CSV managers should target the derived paths rather than literals
    assert defs.resources["csv_io_manager"].base_path == paths.tasks_dir
    assert defs.resources["error_log_io_manager"].base_path == paths.data_root / "7_reporting"

    parsing_mgr = defs.resources["parsing_results_io_manager"]
    summary_mgr = defs.resources["summary_results_io_manager"]
    for mgr, category in ((parsing_mgr, "parsing"), (summary_mgr, "summary")):
        assert isinstance(mgr, CohortCSVIOManager)
        assert mgr._paths == paths
        assert mgr._default_category == category
