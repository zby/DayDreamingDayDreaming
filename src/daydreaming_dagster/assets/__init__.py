"""Assets package init.

This file intentionally avoids re-exporting all asset symbols to reduce
import side-effects and keep import graphs explicit. Import assets from
their concrete modules instead, for example:

    from daydreaming_dagster.assets.results_summary import final_results

    Public entrypoints remain stable via the daydreaming_dagster.definitions module.
"""
