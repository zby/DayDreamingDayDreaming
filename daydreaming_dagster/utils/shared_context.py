"""Shared context utilities for Dagster assets."""


class MockLoadContext:
    """Minimal context object for cross-partition IO manager calls.
    
    This pattern is documented in docs/evaluation_asset_architecture.md as an
    intentional way to load data from foreign key referenced partitions.
    
    The IO manager only needs the partition_key attribute to locate the correct file.
    """
    
    def __init__(self, partition_key: str):
        self.partition_key = partition_key