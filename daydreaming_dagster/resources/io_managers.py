from dagster import IOManager, InputContext, OutputContext
from pathlib import Path
import pandas as pd
import os

# Factory functions removed - use direct class instantiation in definitions.py

class CSVIOManager(IOManager):
    """
    Generic CSV I/O Manager for loading and saving pandas DataFrames.
    Saves DataFrames as CSV files for easy inspection and debugging.
    """
    
    def __init__(self, base_path):
        self.base_path = Path(base_path)
    
    def handle_output(self, context: OutputContext, obj):
        """Save DataFrame as CSV file"""
        # Skip saving empty outputs
        if obj is None or (hasattr(obj, 'empty') and obj.empty):
            context.log.info(f"Skipping output for {context.asset_key} as it is empty")
            return
        # TEMPORARY: allow disabling task CSV writes to advance task-layer removal
        # Only applies when this manager writes under data/2_tasks and the flag is set.
        try:
            if os.environ.get('DD_DISABLE_TASK_CSV_WRITES') == '1':
                base_last = str(self.base_path).rstrip('/').split('/')[-1]
                if base_last == '2_tasks':
                    context.log.warning(
                        "DD_DISABLE_TASK_CSV_WRITES=1: skipping CSV write for %s",
                        "/".join(context.asset_key.path),
                    )
                    return
        except Exception:
            pass
            
        asset_name = context.asset_key.path[-1]
        file_path = self.base_path / f"{asset_name}.csv"
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save DataFrame as CSV
        if hasattr(obj, 'to_csv'):  # pandas DataFrame
            obj.to_csv(file_path, index=False)
            context.log.info(f"Saved {asset_name} to {file_path}")
        else:
            raise ValueError(f"Expected pandas DataFrame for CSV saving, got {type(obj)}")
    
    def load_input(self, context: InputContext):
        """Load DataFrame from CSV file"""
        asset_name = context.asset_key.path[-1]
        file_path = self.base_path / f"{asset_name}.csv"
        
        if not file_path.exists():
            # Small retry to handle rare FS latency between producer/consumer steps
            try:
                import time
                for _ in range(5):
                    time.sleep(0.05)
                    if file_path.exists():
                        break
            except Exception:
                pass
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        return pd.read_csv(file_path)

# csv_io_manager factory function removed - use direct instantiation

# partitioned_text_io_manager and partitioned_concept_io_manager removed - use direct instantiation

# ErrorLogIOManager removed - it's identical to CSVIOManager
# Factory functions removed - use direct CSVIOManager instantiation


# VersionedTextIOManager removed. For persistence, assets write to the gens store
# (data/gens/<stage>/<gen_id>) and scripts read from the filesystem directly.


class InMemoryIOManager(IOManager):
    """
    Simple in-memory IO manager for tests/ephemeral data passing.
    Stores objects per-asset (and partition if present) within a single process/run.
    """

    def __init__(self):
        self._store = {}

    def handle_output(self, context: OutputContext, obj):
        key = (tuple(context.asset_key.path), context.partition_key)
        self._store[key] = obj

    def load_input(self, context: InputContext):
        upstream = context.upstream_output
        key = (tuple(upstream.asset_key.path), upstream.partition_key)
        if key not in self._store:
            raise KeyError(f"InMemoryIOManager: no object stored for {key}")
        return self._store[key]
