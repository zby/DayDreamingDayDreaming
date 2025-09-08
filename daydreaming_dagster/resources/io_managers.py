from dagster import IOManager, InputContext, OutputContext
from pathlib import Path
import pandas as pd
import os
import re
from typing import Optional

class PartitionedTextIOManager(IOManager):
    """
    Saves each partition as a separate text file.
    Preserves your existing file structure and debugging capabilities.

    overwrite: when False (default), refuse to overwrite an existing file. This acts as a
    guard against accidental loss of prior generations.
    """

    def __init__(self, base_path, overwrite: bool = False):
        self.base_path = Path(base_path)
        self.overwrite = overwrite
    
    def handle_output(self, context: OutputContext, obj: str):
        """Save partition response as individual file (atomic write, optional overwrite)."""
        partition_key = context.partition_key
        file_path = self.base_path / f"{partition_key}.txt"

        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Guard against accidental overwrite unless explicitly enabled
        if file_path.exists() and not self.overwrite:
            raise FileExistsError(
                f"Refusing to overwrite existing file: {file_path}. "
                "Delete the file to re-materialize, change the partition key, "
                "or configure the IO manager with overwrite=True."
            )

        # Atomic-ish write via temp then replace
        tmp = file_path.with_suffix(".txt.tmp")
        tmp.write_text(obj, encoding="utf-8")
        os.replace(tmp, file_path)
        context.log.info(f"Saved response to {file_path}")
    
    def load_input(self, context: InputContext) -> str:
        """Load partition response from file.

        Prefer a versioned file `{pk}_vN.txt` in the same directory if present; otherwise
        fall back to the unversioned `{pk}.txt`. This makes reads resilient when files
        have been versioned by other tools.
        """
        partition_key = context.partition_key
        # Prefer versioned if available
        try:
            names = os.listdir(self.base_path)
            best_ver = -1
            best_name = None
            prefix = f"{partition_key}_v"
            for name in names:
                if not name.startswith(prefix) or not name.endswith(".txt"):
                    continue
                m = re.match(rf"^(?P<stem>{re.escape(partition_key)})_v(?P<ver>\d+)\.txt$", name)
                if not m:
                    continue
                try:
                    ver = int(m.group("ver"))
                except Exception:
                    continue
                if ver > best_ver:
                    best_ver = ver
                    best_name = name
            if best_name is not None:
                p = self.base_path / best_name
                if p.exists():
                    return p.read_text(encoding="utf-8")
        except FileNotFoundError:
            pass

        # FALLBACK(OPS): tolerate unversioned file reads in dev; prefer versioned files in production.
        file_path = self.base_path / f"{partition_key}.txt"
        if not file_path.exists():
            raise FileNotFoundError(f"Response file not found: {file_path}")
        return file_path.read_text(encoding="utf-8")

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


# VersionedTextIOManager removed. For persistence, assets write to the docs store
# (data/docs/<stage>/<doc_id>) and scripts read from the filesystem directly.


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
