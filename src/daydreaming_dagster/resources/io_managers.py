from dagster import IOManager, InputContext, OutputContext
from pathlib import Path
from typing import Mapping
import pandas as pd
import os

from ..utils.errors import DDError, Err
from ..data_layer.paths import Paths
from ..data_layer.gens_data_layer import GensDataLayer

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
        # No special-casing for legacy task CSV locations; writes are always enabled.
            
        asset_name = context.asset_key.path[-1]
        file_path = self.base_path / f"{asset_name}.csv"
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save DataFrame as CSV
        if hasattr(obj, 'to_csv'):  # pandas DataFrame
            obj.to_csv(file_path, index=False)
            context.log.info(f"Saved {asset_name} to {file_path}")
        else:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "csv_io_requires_dataframe",
                    "type": str(type(obj)),
                    "asset": asset_name,
                },
            )
    
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
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "csv_file_missing",
                    "path": str(file_path),
                    "asset": asset_name,
                },
            )
        
        return pd.read_csv(file_path)

# csv_io_manager factory function removed - use direct instantiation

# partitioned_text_io_manager and partitioned_concept_io_manager removed - use direct instantiation

# ErrorLogIOManager removed - it's identical to CSVIOManager
# Factory functions removed - use direct CSVIOManager instantiation


# VersionedTextIOManager removed. For persistence, assets write to the gens store
# (data/gens/<stage>/<gen_id>) and scripts read from the filesystem directly.


class CohortCSVIOManager(IOManager):
    """Cohort-scoped CSV manager that routes writes based on the partition key."""

    def __init__(
        self,
        paths: Paths,
        *,
        default_category: str,
        asset_map: Mapping[str, tuple[str, str]] | None = None,
    ) -> None:
        self._paths = paths
        self._default_category = default_category
        self._asset_map = {
            key: (value[0], value[1])
            for key, value in (asset_map or {}).items()
        }

    def _resolve_target(self, asset_name: str) -> tuple[str, str]:
        category, filename = self._asset_map.get(
            asset_name,
            (self._default_category, f"{asset_name}.csv"),
        )
        return category, filename

    def _require_partition_key(self, context) -> str:
        partition_key = None
        if getattr(context, "has_partition_key", False):
            partition_key = context.partition_key
        if not partition_key and hasattr(context, "asset_partition_key"):
            partition_key = context.asset_partition_key
        if not partition_key:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "cohort_partition_required",
                    "asset": ".".join(context.asset_key.path) if context.asset_key else None,
                },
            )
        return str(partition_key)

    def handle_output(self, context: OutputContext, obj):
        if obj is None or (hasattr(obj, "empty") and getattr(obj, "empty")):
            context.log.info(f"Skipping output for {context.asset_key} as it is empty")
            return

        cohort_id = self._require_partition_key(context)
        asset_name = context.asset_key.path[-1] if context.asset_key.path else "asset"
        category, filename = self._resolve_target(asset_name)
        file_path = self._paths.cohort_report_path(cohort_id, category, filename)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if hasattr(obj, "to_csv"):
            obj.to_csv(file_path, index=False)
            context.log.info(f"Saved {asset_name} to {file_path}")
        else:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "csv_io_requires_dataframe",
                    "type": str(type(obj)),
                    "asset": asset_name,
                },
            )

    def load_input(self, context: InputContext):
        cohort_id = self._require_partition_key(context)
        asset_name = context.asset_key.path[-1] if context.asset_key.path else "asset"
        category, filename = self._resolve_target(asset_name)
        file_path = self._paths.cohort_report_path(cohort_id, category, filename)

        if not file_path.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "csv_file_missing",
                    "path": str(file_path),
                    "asset": asset_name,
                    "cohort_id": cohort_id,
                },
            )

        return pd.read_csv(file_path)


class InMemoryIOManager(IOManager):
    """Simple in-memory IO manager for tests/ephemeral data passing."""

    def __init__(self):
        self._store = {}

    def handle_output(self, context: OutputContext, obj):
        partition_key = context.partition_key if context.has_partition_key else None
        key = (tuple(context.asset_key.path), partition_key)
        self._store[key] = obj

    def load_input(self, context: InputContext):
        # Avoid deprecated upstream_output.partition_key access — use InputContext.partition_key
        upstream = context.upstream_output
        partition_key = context.partition_key if context.has_partition_key else None
        stage_asset = upstream.asset_key.path[-1] if upstream.asset_key.path else ""
        key = (tuple(upstream.asset_key.path), partition_key)
        if key not in self._store:
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "reason": "in_memory_missing",
                    "asset": stage_asset,
                    "partition": partition_key,
                },
            )

        return self._store[key]


class RehydratingIOManager(InMemoryIOManager):
    """In-memory IO manager that can rehydrate raw artifacts from the gens store."""

    def __init__(self, data_root: Path | str):
        super().__init__()
        self._layer = GensDataLayer.from_root(data_root)

    def load_input(self, context: InputContext):
        try:
            return super().load_input(context)
        except DDError as err:
            if err.code is not Err.DATA_MISSING:
                raise

            upstream = context.upstream_output
            partition_key = context.partition_key if context.has_partition_key else None
            stage_asset = upstream.asset_key.path[-1] if upstream.asset_key.path else ""
            if not (partition_key and stage_asset.endswith("_raw")):
                raise

            stage = stage_asset[:-4]  # drop "_raw"
            raw_text = self._layer.read_raw(stage, partition_key)

            key = (tuple(upstream.asset_key.path), partition_key)
            self._store[key] = raw_text
            return raw_text
