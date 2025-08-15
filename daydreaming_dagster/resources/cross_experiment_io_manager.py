"""I/O manager for cross-experiment analysis results."""

from dagster import ConfigurableIOManager, MetadataValue
from pathlib import Path
import pandas as pd


class CrossExperimentIOManager(ConfigurableIOManager):
    """I/O manager for cross-experiment analysis results.
    
    Stores results in CSV format in the data/7_cross_experiment/ directory.
    Each asset gets its own CSV file named after the asset.
    """
    
    base_path: str = "data/7_cross_experiment"
    
    def handle_output(self, context, obj):
        """Handle output from assets - save to CSV files."""
        if isinstance(obj, pd.DataFrame):
            # Use the asset name from the context
            asset_name = context.asset_key.path[-1] if context.asset_key else context.name
            output_path = Path(self.base_path) / f"{asset_name}.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            obj.to_csv(output_path, index=False)
            
            context.add_output_metadata({
                "output_path": MetadataValue.path(output_path),
                "row_count": MetadataValue.int(len(obj)),
                "column_count": MetadataValue.int(len(obj.columns)),
                "file_size_mb": MetadataValue.float(output_path.stat().st_size / (1024 * 1024))
            })
        else:
            # For non-DataFrame objects, just log what we got
            context.add_output_metadata({
                "object_type": MetadataValue.text(type(obj).__name__),
                "object_repr": MetadataValue.text(str(obj)[:100] + "..." if len(str(obj)) > 100 else str(obj))
            })
    
    def load_input(self, context):
        """Load input for assets - read from CSV files."""
        # Use the asset name from the context
        asset_name = context.asset_key.path[-1] if context.asset_key else context.name
        input_path = Path(self.base_path) / f"{asset_name}.csv"
        if input_path.exists():
            try:
                df = pd.read_csv(input_path)
                context.log.info(f"Loaded {len(df)} rows from {input_path}")
                return df
            except Exception as e:
                context.log.error(f"Error loading {input_path}: {e}")
                return pd.DataFrame()
        else:
            context.log.warning(f"Input file not found: {input_path}")
            return pd.DataFrame()
