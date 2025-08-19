from dagster import IOManager, InputContext, OutputContext
from pathlib import Path
import pandas as pd
import json

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
        """Save partition response as individual file"""
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

        # Save response
        file_path.write_text(obj)
        context.log.info(f"Saved response to {file_path}")
    
    def load_input(self, context: InputContext) -> str:
        """Load partition response from file"""
        partition_key = context.partition_key
        file_path = self.base_path / f"{partition_key}.txt"
        
        if not file_path.exists():
            raise FileNotFoundError(f"Response file not found: {file_path}")
            
        return file_path.read_text()

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
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        return pd.read_csv(file_path)

# csv_io_manager factory function removed - use direct instantiation

# partitioned_text_io_manager and partitioned_concept_io_manager removed - use direct instantiation

# ErrorLogIOManager removed - it's identical to CSVIOManager
# Factory functions removed - use direct CSVIOManager instantiation


class PartitionedJSONIOManager(IOManager):
    """
    Saves each partition as a separate JSON file.
    Useful for complex data structures like dictionaries.
    """
    
    def __init__(self, base_path, overwrite: bool = True):
        self.base_path = Path(base_path)
        self.overwrite = overwrite
    
    def handle_output(self, context: OutputContext, obj: dict):
        """Save partition data as individual JSON file"""
        partition_key = context.partition_key
        file_path = self.base_path / f"{partition_key}.json"
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Guard against accidental overwrite unless explicitly enabled
        if file_path.exists() and not self.overwrite:
            raise FileExistsError(
                f"Refusing to overwrite existing file: {file_path}. "
                "Delete the file to re-materialize, change the partition key, "
                "or configure the IO manager with overwrite=True."
            )

        # Save data as JSON
        with open(file_path, 'w') as f:
            json.dump(obj, f, indent=2)
        context.log.info(f"Saved data to {file_path}")
    
    def load_input(self, context: InputContext) -> dict:
        """Load partition data from JSON file"""
        partition_key = context.partition_key
        file_path = self.base_path / f"{partition_key}.json"
        
        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")
            
        with open(file_path, 'r') as f:
            return json.load(f)


