from dagster import IOManager, InputContext, OutputContext
from pathlib import Path
import pandas as pd

class PartitionedTextIOManager(IOManager):
    """
    Saves each partition as a separate text file.
    Preserves your existing file structure and debugging capabilities.
    """
    
    def __init__(self, base_path):
        self.base_path = Path(base_path)
    
    def handle_output(self, context: OutputContext, obj: str):
        """Save partition response as individual file"""
        partition_key = context.partition_key
        file_path = self.base_path / f"{partition_key}.txt"
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save response immediately (like your current system)
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
    Enhanced CSV I/O Manager with source mapping and filtering capabilities.
    Saves DataFrames as CSV files for easy inspection and debugging.
    Critical for understanding what combo_001, combo_002, etc. actually contain.
    """
    
    def __init__(self, base_path, source_mappings: dict = None):
        self.base_path = Path(base_path)
        self.source_mappings = source_mappings or {}
    
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
            # Handle tuple of DataFrames (like concept_combinations output)
            if isinstance(obj, tuple) and len(obj) == 2:
                df1, df2 = obj
                if hasattr(df1, 'to_csv') and hasattr(df2, 'to_csv'):
                    df1.to_csv(self.base_path / f"{asset_name}_combinations.csv", index=False)
                    df2.to_csv(self.base_path / f"{asset_name}_relationships.csv", index=False)
                    context.log.info(f"Saved {asset_name} tuple to {self.base_path}")
                else:
                    raise ValueError(f"Expected DataFrames, got {type(df1)}, {type(df2)}")
            else:
                raise ValueError(f"Unsupported object type for CSV saving: {type(obj)}")
    
    def load_input(self, context: InputContext):
        """Load DataFrame - enhanced with source mapping support"""
        asset_name = context.asset_key.path[-1]
        
        # Check if this asset has a source mapping (raw data)
        if asset_name in self.source_mappings:
            mapping = self.source_mappings[asset_name]
            source_file_path = mapping["source_file"]
            
            source_file = Path(source_file_path)
            
            if not source_file.exists():
                raise FileNotFoundError(f"Source file not found: {source_file}")
            
            df = pd.read_csv(source_file)
            
            # Apply filters if specified
            if "filters" in mapping:
                for filter_config in mapping["filters"]:
                    column = filter_config["column"]
                    value = filter_config["value"]
                    operator = filter_config.get("operator", "==")
                    
                    if column not in df.columns:
                        raise KeyError(f"Column '{column}' not found in DataFrame. Available columns: {list(df.columns)}")
                    
                    if operator == "==":
                        # Handle boolean filtering with type safety
                        if isinstance(value, bool):
                            # CSV loading converts booleans to strings, so we need to handle both cases
                            # Match only boolean True/False and their string representations, not integers
                            if value is True:
                                mask = df[column].map(lambda x: x is True or x == 'True')
                            else:  # value is False
                                mask = df[column].map(lambda x: x is False or x == 'False')
                            df = df[mask]
                        else:
                            df = df[df[column] == value]
                    elif operator == "!=":
                        df = df[df[column] != value]
                    elif operator == "isin":
                        df = df[df[column].isin(value)]  # value should be a list
                    else:
                        raise ValueError(f"Unsupported filter operator: {operator}. Supported operators: ==, !=, isin")
            
            return df
        
        # Fall back to normal CSV loading for processed assets
        # Check if it's a tuple asset first
        combinations_file = self.base_path / f"{asset_name}_combinations.csv"
        relationships_file = self.base_path / f"{asset_name}_relationships.csv"
        
        if combinations_file.exists() and relationships_file.exists():
            df1 = pd.read_csv(combinations_file)
            df2 = pd.read_csv(relationships_file)
            return (df1, df2)
        
        # Single DataFrame
        file_path = self.base_path / f"{asset_name}.csv"
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        return pd.read_csv(file_path)

# csv_io_manager factory function removed - use direct instantiation

# partitioned_text_io_manager and partitioned_concept_io_manager removed - use direct instantiation

# ErrorLogIOManager removed - it's identical to CSVIOManager
# Factory functions removed - use direct CSVIOManager instantiation


class TemplateIOManager(IOManager):
    """
    I/O Manager that loads templates based on CSV metadata - single-stage solution.
    Handles both CSV metadata filtering and template file loading in one operation.
    """
    
    def __init__(self, data_paths_config):
        self.output_path = data_paths_config.tasks_dir
        self.data_paths_config = data_paths_config
    
    def handle_output(self, context: OutputContext, obj):
        """Save templates as JSON for easy inspection and debugging."""
        # Skip saving empty outputs
        if obj is None or (isinstance(obj, dict) and not obj):
            context.log.info(f"Skipping output for {context.asset_key} as it is empty")
            return
            
        asset_name = context.asset_key.path[-1]
        output_path = self.output_path / f"{asset_name}.json"
        
        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save templates as JSON for inspection
        import json
        with open(output_path, 'w') as f:
            json.dump(obj, f, indent=2)
        
        context.log.info(f"Saved {len(obj)} templates to {output_path}")
    
    def load_input(self, context: InputContext) -> dict[str, str]:
        """Load templates based on CSV metadata - all in one stage."""
        templates = {}
        
        # Determine paths based on asset name
        asset_name = context.asset_key.path[-1]
        if asset_name == "generation_templates":
            templates_dir = self.data_paths_config.generation_templates_dir
            metadata_csv = self.data_paths_config.generation_templates_csv
        elif asset_name == "evaluation_templates":
            templates_dir = self.data_paths_config.evaluation_templates_dir
            metadata_csv = self.data_paths_config.evaluation_templates_csv
        else:
            context.log.error(f"Unknown template asset name: {asset_name}")
            return templates
        
        # Load and filter metadata
        if not metadata_csv.exists():
            context.log.warning(f"Template metadata CSV not found: {metadata_csv}")
            return templates
            
        metadata_df = pd.read_csv(metadata_csv)
        active_templates = metadata_df[metadata_df["active"] == True]
        
        # Load only active template files
        for _, row in active_templates.iterrows():
            template_id = row["template_id"]
            template_file = templates_dir / f"{template_id}.txt"
            
            if template_file.exists():
                templates[template_id] = template_file.read_text().strip()
                context.log.info(f"Loaded template: {template_id}")
            else:
                context.log.warning(f"Template file not found: {template_file}")
        
        context.log.info(f"Loaded {len(templates)} active templates from {templates_dir}")
        return templates