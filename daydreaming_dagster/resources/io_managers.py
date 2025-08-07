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