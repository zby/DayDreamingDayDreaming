from dagster import IOManager, InputContext, OutputContext
from pathlib import Path
import pandas as pd

class PartitionedTextIOManager(IOManager):
    """
    Saves each partition as a separate text file.
    Preserves your existing file structure and debugging capabilities.
    """
    
    def __init__(self, base_path: str):
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

# Factory functions for different file types
def generation_prompt_io_manager():
    return PartitionedTextIOManager("data/03_generation/generation_prompts")

def generation_response_io_manager():
    return PartitionedTextIOManager("data/03_generation/generation_responses")

def evaluation_prompt_io_manager():
    return PartitionedTextIOManager("data/04_evaluation/evaluation_prompts")

def evaluation_response_io_manager():
    return PartitionedTextIOManager("data/04_evaluation/evaluation_responses")

class CSVIOManager(IOManager):
    """
    Saves DataFrames as CSV files for easy inspection and debugging.
    Critical for understanding what combo_001, combo_002, etc. actually contain.
    """
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
    
    def handle_output(self, context: OutputContext, obj):
        """Save DataFrame as CSV file"""
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
        """Load DataFrame from CSV file"""
        asset_name = context.asset_key.path[-1]
        
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

def csv_io_manager():
    return CSVIOManager("data/02_tasks")

class PartitionedConceptIOManager(IOManager):
    """
    Saves concept contents as individual text files, matching Kedro's partitioned structure.
    """
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
    
    def handle_output(self, context: OutputContext, obj: dict[str, str]):
        """Save each concept as individual file"""
        # Ensure directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Save each concept as a separate file
        for concept_id, content in obj.items():
            file_path = self.base_path / concept_id
            file_path.write_text(content)
            context.log.info(f"Saved concept {concept_id} to {file_path}")
    
    def load_input(self, context: InputContext) -> dict[str, str]:
        """Load all concept files"""
        concept_contents = {}
        for file_path in self.base_path.iterdir():
            if file_path.is_file():
                concept_id = file_path.name
                concept_contents[concept_id] = file_path.read_text()
        return concept_contents

def partitioned_text_io_manager():
    return PartitionedConceptIOManager("data/02_tasks/concept_contents")

class ErrorLogIOManager(IOManager):
    """
    Saves error logs and failure tracking as CSV files.
    """
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
    
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Save error log as CSV"""
        asset_name = context.asset_key.path[-1]
        file_path = self.base_path / f"{asset_name}.csv"
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        obj.to_csv(file_path, index=False)
        context.log.info(f"Saved error log {asset_name} to {file_path}")
    
    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load error log from CSV"""
        asset_name = context.asset_key.path[-1]
        file_path = self.base_path / f"{asset_name}.csv"
        
        if not file_path.exists():
            # Return empty DataFrame if no errors yet
            return pd.DataFrame()
        
        return pd.read_csv(file_path)

def error_log_io_manager():
    return ErrorLogIOManager("data/07_reporting")

def parsing_results_io_manager():
    return CSVIOManager("data/05_parsing")

def summary_results_io_manager():
    return CSVIOManager("data/06_summary")