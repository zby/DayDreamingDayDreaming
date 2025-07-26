"""
Data storage implementation for the daydreaming experiment.
"""
import json
import pandas as pd
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import os


@dataclass
class ExperimentResult:
    """Represents a single experiment result."""
    model: str
    node_id: str
    prompt: str
    response: str
    seed: int
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


class DataStorage:
    """Handles storage and retrieval of experiment data."""
    
    def __init__(self, storage_dir: str = "results"):
        """
        Initialize data storage.
        
        Args:
            storage_dir: Directory to store experiment results
        """
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        
    def save_result(self, result: ExperimentResult) -> None:
        """
        Save a single experiment result.
        
        Args:
            result: ExperimentResult to save
        """
        filename = f"{result.model}_{result.node_id}_{result.timestamp.replace(':', '-')}.json"
        filepath = os.path.join(self.storage_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(asdict(result), f, indent=2)
            
    def save_results_batch(self, results: List[ExperimentResult]) -> None:
        """
        Save a batch of experiment results.
        
        Args:
            results: List of ExperimentResult objects to save
        """
        # Convert to DataFrame for CSV storage
        df_data = [asdict(result) for result in results]
        df = pd.DataFrame(df_data)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"experiment_results_{timestamp}.csv"
        csv_filepath = os.path.join(self.storage_dir, csv_filename)
        
        # Save to CSV
        df.to_csv(csv_filepath, index=False)
        
    def load_results(self, filepath: str) -> List[ExperimentResult]:
        """
        Load experiment results from a JSON file.
        
        Args:
            filepath: Path to the JSON file
            
        Returns:
            List of ExperimentResult objects
        """
        with open(filepath, 'r') as f:
            data = json.load(f)
            
        if isinstance(data, list):
            return [ExperimentResult(**item) for item in data]
        else:
            return [ExperimentResult(**data)]
            
    def load_results_csv(self, filepath: str) -> pd.DataFrame:
        """
        Load experiment results from a CSV file.
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            DataFrame with experiment results
        """
        return pd.read_csv(filepath)