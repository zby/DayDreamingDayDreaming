"""
Unit tests for the storage module.
"""
import pytest
import os
import json
import pandas as pd
from unittest.mock import Mock, patch, mock_open
from daydreaming_experiment.storage import DataStorage, ExperimentResult
from datetime import datetime


class TestExperimentResult:
    """Test cases for ExperimentResult class."""
    
    def test_init_with_timestamp(self):
        """Test initialization with automatic timestamp."""
        result = ExperimentResult(
            model="test-model",
            node_id="test-node",
            prompt="Test prompt",
            response="Test response",
            seed=42
        )
        
        assert result.model == "test-model"
        assert result.node_id == "test-node"
        assert result.prompt == "Test prompt"
        assert result.response == "Test response"
        assert result.seed == 42
        assert result.timestamp is not None
        
    def test_init_with_custom_timestamp(self):
        """Test initialization with custom timestamp."""
        custom_timestamp = "2023-01-01T00:00:00"
        result = ExperimentResult(
            model="test-model",
            node_id="test-node",
            prompt="Test prompt",
            response="Test response",
            seed=42,
            timestamp=custom_timestamp
        )
        
        assert result.timestamp == custom_timestamp


class TestDataStorage:
    """Test cases for DataStorage class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.storage_dir = "test_results"
        self.storage = DataStorage(self.storage_dir)
        
    def test_init(self):
        """Test DataStorage initialization."""
        assert self.storage.storage_dir == self.storage_dir
        
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.makedirs")
    def test_save_result(self, mock_makedirs, mock_file):
        """Test saving a single result."""
        # Create test result with explicit timestamp
        result = ExperimentResult(
            model="test-model",
            node_id="test-node",
            prompt="Test prompt",
            response="Test response",
            seed=42,
            timestamp="2023-01-01T00:00:00"
        )
        
        # Call method
        self.storage.save_result(result)
        
        # Assertions
        expected_filename = "test-model_test-node_2023-01-01T00-00-00.json"
        expected_filepath = os.path.join(self.storage_dir, expected_filename)
        
        mock_file.assert_called_once_with(expected_filepath, 'w')
        
        # Check that json.dump was called with correct data
        handle = mock_file()
        handle.write.assert_called()  # Simplified assertion
            
    @patch("pandas.DataFrame.to_csv")
    def test_save_results_batch(self, mock_to_csv):
        """Test saving a batch of results."""
        # Create test results with explicit timestamps
        results = [
            ExperimentResult(
                model="test-model-1",
                node_id="test-node-1",
                prompt="Test prompt 1",
                response="Test response 1",
                seed=42,
                timestamp="2023-01-01T00:00:00"
            ),
            ExperimentResult(
                model="test-model-2",
                node_id="test-node-2",
                prompt="Test prompt 2",
                response="Test response 2",
                seed=42,
                timestamp="2023-01-01T00:00:01"
            )
        ]
        
        # Mock timestamp for consistent filename
        with patch('daydreaming_experiment.storage.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "20230101_000000"
            
            # Call method
            self.storage.save_results_batch(results)
            
            # Assertions
            expected_filename = "experiment_results_20230101_000000.csv"
            expected_filepath = os.path.join(self.storage_dir, expected_filename)
            
            mock_to_csv.assert_called_once_with(expected_filepath, index=False)
            
    @patch("builtins.open", new_callable=mock_open, read_data='{"model": "test-model", "node_id": "test-node", "prompt": "Test prompt", "response": "Test response", "seed": 42, "timestamp": "2023-01-01T00:00:00"}')
    def test_load_results_single(self, mock_file):
        """Test loading a single result from JSON."""
        filepath = "test_result.json"
        
        # Call method
        results = self.storage.load_results(filepath)
        
        # Assertions
        mock_file.assert_called_once_with(filepath, 'r')
        assert len(results) == 1
        result = results[0]
        assert isinstance(result, ExperimentResult)
        assert result.model == "test-model"
        assert result.node_id == "test-node"
        assert result.prompt == "Test prompt"
        assert result.response == "Test response"
        assert result.seed == 42
        assert result.timestamp == "2023-01-01T00:00:00"
        
    @patch("builtins.open", new_callable=mock_open, read_data='[{"model": "test-model-1", "node_id": "test-node-1", "prompt": "Test prompt 1", "response": "Test response 1", "seed": 42, "timestamp": "2023-01-01T00:00:00"}, {"model": "test-model-2", "node_id": "test-node-2", "prompt": "Test prompt 2", "response": "Test response 2", "seed": 42, "timestamp": "2023-01-01T00:00:01"}]')
    def test_load_results_multiple(self, mock_file):
        """Test loading multiple results from JSON."""
        filepath = "test_results.json"
        
        # Call method
        results = self.storage.load_results(filepath)
        
        # Assertions
        mock_file.assert_called_once_with(filepath, 'r')
        assert len(results) == 2
        for result in results:
            assert isinstance(result, ExperimentResult)
            
    @patch("pandas.read_csv")
    def test_load_results_csv(self, mock_read_csv):
        """Test loading results from CSV."""
        filepath = "test_results.csv"
        mock_df = Mock(spec=pd.DataFrame)
        mock_read_csv.return_value = mock_df
        
        # Call method
        result = self.storage.load_results_csv(filepath)
        
        # Assertions
        mock_read_csv.assert_called_once_with(filepath)
        assert result == mock_df