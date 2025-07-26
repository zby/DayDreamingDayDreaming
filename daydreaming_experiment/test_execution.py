"""
Unit tests for the execution module.
"""
import pytest
from unittest.mock import Mock, patch
from daydreaming_experiment.execution import ExperimentExecutor
from daydreaming_experiment.concept_dag import ConceptDAG, ConceptLevel
from daydreaming_experiment.storage import DataStorage, ExperimentResult


class TestExperimentExecutor:
    """Test cases for ExperimentExecutor class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.dag = ConceptDAG()
        self.dag.add_node("root", "Root content", ConceptLevel.SENTENCE)
        self.dag.add_node("child", "Child content", ConceptLevel.SENTENCE, ["root"])
        
        self.storage = Mock(spec=DataStorage)
        self.mock_client = Mock()
        self.executor = ExperimentExecutor(self.dag, self.storage, self.mock_client)
        
    def test_init(self):
        """Test ExperimentExecutor initialization."""
        assert isinstance(self.executor.dag, ConceptDAG)
        assert self.executor.storage == self.storage
        assert self.executor.client == self.mock_client
        
    def test_get_model_response_success(self):
        """Test successful model response."""
        # Setup mock
        mock_message = Mock()
        mock_message.content = "Test response"
        mock_choice = Mock()
        mock_choice.message = mock_message
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        self.mock_client.chat.completions.create.return_value = mock_response
        
        # Test method
        result = self.executor._get_model_response("test-model", "Test prompt")
        
        # Assertions
        assert result == "Test response"
        self.mock_client.chat.completions.create.assert_called_once_with(
            model="test-model",
            messages=[{"role": "user", "content": "Test prompt"}],
            seed=42
        )
        
    def test_get_model_response_error(self):
        """Test model response with error."""
        # Setup mock to raise exception
        self.mock_client.chat.completions.create.side_effect = Exception("API Error")
        
        # Test method
        result = self.executor._get_model_response("test-model", "Test prompt")
        
        # Assertions
        assert result.startswith("ERROR:")
        assert "API Error" in result